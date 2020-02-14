/* This file is part of opentsdb-rollup-rust.
 *
 * Copyright © 2020 Datto, Inc.
 * Author: Alex Parrill <aparrill@datto.com>
 *
 * Licensed under the Mozilla Public License Version 2.0
 * Fedora-License-Identifier: MPLv2.0
 * SPDX-2.0-License-Identifier: MPL-2.0
 * SPDX-3.0-License-Identifier: MPL-2.0
 *
 * opentsdb-rollup-rust is free software.
 * For more information on the license, see LICENSE.
 * For more information on free software, see <https://www.gnu.org/philosophy/free-sw.en.html>.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at <https://mozilla.org/MPL/2.0/>.
 */


#[macro_use]
extern crate log;

use rdkafka::config::{
	ClientConfig,
	RDKafkaLogLevel,
};
use std::{
	borrow::Cow,
	collections::HashMap,
	num::{
		NonZeroU64,
		NonZeroUsize,
	},
	os::unix::net::UnixStream,
	panic,
	path::PathBuf,
	str::FromStr,
	thread,
	time::Duration,
};
use structopt::StructOpt;

use opentsdb_rollup_rust::*;

/// Key+value for arg parsing, implementing FromStr
#[derive(Debug, Clone)]
struct KV(String, String);
impl FromStr for KV {
	type Err = String;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let index = s
			.find('=')
			.ok_or_else(|| String::from("Must be in format `key=value` (no `=` found)"))?;
		if index == 0 || index == s.len() - 1 {
			return Err("Empty key".into());
		}
		let key = s[..index].into();
		let value = s[index + 1..].into();
		return Ok(KV(key, value));
	}
}

#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
struct Args {
	/// Checkpoint backend and options to use, in format
	/// `backendname:arg1=val1,"arg2"="val2",...`
	///
	/// Backends are:
	///
	/// * `fs:path=/example.checkpoint`: local filesystem
	/// * `hdfs:[name_node=host,][username=username,]path=/example.checkpoint`: Hadoop HDFS filesystem
	/// * `redis:prefix=key-prefix,sentinel_addrs=127.0.0.1:1234/10.1.2.3:4567/[...],master_name=redis_server,[password=asdf,][database=4]`: Redis with Sentinel
	#[structopt(parse(try_from_str=util::parse_checkpoint_opts))]
	checkpoint_backend: (String, HashMap<String, String>),

	/// Topic to write rolled up metics to
	dest_topic: String,

	/// Topics to read metrics from
	#[structopt(required = true)]
	ingest_topics: Vec<String>,

	/// Number of workers for reading+parsing metrics
	#[structopt(short = "C", long, default_value = "3")]
	num_consumer_workers: NonZeroUsize,

	/// Number of workers for processing rollups and producing
	#[structopt(short = "R", long, default_value = "3")]
	num_rollup_workers: NonZeroUsize,

	/// Allowed lateness of metrics to be counted, in ms.
	#[structopt(short = "l", long, default_value = "120000")]
	allowed_lateness: u64,

	/// Interval to wait between reporting metrics, in ms.
	#[structopt(short = "m", long, default_value = "60000")]
	metrics_report_interval: u64,

	/// Checkpoint interval, in ms
	#[structopt(short = "h", long, default_value = "1800000")]
	checkpoint_interval: NonZeroU64,

	/// Custom options for the kafka consumer
	#[structopt(short = "c", long, value_name = "key=value", number_of_values = 1)]
	consumer_config: Vec<KV>,

	/// Custom options for the kafka producer
	#[structopt(short = "p", long, value_name = "key=value", number_of_values = 1)]
	producer_config: Vec<KV>,

	/// `host:port` to write influx line metrics to.
	#[structopt(short = "I", long)]
	influx_host: Option<String>,

	/// Size of the inter-worker queues.
	#[structopt(long, default_value = "10240")]
	queue_size: usize,

	/// If specified, drop metrics this many seconds in the future according to this system's clock.
	#[structopt(long)]
	future_cutoff: Option<u64>,

	/// Turn on debug logging
	#[structopt(short = "v", long)]
	verbose: bool,

	/// Log file destination. Default is stderr
	#[structopt(short = "L", long, parse(try_from_str))]
	log_file: Option<PathBuf>,

	/// Metrics blacklist JSON file
	#[structopt(short = "B", long, parse(try_from_str))]
	blacklist: Option<PathBuf>,
}

fn configure_logging(args: &Args) -> std::io::Result<()> {
	let level_filter = if args.verbose {
		log::LevelFilter::Debug
	} else {
		log::LevelFilter::Info
	};

	let mut dispatch = fern::Dispatch::new()
		.format(|out, message, record| {
			let current_thread = thread::current();
			let thread_name = match current_thread.name() {
				Some(v) => Cow::from(v),
				None => Cow::from(format!("{:?}", current_thread.id())),
			};

			out.finish(format_args!(
				"{}[{}][{}][{}]: {}",
				chrono::Utc::now().to_rfc3339(),
				thread_name,
				record.target(),
				record.level(),
				message,
			))
		})
		.level(level_filter);

	if let Some(dest) = args.log_file.clone() {
		println!("Logging to {}", dest.display());
		dispatch = dispatch.chain(fern::log_file(dest)?);
	} else {
		dispatch = dispatch.chain(std::io::stderr());
	}

	dispatch.apply().expect("Could not apply logging config");
	Ok(())
}

fn main() {
	let args = Args::from_args();

	if let Err(err) = configure_logging(&args) {
		eprintln!("Could not initialize logging: {}", err);
		::std::process::exit(1);
	}
	panic::set_hook(Box::new(logging_panic_hook));

	let (backend_type, backend_params) = args.checkpoint_backend;

	let backend = match checkpoint_backend::create_backend(&backend_type, backend_params) {
		Ok(b) => b,
		Err(e) => {
			eprintln!("Could not create backend: {}", e);
			::std::process::exit(1);
		}
	};

	let metrics = if let Some(host_port) = args.influx_host {
		match influx::InfluxReporter::new(host_port) {
			Ok(v) => Some(v),
			Err(err) => {
				eprintln!("Could not create influx socket: {}", err);
				::std::process::exit(1);
			}
		}
	} else {
		None
	};

	let blacklist = match args
		.blacklist
		.as_ref()
		.map(|path| blacklist::Blacklist::read(path))
	{
		Some(Ok(v)) => v,
		Some(Err(e)) => {
			eprintln!("{}", e);
			::std::process::exit(1);
		}
		None => blacklist::Blacklist::default(),
	};

	let mut group_id = String::from("opentsdb-rollup");

	let mut consumer_config = ClientConfig::new();
	consumer_config.set("group.id", &group_id);
	consumer_config.set("auto.offset.reset", "earliest");
	consumer_config.set("max.poll.interval.ms", "86400000"); // TEMP: checkpointing taking a very long time at the moment
	consumer_config.set(
		"statistics.interval.ms",
		&format!("{}", args.metrics_report_interval),
	);
	consumer_config.set_log_level(RDKafkaLogLevel::Debug);
	for &KV(ref k, ref v) in args.consumer_config.iter() {
		consumer_config.set(k, v);
		if k == "group.id" {
			group_id = String::from(v);
		}
	}

	let mut producer_config = ClientConfig::new();
	producer_config.set(
		"statistics.interval.ms",
		&format!("{}", args.metrics_report_interval),
	);
	producer_config.set("linger.ms", "100");
	producer_config.set("queue.buffering.max.messages", "1000000");
	producer_config.set("compression.codec", "gzip");
	producer_config.set_log_level(RDKafkaLogLevel::Debug);
	for &KV(ref k, ref v) in args.producer_config.iter() {
		producer_config.set(k, v);
	}

	let instance_id = format!("{}.{}", group_id, args.ingest_topics.join("."));

	let (intr_reader, intr_writer) = UnixStream::pair().expect("Could not create interrupt pipe");
	let sigid_intr = signal_hook::pipe::register(
		signal_hook::SIGINT,
		intr_writer.try_clone().expect("Could not clone fd"),
	)
	.expect("Could not create signal handler");
	let sigid_term = signal_hook::pipe::register(
		signal_hook::SIGTERM,
		intr_writer.try_clone().expect("Could not clone fd"),
	)
	.expect("Could not create signal handler");

	let settings = coordinator::Settings {
		instance_id,
		ingest_topics: args.ingest_topics,
		dest_topic: args.dest_topic,
		num_consumer_workers: args.num_consumer_workers.into(),
		num_rollup_workers: args.num_rollup_workers.into(),
		consumer_config,
		producer_config,
		allowed_lateness: args.allowed_lateness,
		future_cutoff: args.future_cutoff,
		window_assigner: StaticWindowAssigner {
			windows: vec![
				types::OpenTsdbDuration {
					count: 10,
					unit: types::OpenTsdbTimeUnit::Minute,
				},
				types::OpenTsdbDuration {
					count: 24,
					unit: types::OpenTsdbTimeUnit::Hour,
				},
			],
		},
		queue_buffer_size: args.queue_size,
		checkpoint_interval: Duration::from_millis(args.checkpoint_interval.into()),
		checkpoint_backend: backend,
		mem_metrics_interval: Duration::from_millis(args.metrics_report_interval),
		interrupt_sigids: vec![sigid_intr, sigid_term],
		interrupt_signal_stream: intr_reader,
		metrics,
		blacklist,
	};

	if let Err(e) = coordinator::run(settings) {
		error!("Could not start: {}", e);
		::std::process::exit(1);
	}
}

#[derive(Debug, Clone)]
struct StaticWindowAssigner {
	pub windows: Vec<types::OpenTsdbDuration>,
}
impl rollup::WindowAssigner for StaticWindowAssigner {
	fn assign(&self, _key: &types::OpenTsdbKey) -> &[types::OpenTsdbDuration] {
		&self.windows
	}
}

fn logging_panic_hook(info: &panic::PanicInfo) {
	let payload = info.payload();
	let msg = if let Some(s) = payload.downcast_ref::<String>() {
		s.as_str()
	} else if let Some(s) = payload.downcast_ref::<&str>() {
		*s
	} else {
		"(non-string payload)"
	};
	error!("Thread panic: {}", msg);
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn kv_fromstr() {
		let kv = KV::from_str("group.id=opentsdb-rollup-rust-test").unwrap();
		assert_eq!(kv.0, "group.id");
		assert_eq!(kv.1, "opentsdb-rollup-rust-test");
	}
}
