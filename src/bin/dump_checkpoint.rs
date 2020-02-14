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


extern crate opentsdb_rollup_rust;

use chrono::offset::{
	TimeZone,
	Utc,
};
use std::collections::HashMap;
use structopt::StructOpt;

/// Writes checkpoint contents to stdout
#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
struct Args {
	/// Checkpoint arguments to use. See the main rollup program for usage.
	#[structopt(parse(try_from_str=opentsdb_rollup_rust::util::parse_checkpoint_opts))]
	checkpoint_backend: (String, HashMap<String, String>),

	/// Skip sorting windows. Prints faster for large checkpoints
	#[structopt(short("s"), long)]
	skip_sorting: bool,
}

fn main() {
	if let Err(err) = run() {
		eprintln!("{}", err);
		::std::process::exit(1);
	}
}

fn run() -> Result<(), String> {
	let args = Args::from_args();

	let (backend_type, backend_params) = args.checkpoint_backend;
	let mut backend =
		opentsdb_rollup_rust::checkpoint_backend::create_backend(&backend_type, backend_params)
			.map_err(|e| format!("Could not initialize backend: {}", e))?;

	let checkpoint = match backend.load() {
		Ok(Some(c)) => c,
		Ok(None) => {
			return Err("No checkpoint found".into());
		}
		Err(err) => {
			return Err(format!("Could not load checkpoint: {}", err));
		}
	};

	let mut offsets = checkpoint.event_time.iter().collect::<Vec<_>>();
	offsets.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

	println!("Timestamps+Offsets:");
	for (topic, partition, t) in offsets {
		println!(
			"{}/{}: ts: {} ({}), offset:{}",
			topic,
			partition,
			format_timestamp(t.timestamp),
			t.timestamp,
			t.offset
		);
	}
	println!("");

	let mut finished_windows = checkpoint
		.submitted
		.lock()
		.unwrap()
		.to_send_iter()
		.cloned()
		.collect::<Vec<_>>();
	finished_windows.sort_unstable_by(|a, b| a.0.cmp(&b.0));
	println!("Finished windows:");
	for (key, window) in finished_windows.into_iter() {
		println!(
			"{} @ {}/{}: min:{}, max:{}, sum:{}, count:{}",
			key,
			window.start,
			window.duration,
			window.aggregated_point.min,
			window.aggregated_point.max,
			window.aggregated_point.sum,
			window.aggregated_point.count,
		);
	}
	println!("");

	println!("Open windows:");
	let mut windows = checkpoint.windows.into_iter().collect::<Vec<_>>();
	if !args.skip_sorting {
		windows.sort_unstable_by(|&(ref k1, _), &(ref k2, _)| k1.cmp(k2));
	}
	for (key, window_set) in windows.into_iter() {
		println!("{}:", key);
		for window in window_set.windows {
			println!(
				"\t{}/{}: min:{}, max:{}, sum:{}, count:{}",
				window.start,
				window.duration,
				window.aggregated_point.min,
				window.aggregated_point.max,
				window.aggregated_point.sum,
				window.aggregated_point.count,
			);
		}
	}

	Ok(())
}

fn format_timestamp(ts: u64) -> String {
	Utc.timestamp((ts / 1000) as i64, (ts % 1000) as u32 * 1_000_000)
		.to_rfc2822()
}
