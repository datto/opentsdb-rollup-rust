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


//! Rollup worker, the part that aggregates metrics and submits those aggregates to Kafka

use crossbeam_channel as cbc;
use rdkafka::{
	client::ClientContext,
	config::{
		ClientConfig,
		RDKafkaLogLevel,
	},
	producer::base_producer::{
		BaseRecord,
		DeliveryResult,
		ProducerContext,
		ThreadedProducer,
	},
};
use serde::Serialize;
use serde_json;
use std::{
	sync::{
		atomic,
		Arc,
		Mutex,
	},
	time::{
		Duration,
		Instant,
	},
};

use crate::{
	coordinator::CoordinatorMessage,
	influx::InfluxReporter,
	smallmap::SmallMap,
	types::*,
	util::HashMapExt,
};

/// Message passed to the rollup queue
#[derive(Debug, Clone)]
pub enum RollupMessage {
	/// Process a metric
	Metric(Metric),
	/// Beginning a checkpoint.
	/// Send state to the coordinator so it can serialize it.
	Checkpoint,
}

/// Trait for determining what duration windows a metric should go into
pub trait WindowAssigner: Send + Clone {
	/// Returns a set of window durations to put the metric in.
	/// Should return the same list contents for the same key.
	fn assign(&self, key: &OpenTsdbKey) -> &[OpenTsdbDuration];
}

pub struct RollupWorkerSettings<Assigner: WindowAssigner> {
	pub id: usize,
	pub instance_id: String,
	pub client_cfg: ClientConfig,
	pub coordinator_sender: cbc::Sender<CoordinatorMessage>,
	pub receiver: Arc<cbc::Receiver<RollupMessage>>,
	pub metrics_sender: Option<Arc<InfluxReporter>>,
	pub assigner: Assigner,
	pub dest_topic: String,
	pub submit_interval: Duration,
	pub allowed_lateness: u64,
	pub state: Arc<RollupState>,
}

/// Worker that manages windows, aggregates metrics into them, and produces rolled-up metrics when
/// the window closes.
pub struct RollupWorker<Assigner: WindowAssigner> {
	settings: RollupWorkerSettings<Assigner>,
	producer: ThreadedProducer<ProducerCtx>,
	metrics: Arc<RollupWorkerMetrics>,

	next_submission: Instant,
}
impl<Assigner: WindowAssigner> RollupWorker<Assigner> {
	pub fn new(settings: RollupWorkerSettings<Assigner>) -> rdkafka::error::KafkaResult<Self> {
		let metrics = Arc::new(RollupWorkerMetrics::new());
		let ctx = ProducerCtx {
			id: settings.id,
			instance_id: settings.instance_id.clone(),
			metrics_sender: settings.metrics_sender.as_ref().map(Arc::clone),
			receiver: Arc::clone(&settings.receiver),
			metrics: Arc::clone(&metrics),
			sent_list: Arc::clone(&settings.state.submitted),
			last_statistic_time: Mutex::new(Instant::now()),
		};

		let producer: ThreadedProducer<ProducerCtx> =
			settings.client_cfg.create_with_context(ctx)?;

		Ok(Self {
			settings,
			producer,
			metrics,

			next_submission: Instant::now(),
		})
	}

	pub fn run(&mut self) {
		debug!("Starting");
		loop {
			let message = match self.settings.receiver.recv() {
				Ok(m) => m,
				Err(_) => {
					break;
				}
			};

			match message {
				RollupMessage::Metric(metric) => {
					self.on_metric(metric);

					if Instant::now() >= self.next_submission {
						debug!("Submitting windows");
						self.submit_windows();
						self.next_submission = Instant::now() + self.settings.submit_interval;
						debug!("Done submitting windows");
					}
				}
				RollupMessage::Checkpoint => {
					debug!("Sending state to coordinator");
					self.settings
						.coordinator_sender
						.send(CoordinatorMessage::CheckpointState(
							self.settings.id,
							Arc::clone(&self.settings.state),
						))
						.expect("coordinator channel unexpectedly closed");
				}
			}
		}
	}

	fn on_metric(&mut self, metric: Metric) {
		self.metrics
			.incoming_count
			.fetch_add(1, atomic::Ordering::Relaxed);

		let state = Arc::get_mut(&mut self.settings.state)
			.expect("Got metric while coordinator still has our state.");

		state.event_time.update(
			&metric.topic,
			metric.partition,
			metric.timestamp,
			metric.offset,
		);

		// Check for late metric
		if let Some(event_time) = state.event_time.time() {
			if metric.timestamp < event_time - self.settings.allowed_lateness {
				return;
			}
		}

		let window_set = state
			.windows
			.entry_to_owned(&metric.key)
			.or_insert_default();

		// Add metric to points
		let window_durations = self.settings.assigner.assign(&metric.key);
		for duration in window_durations {
			let window_start = metric.timestamp - (metric.timestamp % duration.as_ms());
			let window = window_set.get_or_create_window(window_start, &duration);
			window.aggregated_point.add(metric.value);
		}
	}

	/// Called occasionally to submit finished windows to kafka.
	fn submit_windows(&mut self) {
		// Have to reference some fields outside of `self`, since closures below attempt to borrow the entire
		// `self` object, which is impossible because `Arc::get_mut` borrows it mutably.
		let allowed_lateness = self.settings.allowed_lateness;
		let dest_topic = &self.settings.dest_topic;
		let producer = &self.producer;

		let state = Arc::get_mut(&mut self.settings.state)
			.expect("Wanted to submit windows but coordinator still has our state.");

		let event_time = match state.event_time.time() {
			Some(t) => t,
			None => {
				return;
			}
		};

		let mut sent_list = state.submitted.lock().expect("sent_list lock poisoned");

		// First, collect new aggregated points to send
		let mut submitted_metrics = 0;
		state.windows.retain(|key, windowset| {
			windowset.windows.retain(|window| {
				if event_time <= window.start + window.duration.as_ms() + allowed_lateness {
					return true;
				}

				sent_list.to_send_enqueue(key.clone(), window.clone());
				submitted_metrics += 1;
				return false;
			});

			if windowset.windows.is_empty() {
				debug!("Window set for key {} is empty, removing", key);
				return false;
			} else {
				return true;
			}
		});
		debug!(
			"Collected {} new aggregated points, total outbound queue size is {}",
			submitted_metrics,
			sent_list.to_send_len()
		);

		// Next, start sending
		let mut sent_metrics = 0;
		while let Some((id, &(ref key, ref window))) = sent_list.to_send_pop() {
			if let Err(err) = submit_window(dest_topic, producer, key, window, id) {
				warn!("Could not submit aggregated point to producer, will retry later ({} left in send queue): {}",
					sent_list.to_send_len(), err);
				sent_list.sent_err(id);
				break;
			}
			sent_metrics += 1;
		}
		debug!("Sent {} aggregated points to kafka", sent_metrics);

		sent_list.shrink();
	}
}

/// Submits a window to Kafka
///
/// Doesn't take &self since the caller will be borrowing `state` which mean we can't pass it.
fn submit_window(
	dest_topic: &str,
	producer: &ThreadedProducer<ProducerCtx>,
	key: &OpenTsdbKey,
	window: &Window,
	sent_list_index: usize,
) -> Result<(), rdkafka::error::KafkaError> {
	let duration_str = window.duration.to_string();

	let entries = &[
		OpenTsdbAggregateEntry {
			type_: "Aggregate",
			timestamp: window.start / 1000,
			metric: key.name(),
			tags: key.tags(),
			interval: &duration_str,
			aggregator: "MIN",
			value: window.aggregated_point.min.into(),
		},
		OpenTsdbAggregateEntry {
			type_: "Aggregate",
			timestamp: window.start / 1000,
			metric: key.name(),
			tags: key.tags(),
			interval: &duration_str,
			aggregator: "MAX",
			value: window.aggregated_point.max.into(),
		},
		OpenTsdbAggregateEntry {
			type_: "Aggregate",
			timestamp: window.start / 1000,
			metric: key.name(),
			tags: key.tags(),
			interval: &duration_str,
			aggregator: "SUM",
			value: window.aggregated_point.sum.into(),
		},
		OpenTsdbAggregateEntry {
			type_: "Aggregate",
			timestamp: window.start / 1000,
			metric: key.name(),
			tags: key.tags(),
			interval: &duration_str,
			aggregator: "COUNT",
			value: window.aggregated_point.count.into(),
		},
	];

	let payload = serde_json::to_vec(entries).expect("Could not serialize aggregated point");
	let record: BaseRecord<(), Vec<u8>, usize> =
		BaseRecord::with_opaque_to(dest_topic, sent_list_index).payload(&payload);
	producer.send(record).map_err(|e| e.0)
}

/// Helper struct for serializing into OpenTSDB json format
#[derive(Debug, Serialize)]
struct OpenTsdbAggregateEntry<'a> {
	#[serde(rename = "type")]
	pub type_: &'a str,
	pub timestamp: u64,
	pub metric: &'a str,
	pub tags: &'a SmallMap<Box<str>, Box<str>>,
	pub interval: &'a str,

	pub aggregator: &'a str,
	pub value: serde_json::Value,
}

/// Counters added to by the worker and read by the producer context.
struct RollupWorkerMetrics {
	/// Number of metrics the worker has ingested.
	///
	/// When the context emits stats, it reads and resets this value.
	pub incoming_count: atomic::AtomicUsize,
}
impl RollupWorkerMetrics {
	pub fn new() -> RollupWorkerMetrics {
		RollupWorkerMetrics {
			incoming_count: atomic::AtomicUsize::new(0),
		}
	}
}

struct ProducerCtx {
	id: usize,
	instance_id: String,
	metrics_sender: Option<Arc<InfluxReporter>>,
	receiver: Arc<cbc::Receiver<RollupMessage>>,
	metrics: Arc<RollupWorkerMetrics>,
	sent_list: Arc<Mutex<SentList>>,
	last_statistic_time: Mutex<Instant>,
}
impl ClientContext for ProducerCtx {
	fn log(&self, rdlevel: RDKafkaLogLevel, fac: &str, log_message: &str) {
		let level = match rdlevel {
			RDKafkaLogLevel::Emerg
			| RDKafkaLogLevel::Alert
			| RDKafkaLogLevel::Critical
			| RDKafkaLogLevel::Error => log::Level::Error,
			RDKafkaLogLevel::Warning => log::Level::Warn,
			RDKafkaLogLevel::Notice | RDKafkaLogLevel::Info => log::Level::Info,
			RDKafkaLogLevel::Debug => log::Level::Debug,
		};

		log!(level, "{}: {}", fac, log_message);
	}

	fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
		error!("Kafka producer error: {} (reason: {})", error, reason);
	}

	fn stats(&self, statistics: rdkafka::statistics::Statistics) {
		let incoming_count = self
			.metrics
			.incoming_count
			.swap(0, atomic::Ordering::Relaxed);

		let sender = match self.metrics_sender.as_ref() {
			Some(m) => m,
			None => {
				return;
			}
		};

		let now = Instant::now();
		let previous_time = {
			let mut lock = self.last_statistic_time.lock().unwrap();
			let previous_time = (*lock).clone();
			*lock = now.clone();
			previous_time
		};
		let duration_s = (now - previous_time).as_millis() as f64 / 1000f64;

		let worker_id = format!("{}", self.id);
		let tags = &[
			("instance_id", self.instance_id.as_str()),
			("worker_id", worker_id.as_str()),
		];

		let receiver_count = self.receiver.len();
		let receiver_capacity = self.receiver.capacity().unwrap_or(usize::max_value());

		let values = &[
			("msg_cnt", statistics.msg_cnt as f64),
			("msg_size", statistics.msg_size as f64),
			(
				"msg_cnt_percent",
				statistics.msg_cnt as f64 / statistics.msg_max as f64,
			),
			(
				"msg_size_percent",
				statistics.msg_size as f64 / statistics.msg_size_max as f64,
			),
			("incoming_per_second", incoming_count as f64 / duration_s),
			("queue_count", receiver_count as f64),
			(
				"queue_filled_percent",
				receiver_count as f64 / receiver_capacity as f64,
			),
		];

		sender.send(
			"opentsdb_rollup_rust.rollup_worker",
			tags.iter().copied(),
			values.iter().copied(),
		);
	}
}
impl ProducerContext for ProducerCtx {
	type DeliveryOpaque = usize;

	fn delivery(&self, delivery_result: &DeliveryResult, sent_list_index: Self::DeliveryOpaque) {
		let mut sent_list = self.sent_list.lock().expect("sent_list lock poisoned");
		match delivery_result {
			Ok(_) => sent_list.sent_ok(sent_list_index),
			Err((err, _)) => {
				warn!("Could not send aggregate point, will retry: {}", err);
				sent_list.sent_err(sent_list_index);
			}
		}
	}
}
