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


//! Consumer worker, the part that reads from kafka and parses metrics

use crossbeam_channel as cbc;
use lazy_static::lazy_static;
use rdkafka::{
	client::ClientContext,
	config::{
		ClientConfig,
		RDKafkaLogLevel,
	},
	consumer::{
		BaseConsumer,
		CommitMode,
		Consumer,
		ConsumerContext,
	},
	topic_partition_list::{
		Offset,
		TopicPartitionList,
	},
	Message,
};
use regex::Regex;
use serde::Deserialize;
use serde_json::Value;
use std::{
	collections::HashMap,
	str::FromStr,
	sync::Arc,
	thread::sleep,
	time::{
		Duration,
		SystemTime,
	},
};

use crate::{
	blacklist::Blacklist,
	coordinator::CoordinatorMessage,
	influx::InfluxReporter,
	rollup::RollupMessage,
	smallmap::SmallMap,
	types::*,
};

/// Messages to send to the consumer
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumerMessage {
	/// Pauses the consumer.
	/// The consumer will send the `CoordinatorMessage::ConsumerPaused` message after receiving to
	/// inform the consumer that it is paused.
	/// Should be followed up by either the `UnpauseForCheckpoint` message or by closing the channel.
	PauseForCheckpoint,
	/// Unpauses the consumer.
	/// Can only be sent after `PauseForCheckpoint`.
	UnpauseForCheckpoint,
}

struct ConsumerCtx {
	id: usize,
	instance_id: String,
	sender: Option<Arc<InfluxReporter>>,
}
impl ClientContext for ConsumerCtx {
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
		error!("Kafka consumer error: {} (reason: {})", error, reason);
	}

	fn stats(&self, statistics: rdkafka::statistics::Statistics) {
		let sender = match self.sender.as_ref() {
			Some(m) => m,
			None => {
				return;
			}
		};

		let worker_id = format!("{}", self.id);

		for (topic_name, topic) in statistics.topics.iter() {
			for (partition, part_stats) in topic.partitions.iter() {
				if part_stats.consumer_lag == -1 {
					continue;
				}

				let topic_part = format!("{}/{}", topic_name, partition);

				let tags = &[
					("instance_id", self.instance_id.as_str()),
					("worker_id", worker_id.as_str()),
					("topic_partition", topic_part.as_str()),
				];

				let values = &[
					("lag", part_stats.consumer_lag as f64),
					("fetchq_cnt", part_stats.fetchq_cnt as f64),
					("fetchq_size", part_stats.fetchq_size as f64),
					("msgs", part_stats.msgs as f64),
				];

				sender.send(
					"opentsdb_rollup_rust.consumer_worker",
					tags.iter().copied(),
					values.iter().copied(),
				);
			}
		}
	}
}
impl ConsumerContext for ConsumerCtx {}

pub struct ConsumerWorkerSettings {
	pub id: usize,
	pub instance_id: String,
	pub assignment: HashMap<(String, i32), Offset>,
	pub client_cfg: ClientConfig,
	pub future_cutoff: Option<u64>,
	pub coordinator_sender: cbc::Sender<CoordinatorMessage>,
	pub receiver: cbc::Receiver<ConsumerMessage>,
	pub rollup_senders: Vec<cbc::Sender<RollupMessage>>,
	pub metrics_sender: Option<Arc<InfluxReporter>>,
	pub blacklist: Blacklist,
}

/// Consumer worker info.
///
/// This handles polling a Kafka consumer for one or more partitions, parsing
/// the resulting metrics, and sending them to the corresponding rollup worker.
///
/// A dedicated thread should run this.
pub struct ConsumerWorker {
	settings: ConsumerWorkerSettings,
	consumer: BaseConsumer<ConsumerCtx>,
}
impl ConsumerWorker {
	/// Creates a new worker, doing some setup work.
	pub fn new(mut settings: ConsumerWorkerSettings) -> rdkafka::error::KafkaResult<Self> {
		settings
			.client_cfg
			.set("enable.auto.commit", "false")
			.set("enable.auto.offset.store", "true")
			.set(
				"client.id",
				&format!("{}/{}", settings.instance_id, settings.id),
			);
		let ctx = ConsumerCtx {
			id: settings.id,
			instance_id: settings.instance_id.clone(),
			sender: settings.metrics_sender.as_ref().map(|arc| Arc::clone(arc)),
		};

		let consumer: BaseConsumer<ConsumerCtx> = settings.client_cfg.create_with_context(ctx)?;
		consumer.assign(&TopicPartitionList::from_topic_map(&settings.assignment))?;

		Ok(Self { settings, consumer })
	}

	/// Runs the worker. Does not exit until the coordinator tells it to.
	pub fn run(&mut self) {
		debug!("Consumer worker {} starting", self.settings.id);
		loop {
			match self.settings.receiver.try_recv() {
				Ok(m) => {
					if !self.on_coordinator_message(m) {
						break;
					}
				}
				Err(cbc::TryRecvError::Empty) => {}
				Err(cbc::TryRecvError::Disconnected) => {
					info!("Coordinator channel exited, exiting thread.");
					break;
				}
			}

			self.poll_consumer();
		}
	}

	/// Polls the kafka consumer, retrying if needed
	fn poll_consumer(&mut self) {
		let mut has_consumer = true;
		for _consumer_try in 0..3 {
			if has_consumer {
				for _poll_try in 0..10 {
					match self.consumer.poll(Some(Duration::from_millis(100))) {
						Some(Ok(m)) => {
							self.on_kafka_message(m);
							return;
						}
						None => {
							return;
						}
						Some(Err(e)) => {
							warn!("Consumer yielded error, will retry: {}", e);
							sleep(Duration::from_millis(1000));
						}
					};
				}
				error!("Too many errors from the consumer, reconnecting...");
				has_consumer = false;
			}

			let ctx = ConsumerCtx {
				id: self.settings.id,
				instance_id: self.settings.instance_id.clone(),
				sender: self
					.settings
					.metrics_sender
					.as_ref()
					.map(|arc| Arc::clone(arc)),
			};

			let consumer_res = self
				.settings
				.client_cfg
				.create_with_context::<ConsumerCtx, BaseConsumer<ConsumerCtx>>(ctx)
				.and_then(|consumer| {
					let res = consumer.assign(&TopicPartitionList::from_topic_map(
						&self.settings.assignment,
					));
					res.map(|()| consumer)
				});
			match consumer_res {
				Ok(consumer) => {
					self.consumer = consumer;
					has_consumer = true;
				}
				Err(err) => {
					error!("Error recreating consumer: {}", err);
					sleep(Duration::from_millis(1000));
				}
			};
		}

		panic!("Too many retries connecting to kafka");
	}

	fn on_kafka_message(&self, message: rdkafka::message::BorrowedMessage) {
		// Try to get the payload as a byte slice
		let payload = match message.payload_view::<[u8]>() {
			Some(Ok(bytes)) => bytes,
			None => {
				return;
			}
			Some(Err(e)) => {
				warn!("Could not get kafka value: {:?}", e);
				return;
			}
		};

		// Deserialize telegraf object
		let telegraf_obj = match serde_json::from_slice::<TelegramMetric>(payload) {
			Ok(v) => v,
			Err(err) => {
				warn!(
					"Could not deserialize.\nBody:\n{:?}\nError: {}",
					payload, err
				);
				return;
			}
		};

		// If the metric is in the future, drop it
		if let Some(future_cutoff) = self.settings.future_cutoff.clone() {
			let now = SystemTime::UNIX_EPOCH.elapsed().unwrap().as_secs();
			if telegraf_obj.timestamp >= now + future_cutoff {
				warn!(
					"Dropping metric that is too far into the future: {:?}",
					telegraf_obj
				);
				return;
			}
		}

		let tags = Arc::new(filter_tags(telegraf_obj.tags));

		// Create a metric for each telegraf field
		for (field_key, field_json_value) in telegraf_obj.fields.iter() {
			// Get a f64 value, either directly or by parsing the string.
			// Skip non-number values.
			let value = if let Some(val) = field_json_value.as_f64() {
				val
			} else if let Some(s) = field_json_value.as_str() {
				match f64::from_str(s) {
					Ok(val) => val,
					Err(_) => {
						continue;
					}
				}
			} else {
				continue;
			};

			// Create the metric object
			let key = OpenTsdbKey::new(
				filter_name(format!("{}.{}", telegraf_obj.name, field_key)).into_boxed_str(),
				Arc::clone(&tags),
			);

			if self.settings.blacklist.matches(&key) {
				continue;
			}

			let metric = Metric {
				timestamp: telegraf_obj.timestamp * 1000,
				key,
				value,
				topic: message.topic().into(),
				partition: message.partition(),
				offset: message.offset(),
			};

			// Find which worker should handle this metric.
			// Needs to be consistent w.r.t. the opentsdb key so that windows aren't
			// duplicated across threads.
			let index = metric
				.key
				.distribute_index(self.settings.rollup_senders.len());

			// Send metric
			self.settings.rollup_senders[index]
				.send(RollupMessage::Metric(metric))
				.expect("rollup channel unexpectedly closed");
		}
	}

	fn on_coordinator_message(&self, message: ConsumerMessage) -> bool {
		match message {
			ConsumerMessage::PauseForCheckpoint => {
				debug!(
					"Consumer worker {} paused for checkpointing",
					self.settings.id
				);

				if let Err(err) = self.consumer.commit_consumer_state(CommitMode::Sync) {
					warn!(
						"Consumer {} could not commit offsets, ignoring. Error was: {}",
						self.settings.id, err
					);
				}

				// Let consumer we received this message and are paused
				self.settings
					.coordinator_sender
					.send(CoordinatorMessage::ConsumerPaused(self.settings.id))
					.expect("coordinator sender channel unexpectedly closed");

				// Wait for unpause message
				let msg = self.settings.receiver.recv();
				match msg {
					Ok(ConsumerMessage::UnpauseForCheckpoint) => {
						debug!("Consumer worker {} unpaused", self.settings.id);
						return true;
					}
					Ok(v) => panic!(
						"Got unexpected message while waiting for checkpoint unpause: {:?}",
						v
					),
					Err(_) => {
						return false;
					}
				};
			}
			ConsumerMessage::UnpauseForCheckpoint => {
				// Not supposed to get this outside of a checkpoint
				panic!("Got UnpauseForCheckpoint while not within a checkpoint");
			}
		}
	}
}

#[derive(Debug, Deserialize)]
struct TelegramMetric<'a> {
	name: &'a str,
	timestamp: u64,
	tags: SmallMap<Box<str>, Box<str>>,
	fields: SmallMap<&'a str, Value>,
}

lazy_static! {
	static ref RE_INVALID_CHARS: Regex = Regex::new(r"[^a-zA-Z0-9-_./]").unwrap();
}

fn filter_name(mut s: String) -> String {
	s.make_ascii_lowercase();

	// In-place replacement
	let mut start_index = 0;
	loop {
		// Need to limit match lifetime so that we can modify the string.
		let (start, end) = if let Some(m) = RE_INVALID_CHARS.find_at(&s, start_index) {
			(m.start(), m.end())
		} else {
			break;
		};

		s.replace_range(start..end, "");
		start_index = start
	}
	s
}

fn filter_tags(map: SmallMap<Box<str>, Box<str>>) -> SmallMap<Box<str>, Box<str>> {
	map.into_iter()
		.map(|(k, v)| {
			let k_filtered = filter_name(String::from(k));
			let v_filtered = filter_name(String::from(v));
			(k_filtered.into_boxed_str(), v_filtered.into_boxed_str())
		})
		.filter(|(k, v)| k.as_ref() != "" && v.as_ref() != "")
		.collect::<SmallMap<_, _>>()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_filter_name() {
		assert_eq!(filter_name("helloworld".into()), "helloworld");
		assert_eq!(filter_name("Hello worlD".into()), "helloworld");
		assert_eq!(filter_name("Java Stats 123-45".into()), "javastats123-45");
	}
}
