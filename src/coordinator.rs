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


//! Coordinator worker, the part that starts the other workers and handles checkpointing.

use crossbeam_channel as cbc;
use rand::{
	seq::SliceRandom,
	thread_rng,
};
use rdkafka::{
	consumer::{
		BaseConsumer,
		Consumer,
	},
	ClientConfig,
	Offset,
};
use std::{
	collections::HashMap,
	fmt,
	io::{
		self,
		Read,
	},
	os::unix::net::UnixStream,
	sync::Arc,
	thread,
	time::{
		Duration,
		Instant,
	},
};

use crate::{
	blacklist::Blacklist,
	checkpoint_backend::CheckpointBackend,
	consumer::{
		ConsumerMessage,
		ConsumerWorker,
		ConsumerWorkerSettings,
	},
	influx::InfluxReporter,
	memmetrics::MemMetrics,
	rollup::{
		RollupMessage,
		RollupWorker,
		RollupWorkerSettings,
		WindowAssigner,
	},
	types::*,
};

/// Messages to send to the coordinator
#[derive(Clone)]
pub enum CoordinatorMessage {
	/// Consumer has paused.
	/// Consumers send this in response to `ConsumerMessage::PauseForCheckpoint` with their id.
	ConsumerPaused(usize),
	/// Rollup checkpoint state.
	/// Rollup workers send this in response to `RollupMessage::Checkpoint`, so that the
	/// coordinator can borrow its state to serialize it.
	/// The coordinator must drop the `Arc` before resuming ingest so that the worker can
	/// get mutable access to its state.
	CheckpointState(usize, Arc<RollupState>),
}
impl fmt::Debug for CoordinatorMessage {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			CoordinatorMessage::ConsumerPaused(s) => f
				.debug_tuple("CoordinatorMessage::ConsumerPaused")
				.field(&s)
				.finish(),
			CoordinatorMessage::CheckpointState(id, _) => f
				.debug_tuple("CoordinatorMessage::CheckpointState")
				.field(&id)
				.field(&"<state>")
				.finish(),
		}
	}
}

/// Settings for the service
pub struct Settings<Assigner: WindowAssigner> {
	/// Program-unique ID, used in Kafka client IDs and metrics.
	pub instance_id: String,
	/// Kafka topics to ingest from
	pub ingest_topics: Vec<String>,
	/// Kafka topic to write rolled up metrics to
	pub dest_topic: String,
	/// Number of threads to spawn for polling kafka and parsing metrics
	pub num_consumer_workers: usize,
	/// Number of threads to spawn for aggregating metrics and submitting them to kafka
	pub num_rollup_workers: usize,
	/// Configuration for Kafka consumers
	pub consumer_config: ClientConfig,
	/// Configuration for Kafka producers
	pub producer_config: ClientConfig,
	/// Allowed lateness of metrics, in milliseconds.
	///
	/// Windows will be held open and submission delayed for at least this amount of milliseconds after
	/// the real closing time, allowing late metrics to still be counted. Late metrics after the allowed
	/// lateness are dropped.
	pub allowed_lateness: u64,
	/// If specified, drop metrics this many ms in the future according to this system's clock.
	pub future_cutoff: Option<u64>,
	/// The window assigner. Returns a list of windows to add metrics into, depending on the metric+tags.
	pub window_assigner: Assigner,
	/// Size of buffers between workers.
	pub queue_buffer_size: usize,
	/// How often to do checkpoints. Less time means less work that has to be redone in case of a crash,
	/// but checkpointing is an expensive operation that pauses ingest.
	pub checkpoint_interval: Duration,
	/// Storage implementation to use for storing checkpoints.
	pub checkpoint_backend: Box<dyn CheckpointBackend>,
	/// Interval for process memory metrics
	pub mem_metrics_interval: Duration,

	/// Signal IDs of the registered signal handlers.
	///
	/// The program can set up a signal handler to write to a pipe in order to perform a clean shutdown,
	/// where the program checkpoints before exiting.
	///
	/// See the `sianl_hook` documentation on how to set it up.
	pub interrupt_sigids: Vec<signal_hook::SigId>,
	/// Stream to read signals from. See `interrupt_sigids` for more info.
	pub interrupt_signal_stream: UnixStream,

	/// Optional metric writer. If set, will write several metrics to it to monitor progress.
	pub metrics: Option<InfluxReporter>,

	pub blacklist: Blacklist,
}

/// Starts up worker threads and begins processes.
///
/// Runs "forever", only returns if a signal handler was registered and it caught a closing signal.
/// Panics on error.
///
/// Returns a string error message if initialization failed
pub fn run<Assigner: 'static + WindowAssigner>(
	mut settings: Settings<Assigner>,
) -> Result<(), String> {
	// Setup resources
	let (coordinator_sender, coordinator_receiver) = cbc::unbounded::<CoordinatorMessage>();

	let (consumer_senders, consumer_receivers) =
		create_channels(settings.num_consumer_workers, settings.queue_buffer_size);
	let (rollup_senders, rollup_receivers) =
		create_channels(settings.num_rollup_workers, settings.queue_buffer_size);
	let (memmetrics_sender, memmetrics_receiver) = cbc::unbounded::<()>();

	let mut checkpoint_backend = settings.checkpoint_backend;
	info!("Loading checkpoint");
	let start_state = match checkpoint_backend.load() {
		Ok(Some(s)) => {
			info!("Loaded checkpoint");
			for (topic, partition, t) in s.event_time.iter() {
				info!(
					"\tPartiton {}/{} will start at offset {}",
					topic, partition, t.offset
				);
			}
			s
		}
		Ok(None) => {
			warn!("No checkpoint file found, starting from scratch");
			Default::default()
		}
		Err(e) => {
			return Err(format!("Could not load checkpoint: {}", e));
		}
	};

	// Move stuff out of `settings` so closure can borrow them
	let instance_id = settings.instance_id;
	let consumer_config = settings.consumer_config;
	let producer_config = settings.producer_config;
	let ingest_topics = settings.ingest_topics;
	let window_assigner = settings.window_assigner;
	let dest_topic = settings.dest_topic;
	let allowed_lateness = settings.allowed_lateness;
	let metrics = settings.metrics.map(|v| Arc::new(v));
	let future_cutoff = settings.future_cutoff;
	let blacklist = settings.blacklist;

	// Fetch list of topics and partitions with a temporary client.
	// Do it in a scope to close the temporary connection afterwards.
	let topics_for_consumers = {
		let metadata_client = consumer_config
			.create::<BaseConsumer>()
			.map_err(|e| format!("Could not create metadata client: {}", e))?;
		let metadata = metadata_client
			.fetch_metadata(None, None)
			.map_err(|e| format!("Could not get topic metadata: {}", e))?;
		let mut all_topics = metadata
			.topics()
			.iter()
			.filter(|t1| ingest_topics.iter().any(|t2| t1.name() == t2))
			.flat_map(|topic| {
				topic
					.partitions()
					.iter()
					.map(move |partition| (String::from(topic.name()), partition.id()))
			})
			.collect::<Vec<_>>();

		// Shuffle topics then assign an even number of topics to consumers.
		// TODO: balance based on topic business somehow?
		let mut rng = thread_rng();
		all_topics.shuffle(&mut rng);

		let mut buckets = vec![HashMap::new(); consumer_receivers.len()];
		for (i, (topic, partition)) in all_topics.into_iter().enumerate() {
			let offset = start_state
				.event_time
				.offset(&topic, partition)
				.map(|offset| Offset::Offset(offset))
				.unwrap_or(Offset::Stored);

			let index = i % buckets.len();
			buckets[index].insert((topic, partition), offset);
		}
		buckets
	};

	let states = start_state.distribute(settings.num_rollup_workers);

	// Spawn consumer worker threads
	let iter = consumer_receivers
		.into_iter()
		.zip(topics_for_consumers.into_iter())
		.enumerate();
	for (i, (receiver, assignment)) in iter {
		let settings = ConsumerWorkerSettings {
			id: i,
			instance_id: instance_id.clone(),
			assignment,
			future_cutoff: future_cutoff.clone(),
			client_cfg: consumer_config.clone(),
			coordinator_sender: coordinator_sender.clone(),
			receiver,
			rollup_senders: rollup_senders.clone(),
			metrics_sender: metrics.as_ref().map(|v| Arc::clone(v)),
			blacklist: blacklist.clone(),
		};
		let mut worker = ConsumerWorker::new(settings)
			.map_err(|e| format!("Could not create consumer worker: {}", e))?;

		thread::Builder::new()
			.name(format!("consumer-{}", i))
			.spawn(move || {
				worker.run();
			})
			.map_err(|e| format!("Could not spawn consumer thread: {}", e))?;
	}

	// Spawn rollup worker threads
	let iter = rollup_receivers
		.into_iter()
		.zip(states.into_iter())
		.enumerate();
	for (i, (receiver, state)) in iter {
		let settings = RollupWorkerSettings {
			id: i,
			instance_id: instance_id.clone(),
			client_cfg: producer_config.clone(),
			coordinator_sender: coordinator_sender.clone(),
			receiver: Arc::new(receiver),
			metrics_sender: metrics.as_ref().map(Arc::clone),
			assigner: window_assigner.clone(),
			dest_topic: dest_topic.clone(),
			submit_interval: Duration::from_secs(10),
			allowed_lateness,
			state: Arc::new(state),
		};

		let mut worker = RollupWorker::new(settings)
			.map_err(|e| format!("Could not create rollup worker: {}", e))?;

		thread::Builder::new()
			.name(format!("rollup-{}", i))
			.spawn(move || {
				worker.run();
			})
			.map_err(|e| format!("Could not spawn rollup worker: {}", e))?;
	}

	// Spawn memory metrics thread
	if let Some(metrics) = metrics.as_ref().map(|arc| Arc::clone(arc)) {
		let mut memmetrics = MemMetrics::new(
			instance_id.clone(),
			memmetrics_receiver,
			settings.mem_metrics_interval,
			metrics,
		);

		thread::Builder::new()
			.name(String::from("mem-metrics"))
			.spawn(move || {
				memmetrics.run();
			})
			.map_err(|e| format!("Could not spawn memory metrics thread: {}", e))?;
	}

	// Workers are now running. Now we just handle checkpoints and closing
	main_loop(
		&settings.checkpoint_interval,
		&coordinator_receiver,
		&consumer_senders,
		&rollup_senders,
		&memmetrics_sender,
		&mut settings.interrupt_signal_stream,
		settings.interrupt_sigids,
		&mut *checkpoint_backend,
	);

	info!("Coordinator exiting");
	Ok(())
}

fn main_loop(
	checkpoint_interval: &Duration,
	coordinator_receiver: &cbc::Receiver<CoordinatorMessage>,
	consumer_senders: &Vec<cbc::Sender<ConsumerMessage>>,
	rollup_senders: &Vec<cbc::Sender<RollupMessage>>,
	_memmetrics_sender: &cbc::Sender<()>,
	interrupt_signal_stream: &mut UnixStream,
	interrupt_sigids: Vec<signal_hook::SigId>,
	checkpoint_backend: &mut dyn CheckpointBackend,
) {
	let mut interrupt_sigids = Some(interrupt_sigids);
	let mut do_continue = true;
	let mut next_checkpoint = Instant::now() + *checkpoint_interval;
	loop {
		// Wait for next checkpoint
		loop {
			// Are we past the checkpoint time?
			let now = Instant::now();
			if now >= next_checkpoint {
				next_checkpoint = now + *checkpoint_interval;
				break;
			}

			// Wait by blocking on the signal pipe
			if !wait_on_signal(next_checkpoint - now, interrupt_signal_stream) {
				// Got interrupt signal. Do one last checkpoint and exit
				info!("Got interrupt signal, saving checkpoint and exiting");
				if let Some(sigids) = interrupt_sigids.take() {
					sigids.into_iter().for_each(|s| {
						signal_hook::unregister(s);
					});
				}
				do_continue = false;
				break;
			}
		}

		if let Err(err) = checkpoint(
			coordinator_receiver,
			consumer_senders,
			rollup_senders,
			checkpoint_backend,
			do_continue,
		) {
			error!("Saving checkpoint failed! Error: {}", err);
		}
		if !do_continue {
			break;
		}
	}
}

/// Helper function that creates channels and assembles them into a vector of senders and receivers.
fn create_channels<T: Send>(
	num: usize,
	queue_buffer_size: usize,
) -> (Vec<cbc::Sender<T>>, Vec<cbc::Receiver<T>>) {
	let mut senders = Vec::with_capacity(num);
	let mut receivers = Vec::with_capacity(num);
	for _i in 0..num {
		let (sender, receiver) = cbc::bounded(queue_buffer_size);
		senders.push(sender);
		receivers.push(receiver);
	}
	return (senders, receivers);
}

/// Waits by doing a blocking read on a signal stream.
///
/// Somewhat complicated cuz it has to handle the interrupted error.
///
/// Returns `true` if uninterrupted, `false` if interrupted.
fn wait_on_signal(sleep_time: Duration, stream: &mut UnixStream) -> bool {
	// Set up socket to wait for up to the duration
	stream
		.set_nonblocking(false)
		.expect("Interrupt stream IO error");
	stream
		.set_read_timeout(Some(sleep_time))
		.expect("Interrupt stream IO error");

	loop {
		let mut buf = [0];
		match stream.read(&mut buf) {
			Ok(0) => {
				return true;
			}
			Err(ref e) if e.kind() == io::ErrorKind::TimedOut => {
				return true;
			}
			Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
				return true;
			}
			Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {
				// Signal interrupted our read. Re-read in non-bocking mode to see if
				// the interrupting signal wrote to our handler.
				stream
					.set_nonblocking(false)
					.expect("Interrupt stream IO error");
				continue;
			}
			Ok(_) => {
				// Got interrupted
				return false;
			}
			res @ Err(_) => {
				res.expect("Interrupt stream IO error");
			}
		};
	}
}

fn checkpoint(
	coordinator_receiver: &cbc::Receiver<CoordinatorMessage>,
	consumer_senders: &Vec<cbc::Sender<ConsumerMessage>>,
	rollup_senders: &Vec<cbc::Sender<RollupMessage>>,
	checkpoint_backend: &mut dyn CheckpointBackend,
	resume: bool,
) -> Result<(), String> {
	info!("Beginning checkpoint");
	let start = Instant::now();

	// Pause all consumers, then wait for all to stop
	for sender in consumer_senders {
		sender
			.send(ConsumerMessage::PauseForCheckpoint)
			.expect("Consumer worker unexpectedly closed channel");
	}

	let mut paused = vec![false; consumer_senders.len()];
	while paused.iter().any(|has_paused| !has_paused) {
		match coordinator_receiver.recv_timeout(Duration::from_secs(60 * 5)) {
			Ok(CoordinatorMessage::ConsumerPaused(id)) => {
				if paused[id] {
					panic!(
						"Consumer worker {} sent multiple ConsumerPaused messages",
						id
					);
				}
				paused[id] = true;
			}
			Ok(msg) => {
				panic!("Unexpected message: {:?}", msg);
			}
			Err(cbc::RecvTimeoutError::Timeout) => {
				panic!(
					"Timed out waiting for consumer workers to pause. Paused consumers: {:?}",
					paused
				);
			}
			Err(cbc::RecvTimeoutError::Disconnected) => {
				panic!("Unexpected disconnect");
			}
		}
	}

	// Get rollup state
	for sender in rollup_senders {
		sender
			.send(RollupMessage::Checkpoint)
			.expect("Rollup worker unexpectedly closed channel");
	}
	let mut states = vec![None; rollup_senders.len()];
	while states.iter().any(|state| state.is_none()) {
		match coordinator_receiver.recv_timeout(Duration::from_secs(60 * 5)) {
			Ok(CoordinatorMessage::CheckpointState(id, state)) => {
				if states[id].is_some() {
					panic!(
						"Rollup worker {} sent multiple CheckpointState messages",
						id
					);
				}
				states[id] = Some(state);
			}
			Ok(msg) => {
				panic!("Unexpected message: {:?}", msg);
			}
			Err(cbc::RecvTimeoutError::Timeout) => {
				panic!(
					"Timed out waiting for rollup workers to pause. Paused consumers: {:?}",
					paused
				);
			}
			Err(cbc::RecvTimeoutError::Disconnected) => {
				panic!("Unexpected disconnect");
			}
		}
	}

	// Unwrap all Options in the states array now that they are all filled.
	let states = states.into_iter().map(|s| s.unwrap()).collect::<Vec<_>>();

	// Save state
	let result = checkpoint_backend.save(&states);
	let end = Instant::now();

	// Drop states before resuming to release the `Arc` references and let
	// rollup workers access it mutably again.
	std::mem::drop(states);

	if resume {
		for sender in consumer_senders {
			sender
				.send(ConsumerMessage::UnpauseForCheckpoint)
				.expect("Consumer worker unexpectedly closed channel");
		}
	}

	info!("Checkpointing took {}ms", (end - start).as_millis());

	result
}
