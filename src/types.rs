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


//! Common types used throughout the program.

// READ BEFORE EDITING
// READ BEFORE EDITING
// READ BEFORE EDITING
//
// If you modify a type in a way that affects serialization, you will need to copy
// the old structures into `serialized`, make a new version, and write conversion code
// to load the old checkpoints.

use serde::{
	de,
	ser::{
		self,
		SerializeSeq as _,
	},
	Deserialize,
	Serialize,
};
use std::{
	cmp::Ordering,
	collections::{
		BTreeMap,
		HashMap,
		VecDeque,
	},
	fmt,
	hash::{
		Hash,
		Hasher,
	},
	str::FromStr,
	sync::{
		Arc,
		Mutex,
	},
};

use crate::{
	smallmap::SmallMap,
	util::HashMapExt,
};

/// Complete rollup state.
///
/// May be for the whole program or just one worker.
#[derive(Default, Clone, Serialize, Deserialize)]
pub struct RollupState {
	pub windows: HashMap<OpenTsdbKey, WindowSet>,
	pub event_time: EventTime,
	pub submitted: Arc<Mutex<SentList>>,
}
impl RollupState {
	/// Splits up a single combined state into one state for each rollup worker, distributing in-progress windows
	/// the same way new metrics would be assigned.
	pub fn distribute(self, num: usize) -> Vec<Self> {
		let mut submitted = Some(self.submitted);
		let event_time = self.event_time;
		let mut states = (0..num)
			.map(|_| Self {
				windows: HashMap::new(),
				event_time: event_time.clone(),
				submitted: submitted
					.take()
					.unwrap_or_else(|| Arc::new(Mutex::new(SentList::new()))),
			})
			.collect::<Vec<_>>();

		for (key, window_set) in self.windows.into_iter() {
			let index = key.distribute_index(num);
			states[index].windows.insert(key, window_set);
		}

		states
	}
}

/// Set of open windows for an OpenTSDB key
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct WindowSet {
	/// List of open windows for this opentsdb key
	pub windows: Vec<Window>,
}
impl WindowSet {
	/// Gets the window for a start time and duration, creating one if it does not exist.
	pub fn get_or_create_window<'a, 'b>(
		&'a mut self,
		start: u64,
		duration: &'b OpenTsdbDuration,
	) -> &'a mut Window {
		{
			if let Some(window_index) = self
				.windows
				.iter()
				.position(|v| v.start == start && v.duration == *duration)
			{
				return &mut self.windows[window_index];
			}
		}

		let last_index = self.windows.len();
		self.windows.push(Window {
			start,
			duration: duration.clone(),
			aggregated_point: Default::default(),
		});
		return &mut self.windows[last_index];
	}
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Window {
	/// Window start time in ms since unix epoch
	pub start: u64,
	/// Window duration in milliseconds
	pub duration: OpenTsdbDuration,
	pub aggregated_point: AggregatedPoint,
}

/// Aggregated metric values over a single window
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AggregatedPoint {
	/// Minimum metric value over the window
	pub min: f64,
	/// Maximum metric value over the window
	pub max: f64,
	/// Sum of all metric values over the window
	pub sum: f64,
	/// Number of metrics accumulated over the window
	pub count: u64,
}
impl AggregatedPoint {
	/// Creates a new aggregated metric with starting values.
	///
	/// min starts as +inf, max starts as -inf, and sum and conut start as 0.
	///
	/// Same as `Default::default()`.
	pub fn new() -> Self {
		Self {
			min: std::f64::INFINITY,
			max: std::f64::NEG_INFINITY,
			sum: 0.,
			count: 0,
		}
	}

	/// Adds a value to the point.
	///
	/// Calculates a new min+max, adds the value to sum, and adds 1 to
	/// the count.
	pub fn add(&mut self, value: f64) {
		self.min = self.min.min(value);
		self.max = self.max.max(value);
		self.sum += value;
		self.count += 1;
	}
}
impl Default for AggregatedPoint {
	fn default() -> Self {
		Self::new()
	}
}

/// OpenTSDB "key"; a metric name and map of tags.
///
/// Represents a unique metric line. Hashable.
#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub struct OpenTsdbKey {
	// Use boxed str instead of String since this immutable type doesn't
	// need capacity.
	/// Metric name
	name: Box<str>,
	/// Metric tags.
	///
	/// Shared (via Arc) between several keys to reduce allocations.
	#[serde(serialize_with = "arc_ser", deserialize_with = "arc_de")]
	tags: Arc<SmallMap<Box<str>, Box<str>>>,
	/// Cached hash of this key
	hash: u64,
}
impl OpenTsdbKey {
	pub fn new(name: Box<str>, tags: Arc<SmallMap<Box<str>, Box<str>>>) -> Self {
		let mut hasher = std::collections::hash_map::DefaultHasher::new();
		name.hash(&mut hasher);
		tags.hash(&mut hasher);
		let hash = hasher.finish();
		Self { name, tags, hash }
	}

	pub fn name(&self) -> &str {
		&*self.name
	}

	pub fn tags(&self) -> &SmallMap<Box<str>, Box<str>> {
		&*self.tags
	}

	/// Finds which worker should process metrics with this key.
	pub fn distribute_index(&self, num_slots: usize) -> usize {
		return (self.hash as usize) % num_slots;
	}
}
impl Hash for OpenTsdbKey {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.hash.hash(state)
	}
}
impl fmt::Display for OpenTsdbKey {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(&self.name)?;
		f.write_str("{")?;
		for (i, (k, v)) in self.tags.iter().enumerate() {
			if i != 0 {
				f.write_str(",")?;
			}
			write!(f, "{}={}", k, v)?;
		}
		f.write_str("}")
	}
}
impl PartialOrd for OpenTsdbKey {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(
			self.name
				.cmp(&other.name)
				.then_with(|| self.tags.cmp(&other.tags)),
		)
	}
}
impl Ord for OpenTsdbKey {
	fn cmp(&self, other: &Self) -> Ordering {
		self.name
			.cmp(&other.name)
			.then_with(|| self.tags.cmp(&other.tags))
	}
}

/// Unique data point
#[derive(Debug, Clone)]
pub struct Metric {
	/// Metric key
	pub key: OpenTsdbKey,
	/// Timestamp that the metric was collected at, in ms since unix epoch
	pub timestamp: u64,
	/// Metric value
	pub value: f64,
	/// Kafka topic that this metric came from
	pub topic: String,
	/// Kafka partition that this metric came from
	pub partition: i32,
	/// Kafka offset within the topic+partition that this metric came from
	pub offset: i64,
}

/// OpenTSDB units
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OpenTsdbTimeUnit {
	Day,
	Hour,
	Minute,
	Second,
	Millisecond,
}
impl OpenTsdbTimeUnit {
	/// Number of milliseconds in a single one of these units
	pub fn ms_multiplier(&self) -> u64 {
		match self {
			OpenTsdbTimeUnit::Day => 24 * 60 * 60 * 1000,
			OpenTsdbTimeUnit::Hour => 60 * 60 * 1000,
			OpenTsdbTimeUnit::Minute => 60 * 1000,
			OpenTsdbTimeUnit::Second => 1000,
			OpenTsdbTimeUnit::Millisecond => 1,
		}
	}

	/// Gets the OpenTSDB compatible suffix for this unit, as a static string.
	pub fn to_static_str(&self) -> &'static str {
		match self {
			OpenTsdbTimeUnit::Day => "d",
			OpenTsdbTimeUnit::Hour => "h",
			OpenTsdbTimeUnit::Minute => "m",
			OpenTsdbTimeUnit::Second => "s",
			OpenTsdbTimeUnit::Millisecond => "ms",
		}
	}
}
impl FromStr for OpenTsdbTimeUnit {
	type Err = String;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s.trim() {
			"d" => Ok(OpenTsdbTimeUnit::Day),
			"h" => Ok(OpenTsdbTimeUnit::Hour),
			"m" => Ok(OpenTsdbTimeUnit::Minute),
			"s" => Ok(OpenTsdbTimeUnit::Second),
			"ms" => Ok(OpenTsdbTimeUnit::Millisecond),
			_ => Err(format!("Unrecognized OpenTSDB time unit: {:?}", s)),
		}
	}
}
impl fmt::Display for OpenTsdbTimeUnit {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(self.to_static_str())
	}
}

/// Duration format for OpenTSDB. Includes a unit, since in OpenTSDB, equivalent durations
/// with different units are distinct in some contexts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OpenTsdbDuration {
	/// Number of units for this duration.
	pub count: u64,
	/// Unit that `count` is in
	pub unit: OpenTsdbTimeUnit,
}
impl OpenTsdbDuration {
	/// Gets the duration in milliseconds
	pub fn as_ms(&self) -> u64 {
		self.count * self.unit.ms_multiplier()
	}
}
impl fmt::Display for OpenTsdbDuration {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}{}", self.count, self.unit)
	}
}

/// Struct for managing event time.
///
/// The event time is the minimum timestamp last seen across topics and partitions.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct EventTime {
	timestamps: HashMap<String, BTreeMap<i32, TopicPartTime>>,
}
impl EventTime {
	/// Creates a new event timer with zero timestamps in all partitions.
	pub fn new() -> Self {
		Self {
			timestamps: HashMap::new(),
		}
	}

	/// Updates the time for a partition.
	pub fn update(&mut self, topic: &str, partition: i32, timestamp: u64, offset: i64) {
		self.timestamps
			.entry_to_owned(topic)
			.or_insert_with(|| BTreeMap::new())
			.entry(partition)
			.and_modify(|v| {
				v.timestamp = v.timestamp.max(timestamp);
				v.offset = v.offset.max(offset);
			})
			.or_insert(TopicPartTime { timestamp, offset });
	}

	/// Gets the current event time
	pub fn time(&self) -> Option<u64> {
		self.timestamps
			.values()
			.flat_map(|parts| parts.values().map(|t| t.timestamp))
			.min()
	}

	/// Gets the offset for a topic+partition pair
	pub fn offset(&self, topic: &str, partition: i32) -> Option<i64> {
		let offset = self.timestamps.get(topic)?.get(&partition)?.offset;
		Some(offset)
	}

	/// Merges times with another clock
	pub fn merge(&mut self, other: &Self) {
		for (topic, parts) in other.timestamps.iter() {
			for (partition, t) in parts.iter() {
				self.update(topic, *partition, t.timestamp, t.offset);
			}
		}
	}

	/// Iterate over all recorded topics, partitions, and timestamps/offsets
	pub fn iter(&self) -> impl Iterator<Item = (&str, i32, TopicPartTime)> {
		self.timestamps.iter().flat_map(|(topic, parts)| {
			parts
				.iter()
				.map(move |(partition, t)| (topic.as_str(), *partition, *t))
		})
	}

	/// Merges event time clocks from an iterator.
	pub fn merge_iter<'a, I: Iterator<Item = &'a Self>>(iter: I) -> Self {
		let mut merged_event_time = Self::new();
		for i in iter {
			merged_event_time.merge(i);
		}
		merged_event_time
	}
}
impl Default for EventTime {
	fn default() -> Self {
		Self::new()
	}
}
impl From<HashMap<String, BTreeMap<i32, TopicPartTime>>> for EventTime {
	fn from(v: HashMap<String, BTreeMap<i32, TopicPartTime>>) -> Self {
		Self { timestamps: v }
	}
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicPartTime {
	pub timestamp: u64,
	pub offset: i64,
}

/// List of metrics to send and in-flight aggregated datapoints.
///
/// Contains a queue of metrics that need to be sent, as well as a set of metrics
/// that have been sent but haven't been confirmed to be in the topic yet.
pub struct SentList {
	to_send: VecDeque<(OpenTsdbKey, Window)>,
	// TODO: may be more efficient as a circular buffer of some sort
	sent: HashMap<usize, (OpenTsdbKey, Window)>,
	next: usize,
}
impl SentList {
	/// Creates a new empty sent list
	pub fn new() -> Self {
		Self {
			to_send: VecDeque::new(),
			sent: HashMap::new(),
			next: 1,
		}
	}

	/// Enqueues a metric into the send queue
	pub fn to_send_enqueue(&mut self, key: OpenTsdbKey, window: Window) {
		self.to_send.push_back((key, window));
	}

	/// Pops an element from the send queue and moves it to the sent set, returning
	/// its sent id and the aggregate point info.
	///
	/// The code using this should actually send this after calling this, and call
	/// `sent_ok` or `sent_err` when finished.
	pub fn to_send_pop(&mut self) -> Option<(usize, &(OpenTsdbKey, Window))> {
		let elem = self.to_send.pop_front()?;

		let id = self.next;
		self.next = self.next.wrapping_add(1);

		let prev = self.sent.insert(id, elem);
		assert!(prev.is_none());

		let elem_ref = self.sent.get(&id).unwrap();

		return Some((id, elem_ref));
	}

	/// Records that a record had been added to the topic OK.
	///
	/// Clears the point from the sent list.
	pub fn sent_ok(&mut self, id: usize) {
		self.sent.remove(&id);
	}

	/// Records that a record has failed to make it to the topic.
	///
	/// Moves the metric back onto the send queue
	pub fn sent_err(&mut self, id: usize) {
		if let Some(v) = self.sent.remove(&id) {
			self.to_send.push_back(v);
		}
	}

	/// Gets the aggregate point for the corresponding id
	pub fn sent_get(&self, id: usize) -> Option<&(OpenTsdbKey, Window)> {
		self.sent.get(&id)
	}

	/// Iterates over in-flight requests
	pub fn sent_iter(&self) -> std::collections::hash_map::Iter<usize, (OpenTsdbKey, Window)> {
		self.sent.iter()
	}

	/// Gets the number of in-flight requests
	pub fn sent_len(&self) -> usize {
		self.sent.len()
	}

	/// Iterates over metrics to send, without consuming them
	pub fn to_send_iter(&self) -> std::collections::vec_deque::Iter<(OpenTsdbKey, Window)> {
		self.to_send.iter()
	}

	/// Gets the number of requests in the send queue
	pub fn to_send_len(&self) -> usize {
		self.to_send.len()
	}

	/// Shrinks the internal queues and maps to reduce memory footprint.
	pub fn shrink(&mut self) {
		let cap = self.to_send.capacity();
		let len = self.to_send.len();
		if cap > 1000000 / 2 && (len == 0 || cap / len > 3) {
			self.to_send.shrink_to_fit();
		}

		let cap = self.sent.capacity();
		let len = self.sent.len();
		if cap > 1000000 / 2 && (len == 0 || cap / len > 3) {
			self.sent.shrink_to_fit();
		}
	}
}
impl ser::Serialize for SentList {
	fn serialize<S: ser::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
		let mut seq = s.serialize_seq(Some(self.sent.len() + self.to_send.len()))?;
		for tup in self.sent.keys() {
			seq.serialize_element(tup)?;
		}
		for tup in self.to_send.iter() {
			seq.serialize_element(tup)?;
		}
		seq.end()
	}
}
impl<'de> de::Deserialize<'de> for SentList {
	fn deserialize<D: de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
		let resend = VecDeque::<(OpenTsdbKey, Window)>::deserialize(d)?;
		Ok(Self {
			to_send: resend,
			sent: HashMap::new(),
			next: 1,
		})
	}
}
impl Default for SentList {
	fn default() -> Self {
		Self::new()
	}
}

/// Serialize an object contained in an Arc
fn arc_ser<T: Serialize, S: serde::ser::Serializer>(v: &Arc<T>, s: S) -> Result<S::Ok, S::Error> {
	(**v).serialize(s)
}
/// Deserialize an object, wrapping it in a new Arc
fn arc_de<'de, T: Deserialize<'de>, D: serde::de::Deserializer<'de>>(
	d: D,
) -> Result<Arc<T>, D::Error> {
	T::deserialize(d).map(|v| Arc::new(v))
}
