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


pub mod serde_utils;
pub mod v1;

use num_derive::FromPrimitive;
use num_traits::cast::FromPrimitive;
use rmp_serde;
use serde::{
	de,
	ser::{
		self,
		SerializeSeq as _,
		SerializeStruct as _,
	},
	Deserialize,
	Serialize,
};
use std::{
	borrow::Cow,
	io::{
		self,
		Read,
		Write,
	},
	sync::{
		Arc,
		MutexGuard,
	},
};

use crate::types::*;

#[derive(FromPrimitive)]
pub enum CompressionType {
	Uncompressed = 0x00,
	Gz = 0x01,
	Zstd = 0x02,
}

#[derive(FromPrimitive)]
pub enum FormatType {
	MessagePack = 0x01,
}

/// Writes the state to a `Write` object.
///
/// Common function so that the only thing `CheckpointBackend` implementations need to worry about is
/// opening the file.
pub fn write_state<W: io::Write>(writer: W, state: &[Arc<RollupState>]) -> io::Result<()> {
	let serializable = SerializableRollupState::from(state);
	let header = [CompressionType::Zstd as u8, FormatType::MessagePack as u8];

	let mut writer = io::BufWriter::new(writer);
	writer.write_all(&header)?;
	let mut writer = zstd::Encoder::new(writer, 3)?;
	rmp_serde::encode::write_named(&mut writer, &serializable)
		.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
	writer.finish()?;
	Ok(())
}

/// Reads the state from a `Read` object.
///
/// Common function so that the only thing `CheckpointBackend` implementations need to worry about is
/// opening the file.
pub fn read_state<R: io::Read>(reader: R) -> io::Result<RollupState> {
	let mut reader = io::BufReader::new(reader);

	let mut header: [u8; 2] = [0, 0];
	reader.read_exact(&mut header)?;

	let compression_type = CompressionType::from_u8(header[0]);
	let format_type = FormatType::from_u8(header[1]);

	let reader: Box<dyn Read> = match compression_type {
		Some(CompressionType::Uncompressed) => {
			debug!("State is uncompressed");
			Box::new(reader)
		}
		Some(CompressionType::Gz) => {
			debug!("State is compressed with gzip");
			Box::new(flate2::bufread::GzDecoder::new(reader))
		}
		Some(CompressionType::Zstd) => {
			debug!("State is compressed with zstd");
			Box::new(zstd::Decoder::new(reader)?)
		}
		None => {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				format!("Unrecognized compression id: {}", header[0]),
			));
		}
	};

	let deserialized_res = match format_type {
		Some(FormatType::MessagePack) => {
			debug!("State is serialized with MessagePack");
			rmp_serde::decode::from_read::<_, SerializableRollupState>(reader)
				.map_err(|e| io::Error::new(io::ErrorKind::Other, e))
		}
		None => {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				format!("Unrecognized format id: {}", header[1]),
			));
		}
	};

	deserialized_res.map(|v| {
		v.try_unwrap_deserialized()
			.unwrap_or_else(|_| unreachable!())
	})
}

/// Versioned rollup state.
///
/// You don't usually need to work with this directly; use `read_state` and
/// `write_state` above.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SerializableRollupState<'a> {
	V1(v1::V1RollupState),
	V2(SeparatedRollupState<'a>),
}

impl From<RollupState> for SerializableRollupState<'static> {
	fn from(v: RollupState) -> Self {
		SerializableRollupState::V2(v.into())
	}
}
impl<'a> From<&'a [Arc<RollupState>]> for SerializableRollupState<'a> {
	fn from(arr: &'a [Arc<RollupState>]) -> Self {
		SerializableRollupState::V2(arr.into())
	}
}

impl<'a> SerializableRollupState<'a> {
	/// If the `SerializableRollupState` contains only one rollup state, converts it and returns it.
	/// Otherwise returns self unmodified
	///
	/// This always succeeds if the `SerializableRollupState` was created via serde deserialization.
	pub fn try_unwrap_deserialized(self) -> Result<RollupState, Self> {
		match self {
			SerializableRollupState::V1(state) => Ok(state.into()),
			SerializableRollupState::V2(SeparatedRollupState(Cow::Borrowed(_))) => Err(self),
			SerializableRollupState::V2(SeparatedRollupState(Cow::Owned(vec))) => {
				if vec.len() != 1 {
					return Err(SerializableRollupState::V2(SeparatedRollupState(
						Cow::Owned(vec),
					)));
				}

				let arc = vec.into_iter().next().unwrap();

				match Arc::try_unwrap(arc) {
					Ok(v) => Ok(v),
					Err(arc) => Err(SerializableRollupState::V2(SeparatedRollupState(
						Cow::Owned(vec![arc]),
					))),
				}
			}
		}
	}
}

/// Wrapper for `RollupState` to serialize.
///
/// For serialization, can take several different rollup states, one for each worker,
/// and combine them to serialize them without any copying.
///
/// Deserialization always produces an owned vector with one single-reference Arc.
pub struct SeparatedRollupState<'a>(pub Cow<'a, [Arc<RollupState>]>);

impl<'a> From<RollupState> for SeparatedRollupState<'a> {
	fn from(v: RollupState) -> Self {
		Self(vec![Arc::new(v)].into())
	}
}

impl<'a> From<&'a [Arc<RollupState>]> for SeparatedRollupState<'a> {
	fn from(arr: &'a [Arc<RollupState>]) -> Self {
		Self(arr.into())
	}
}

impl<'a> ser::Serialize for SeparatedRollupState<'a> {
	fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		let mut root = serializer.serialize_struct("RollupState", 3)?;

		// Write windows.
		// Need to gather window size up-front, since flat_map can't calculate
		// size.
		debug!("Serialization: writing windows");
		let num_windows = self.0.iter().map(|state| state.windows.len()).sum();
		root.serialize_field(
			"windows",
			&serde_utils::SerMapIter::with_size(
				self.0.iter().flat_map(|state| state.windows.iter()),
				num_windows,
			),
		)?;

		// Merge event times
		debug!("Serialization: writing event time");
		let merged_event_time = EventTime::merge_iter(self.0.iter().map(|state| &state.event_time));
		root.serialize_field("event_time", &merged_event_time)?;

		// Write pending metrics.
		debug!("Serialization: writing pending aggregates");
		struct SubmittedSerializer<'a>(Vec<MutexGuard<'a, SentList>>);
		impl<'a> Serialize for SubmittedSerializer<'a> {
			fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
				let len = self
					.0
					.iter()
					.map(|list| list.sent_len() + list.to_send_len())
					.sum();
				let mut seq = serializer.serialize_seq(Some(len))?;
				for list in self.0.iter() {
					for (_, tup) in list.sent_iter() {
						seq.serialize_element(tup)?;
					}
					for tup in list.to_send_iter() {
						seq.serialize_element(tup)?;
					}
				}
				seq.end()
			}
		}
		let locks = self
			.0
			.iter()
			.map(|state| state.submitted.lock().expect("submitted lock poisoned"))
			.collect::<Vec<_>>();
		root.serialize_field("submitted", &SubmittedSerializer(locks))?;

		root.end()
	}
}
impl<'a, 'de> de::Deserialize<'de> for SeparatedRollupState<'a> {
	fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		let state = RollupState::deserialize(deserializer)?;
		Ok(Self::from(state))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::{
		collections::{
			BTreeMap,
			HashMap,
		},
		sync::{
			Arc,
			Mutex,
		},
	};

	#[test]
	fn state_serialization() {
		let window_duration = OpenTsdbDuration {
			count: 10,
			unit: OpenTsdbTimeUnit::Minute,
		};

		let window1 = (
			OpenTsdbKey::new("metric1".into(), Default::default()),
			WindowSet {
				windows: vec![Window {
					start: 123,
					duration: window_duration.clone(),
					aggregated_point: AggregatedPoint {
						min: -1.,
						max: 1.,
						sum: 42.,
						count: 70,
					},
				}],
			},
		);

		let window2 = (
			OpenTsdbKey::new("metric2".into(), Default::default()),
			WindowSet {
				windows: vec![Window {
					start: 999,
					duration: window_duration.clone(),
					aggregated_point: AggregatedPoint {
						min: 10.,
						max: 40.,
						sum: 80.,
						count: 6,
					},
				}],
			},
		);

		let state1 = RollupState {
			windows: vec![window1.clone()].into_iter().collect(),
			event_time: EventTime::from(
				vec![(
					"topic1".into(),
					vec![
						(
							1,
							TopicPartTime {
								offset: 123,
								timestamp: 1000,
							},
						),
						(
							2,
							TopicPartTime {
								offset: 456,
								timestamp: 2000,
							},
						),
					]
					.into_iter()
					.collect::<BTreeMap<_, _>>(),
				)]
				.into_iter()
				.collect::<HashMap<_, _>>(),
			),
			submitted: Arc::new(Mutex::new(SentList::new())),
		};
		let state2 = RollupState {
			windows: vec![window2.clone()].into_iter().collect(),
			event_time: EventTime::from(
				vec![
					(
						"topic1".into(),
						vec![(
							1,
							TopicPartTime {
								offset: 777,
								timestamp: 900,
							},
						)]
						.into_iter()
						.collect::<BTreeMap<_, _>>(),
					),
					(
						"topic2".into(),
						vec![(
							1,
							TopicPartTime {
								offset: 222,
								timestamp: 100,
							},
						)]
						.into_iter()
						.collect::<BTreeMap<_, _>>(),
					),
				]
				.into_iter()
				.collect::<HashMap<_, _>>(),
			),
			submitted: Arc::new(Mutex::new(SentList::new())),
		};

		let states = vec![Arc::new(state1), Arc::new(state2)];

		let serialized =
			rmp_serde::to_vec_named(&SerializableRollupState::from(states.as_slice())).unwrap();

		let deserialized = rmp_serde::from_slice::<SerializableRollupState>(&serialized)
			.unwrap()
			.try_unwrap_deserialized()
			.unwrap_or_else(|_| unreachable!());

		assert_eq!(
			deserialized.windows,
			vec![window1.clone(), window2.clone()].into_iter().collect()
		);
		assert_eq!(
			deserialized.event_time,
			EventTime::from(
				vec![
					(
						"topic1".into(),
						vec![
							(
								1,
								TopicPartTime {
									offset: 777,
									timestamp: 1000
								}
							),
							(
								2,
								TopicPartTime {
									offset: 456,
									timestamp: 2000
								}
							),
						]
						.into_iter()
						.collect::<BTreeMap<_, _>>()
					),
					(
						"topic2".into(),
						vec![(
							1,
							TopicPartTime {
								offset: 222,
								timestamp: 100
							}
						),]
						.into_iter()
						.collect::<BTreeMap<_, _>>()
					),
				]
				.into_iter()
				.collect::<HashMap<_, _>>()
			)
		);
	}
}
