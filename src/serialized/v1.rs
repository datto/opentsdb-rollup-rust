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


//! Version 1 Checkpoint Format
//!
//! Event time stored per-key, but that makes it impossible to
//! remove dead keys from rollup.

use serde::{
	Deserialize,
	Serialize,
};
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

use crate::types::*;

#[derive(Serialize, Deserialize)]
pub struct V1RollupState {
	pub windows: HashMap<OpenTsdbKey, V1WindowSet>,
	pub latest_offsets: HashMap<String, HashMap<i32, i64>>,
}

#[derive(Serialize, Deserialize)]
pub struct V1WindowSet {
	/// Watermark for this set, indicating how far in "event time" we are into
	/// the kafka stream.
	///
	/// Map of partitions to unix timestamps (ms)
	pub watermarks: BTreeMap<i32, u64>,
	/// List of open windows for this opentsdb key
	pub windows: Vec<Window>,
}

impl Into<RollupState> for V1RollupState {
	fn into(self) -> RollupState {
		// Convert offsets to half of EventTime
		let timestamps = self
			.latest_offsets
			.into_iter()
			.map(|(topic, parts_offsets)| {
				(
					topic,
					parts_offsets
						.into_iter()
						.map(|(part, offset)| {
							(
								part,
								TopicPartTime {
									timestamp: 0,
									offset,
								},
							)
						})
						.collect::<BTreeMap<_, _>>(),
				)
			})
			.collect::<HashMap<_, _>>();

		// Convert windowsets.
		// Have to drop watermarks since we don't know what topics they go to.
		// Shouldn't be too big of a deal since new metrics should update the watermarks
		let windowsets = self
			.windows
			.into_iter()
			.map(|(key, set)| {
				(
					key,
					WindowSet {
						windows: set.windows,
					},
				)
			})
			.collect::<HashMap<_, _>>();

		return RollupState {
			windows: windowsets,
			event_time: EventTime::from(timestamps),
			submitted: Arc::new(Mutex::new(SentList::new())),
		};
	}
}
