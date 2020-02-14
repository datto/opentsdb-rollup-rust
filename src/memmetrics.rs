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


use crossbeam_channel::{
	self as cbc,
	select,
};
use psutil::process::Memory;
use std::{
	process::id as pid,
	sync::Arc,
	time::{
		Duration,
		Instant,
	},
};

use crate::influx::InfluxReporter;

pub struct MemMetrics {
	instance_id: String,
	quit_channel: cbc::Receiver<()>,
	ticker_channel: cbc::Receiver<Instant>,
	metrics_sender: Arc<InfluxReporter>,
}
impl MemMetrics {
	pub fn new(
		instance_id: String,
		quit_channel: cbc::Receiver<()>,
		interval: Duration,
		metrics_sender: Arc<InfluxReporter>,
	) -> Self {
		Self {
			instance_id,
			quit_channel,
			ticker_channel: cbc::tick(interval),
			metrics_sender,
		}
	}

	pub fn run(&mut self) {
		loop {
			select! {
				recv(self.quit_channel) -> _ => { break; },
				recv(self.ticker_channel) -> _instant => {
					let meminfo = match Memory::new(pid() as i32) {
						Ok(m) => m,
						Err(e) => {
							warn!("Could not get memory usage, skipping: {}", e);
							continue;
						},
					};

					let tags = &[
						("instance_id", self.instance_id.as_str()),
					];
					let values = &[
						("size", meminfo.size as f64),
						("resident", meminfo.resident as f64),
						("share", meminfo.share as f64),
						("text", meminfo.text as f64),
						("data", meminfo.data as f64),
					];

					self.metrics_sender.send(
						"opentsdb_rollup_rust.memory",
						tags.iter().copied(),
						values.iter().copied(),
					);
				},
			}
		}
	}
}
