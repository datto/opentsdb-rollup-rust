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


//! Module for writing metrics in Influx format

use lazy_static::lazy_static;
use regex::Regex;
use std::{
	fmt::{
		Display,
		Write,
	},
	io,
	net,
};

lazy_static! {
	static ref RE_ESCAPE_TAG: Regex = Regex::new("[,= \\\\]").unwrap();
	static ref RE_ESCAPE_MEASUREMENT: Regex = Regex::new("[, \\\\]").unwrap();
}

/// Type for reporting metrics in Influx format to a UDP socket
pub struct InfluxReporter {
	socket: net::UdpSocket,
}
impl InfluxReporter {
	/// Creates a new reporter, sending to the given address.
	pub fn new<A: net::ToSocketAddrs>(addr: A) -> io::Result<InfluxReporter> {
		let socket = net::UdpSocket::bind("127.0.0.1:0")?;
		socket.connect(addr)?;
		Ok(Self { socket })
	}

	/// Sends a batch of metrics.
	///
	/// Tags and metrics are given via iterators.
	pub fn send<TI, TK, TV, VI, VK, VV>(&self, prefix: &str, tags: TI, values: VI)
	where
		TI: Iterator<Item = (TK, TV)>,
		TK: Display,
		TV: Display,
		VI: Iterator<Item = (VK, VV)>,
		VK: Display,
		VV: Display,
	{
		let mut line = RE_ESCAPE_TAG.replace_all(prefix, r"\\\0").into_owned();
		let mut buffer = String::new();
		for (k, v) in tags {
			line.push(',');
			line.push_str(&RE_ESCAPE_TAG.replace_all(display_into(&mut buffer, &k), r"\\\0"));
			line.push('=');
			line.push_str(&RE_ESCAPE_TAG.replace_all(display_into(&mut buffer, &v), r"\\\0"));
		}

		line.push(' ');

		for (i, (k, v)) in values.enumerate() {
			if i != 0 {
				line.push(',');
			}
			line.push_str(&RE_ESCAPE_TAG.replace_all(display_into(&mut buffer, &k), r"\\\0"));
			line.push('=');
			line.push_str(
				&RE_ESCAPE_MEASUREMENT.replace_all(display_into(&mut buffer, &v), r"\\\0"),
			);
		}

		match self.socket.send(line.as_bytes()) {
			Ok(v) if v != line.len() => {
				warn!(
					"Influx socket only wrote {} / {} bytes of metric",
					v,
					line.len()
				);
			}
			Err(err) => {
				warn!("Influx socket error: {}", err);
			}
			Ok(_) => {}
		};
	}
}

fn display_into<'a, T: Display>(s: &'a mut String, v: &T) -> &'a str {
	s.clear();
	write!(*s, "{}", v).unwrap();
	s.as_str()
}
