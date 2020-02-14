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


use redis;
use std::{
	borrow::Cow,
	collections::HashMap,
	fmt,
};

pub struct RedisSentinel {
	master_name: String,
	sentinels: Vec<redis::Client>,
	current_connection: Option<redis::Connection>,
}

/// Redis sentinel client.
///
/// Fetches the IP and port for a single master.
/// Automatically updates list of sentinels for the master.
impl RedisSentinel {
	/// Creates a new sentinel client.
	///
	/// Does not make any network requests.
	pub fn new(master_name: String, starting_sentinels_list: Vec<(String, u16)>) -> Self {
		Self {
			master_name,
			sentinels: starting_sentinels_list
				.into_iter()
				.map(|(ip, port)| Self::ip_port_to_client(ip, port))
				.collect(),
			current_connection: None,
		}
	}

	/// Fetches the master address, failing over if needed.
	pub fn master_addr(&mut self) -> Result<(String, u16), SentinelError> {
		if let Some(ref mut conn) = self.current_connection {
			match Self::get_master_addr_by_name(conn, &self.master_name) {
				Ok(Some(tup)) => {
					return Ok(tup);
				}
				Ok(None) => {
					warn!("Existing sentinel connection no longer knows about master, will find a new sentinel.");
				}
				Err(err) => {
					warn!("Got error from existing sentinel connection, will find a new sentinel. Error was: {}", err);
				}
			}
		} else {
			debug!("Getting initial sentinel connection");
		}

		let tup = self.new_sentinel_conection()?;
		if let Err(err) = self.update_sentinels() {
			warn!("Could not update list of sentinels: {}", err);
		}
		Ok(tup)
	}

	fn new_sentinel_conection(&mut self) -> Result<(String, u16), SentinelError> {
		for client in self.sentinels.iter() {
			let mut conn = match client.get_connection() {
				Ok(c) => c,
				Err(e) => {
					warn!(
						"Could not connect to sentinel {:?}, trying another. Error: {}",
						client, e
					);
					continue;
				}
			};

			let (ip, port): (String, u16) = match Self::get_master_addr_by_name(
				&mut conn,
				&self.master_name,
			) {
				Ok(Some(v)) => v,
				Ok(None) => {
					warn!(
						"Sentinel {:?} does not know about master {:?}, trying another.",
						client, self.master_name
					);
					continue;
				}
				Err(e) => {
					warn!("Sentinel {:?} returned error when asked about the master, trying another. Error: {}", client, e);
					continue;
				}
			};

			self.current_connection = Some(conn);
			return Ok((ip, port));
		}
		return Err(SentinelError);
	}

	fn update_sentinels(&mut self) -> redis::RedisResult<()> {
		debug!("Updating sentinels list");
		let conn = self
			.current_connection
			.as_mut()
			.expect("update_sentinels called without current connection");
		let sentinels_raw: Vec<HashMap<String, String>> = redis::cmd("sentinel")
			.arg("sentinels")
			.arg(&self.master_name)
			.query(conn)?;

		let mut new_sentinels = Vec::with_capacity(sentinels_raw.len());
		for mut sentinel_raw in sentinels_raw.into_iter() {
			let name = sentinel_raw
				.remove("name")
				.map(|s| Cow::from(s))
				.unwrap_or(Cow::from("(unnamed)"));
			let ip = match sentinel_raw.remove("ip") {
				Some(v) => v,
				None => {
					warn!("Sentinel {} has no ip field, skipping", name);
					continue;
				}
			};

			let port = match sentinel_raw.remove("port").map(|p| p.parse::<u16>()) {
				Some(Ok(v)) => v,
				Some(Err(_)) => {
					warn!("Sentinel {} has invalid port, skipping", name);
					continue;
				}
				None => {
					warn!("Sentinel {} has no port field, skipping", name);
					continue;
				}
			};
			debug!("Found sentinel: {}:{}", ip, port);
			new_sentinels.push(Self::ip_port_to_client(ip, port));
		}

		if new_sentinels.is_empty() {
			warn!("When updating sentinels list, no sentinels found for master. Using old list.");
			return Ok(());
		}
		debug!("Done updating sentinels list");
		new_sentinels.shrink_to_fit();
		self.sentinels = new_sentinels;
		Ok(())
	}

	fn get_master_addr_by_name(
		conn: &mut redis::Connection,
		master: &str,
	) -> redis::RedisResult<Option<(String, u16)>> {
		redis::cmd("sentinel")
			.arg("get-master-addr-by-name")
			.arg(master)
			.query(conn)
	}

	fn ip_port_to_client(ip: String, port: u16) -> redis::Client {
		let info = redis::ConnectionInfo {
			addr: Box::new(redis::ConnectionAddr::Tcp(String::from(ip), port)),
			db: 0,
			passwd: None,
		};
		redis::Client::open(info).unwrap()
	}
}

/// Error returned when no sentinel servers could be contacted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SentinelError;
impl fmt::Display for SentinelError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("Could not contact any sentinel that knows about the requested master. Check warning log for more information.")
	}
}
impl std::error::Error for SentinelError {}
