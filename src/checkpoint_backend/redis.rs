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


//! Redis checkpoint backend

use redis::{
	self,
	Commands as _,
	PipelineCommands as _,
};
use serde::ser::Serialize;
use serde_json;
use std::{
	collections::HashMap,
	io::Write,
	sync::{
		Arc,
		Mutex,
	},
};

use crate::{
	checkpoint_backend::CheckpointBackend,
	redis_sentinel::{
		RedisSentinel,
		SentinelError,
	},
	types::*,
};

const PIPELINE_SIZE: usize = 100;
const IN_PROGRESS_EXPIRE_SECS: usize = 7200; // 2 hours;

/// Backend that writes state to a redis database.
pub struct RedisCheckpointBackend {
	sentinel: RedisSentinel,
	password: Option<String>,
	database: i64,
	pipeline: redis::Pipeline,

	windows_name: String,
	time_name: String,
	to_send_name: String,

	in_progress_windows_name: String,
	in_progress_time_name: String,
	in_progress_to_send_name: String,
}
impl RedisCheckpointBackend {
	pub fn new(
		sentinel_client: RedisSentinel,
		password: Option<String>,
		database: i64,
		prefix: String,
	) -> Self {
		Self {
			sentinel: sentinel_client,
			password,
			database,
			pipeline: redis::pipe(),

			windows_name: format!("{{{}}}-windows", prefix),
			time_name: format!("{{{}}}-time", prefix),
			to_send_name: format!("{{{}}}-to-send", prefix),

			in_progress_windows_name: format!("{{{}}}-windows.in_progress", prefix),
			in_progress_time_name: format!("{{{}}}-time.in_progress", prefix),
			in_progress_to_send_name: format!("{{{}}}-to-send.in_progress", prefix),
		}
	}

	pub fn from_opts(mut opts: HashMap<String, String>) -> Result<Self, String> {
		let master_name = opts
			.remove("master_name")
			.ok_or_else(|| String::from("Missing argument: master_name"))?;
		let sentinel_addrs = opts
			.remove("sentinel_addrs")
			.ok_or_else(|| String::from("Missing argument: sentinel_addrs"))
			.and_then(|s| parse_ips_ports(&s))
			.map_err(|err| format!("Could not parse `sentinel_addrs`: {}", err))?;
		let prefix = opts
			.remove("prefix")
			.ok_or_else(|| String::from("Missing argument: prefix"))?;
		let database = opts
			.remove("database")
			.unwrap_or_else(|| "0".into())
			.parse()
			.map_err(|e| format!("Could not parse `database`: {}", e))?;
		let password = opts.remove("password");

		if !opts.is_empty() {
			let unrecognized_keys = opts
				.keys()
				.map(|s| s.as_str())
				.collect::<Vec<_>>()
				.join(",");
			return Err(format!("Unrecognized argument(s): {}", unrecognized_keys));
		}

		let mut sentinel_client = RedisSentinel::new(master_name, sentinel_addrs);

		// Connect once to check config
		if let Err(_) = sentinel_client.master_addr() {
			return Err("Could not connect to redis, check sentinel config".into());
		}

		return Ok(Self::new(sentinel_client, password, database, prefix));
	}

	fn master(&mut self) -> Result<redis::Client, SentinelError> {
		let (ip, port) = self.sentinel.master_addr()?;

		let info = redis::ConnectionInfo {
			addr: Box::new(redis::ConnectionAddr::Tcp(ip, port)),
			db: self.database,
			passwd: self.password.clone(),
		};
		Ok(redis::Client::open(info).unwrap())
	}
}
impl CheckpointBackend for RedisCheckpointBackend {
	fn save(&mut self, states: &[Arc<RollupState>]) -> Result<(), String> {
		// Atomic checkpoints with redis:
		// Store the event time, windows, and send list into in-progress temporary keys, pipelined
		// but outside of transactions, since it can take quite a lot of data. Set those keys to expire
		// after awhile in case checkpointing fails to clean up a failed transaction. When all of the
		// data is in redis, in a transaction, remove the expiry and rename the in-progress keys to the
		// real name, overwriting the old checkpoint.

		let client = self
			.master()
			.map_err(|e| format!("could not get redis master: {}", e))?;
		let mut connection = client
			.get_connection()
			.map_err(|e| format!("could not connect to redis: {}", e))?;

		let mut ser_buf = vec![];

		let event_time = EventTime::merge_iter(states.iter().map(|state| &state.event_time));

		debug!("Cleaning up previous in progress keys and setting offsets");
		self.pipeline
			.cmd("del")
			.arg(&self.in_progress_windows_name)
			.arg(&self.in_progress_time_name)
			.arg(&self.in_progress_to_send_name)
			.ignore()
			.set(
				&self.in_progress_time_name,
				serialize_to_buf(&mut ser_buf, &event_time),
			)
			.ignore()
			.expire(&self.in_progress_time_name, IN_PROGRESS_EXPIRE_SECS)
			.ignore();
		pipeline_send(&mut self.pipeline, &mut connection)?;

		debug!("Sending windows");
		let mut has_windows = false;
		for (i, (key, windows)) in states
			.iter()
			.flat_map(|state| state.windows.iter())
			.enumerate()
		{
			if i != 0 && i % PIPELINE_SIZE == 0 {
				pipeline_send(&mut self.pipeline, &mut connection)?;
			}

			// not using hset here since it'd need two buffers
			self.pipeline
				.cmd("hset")
				.arg(&self.in_progress_windows_name)
				.arg(serialize_to_buf(&mut ser_buf, key))
				.arg(serialize_to_buf(&mut ser_buf, windows))
				.ignore();
			if i == 0 {
				has_windows = true;
				self.pipeline
					.expire(&self.in_progress_windows_name, IN_PROGRESS_EXPIRE_SECS)
					.ignore();
			}
		}
		pipeline_send(&mut self.pipeline, &mut connection)?;

		debug!("Sending submitted points");
		let locks = states
			.iter()
			.map(|state| state.submitted.lock().expect("submitted lock poisoned"))
			.collect::<Vec<_>>();
		let mut has_submitted = false;
		let iter = locks
			.iter()
			.flat_map(|sent_list| {
				sent_list
					.to_send_iter()
					.chain(sent_list.sent_iter().map(|(_, v)| v))
			})
			.enumerate();
		for (i, tup) in iter {
			if i != 0 && i % PIPELINE_SIZE == 0 {
				pipeline_send(&mut self.pipeline, &mut connection)?;
			}

			self.pipeline
				.sadd(
					&self.in_progress_to_send_name,
					serialize_to_buf(&mut ser_buf, tup),
				)
				.ignore();
			if i == 0 {
				has_submitted = true;
				self.pipeline
					.expire(&self.in_progress_to_send_name, IN_PROGRESS_EXPIRE_SECS)
					.ignore();
			}
		}
		pipeline_send(&mut self.pipeline, &mut connection)?;

		debug!("Committing");
		self.pipeline
			.atomic()
			.persist(&self.in_progress_time_name)
			.ignore()
			.rename(&self.in_progress_time_name, &self.time_name)
			.ignore();
		if has_windows {
			self.pipeline
				.persist(&self.in_progress_windows_name)
				.ignore()
				.rename(&self.in_progress_windows_name, &self.windows_name)
				.ignore();
		}
		if has_submitted {
			self.pipeline
				.persist(&self.in_progress_to_send_name)
				.ignore()
				.rename(&self.in_progress_to_send_name, &self.to_send_name)
				.ignore();
		}
		pipeline_send(&mut self.pipeline, &mut connection)?;

		Ok(())
	}

	fn load(&mut self) -> Result<Option<RollupState>, String> {
		let client = self
			.master()
			.map_err(|e| format!("could not get redis master: {}", e))?;
		let mut connection = client
			.get_connection()
			.map_err(|e| format!("could not connect to redis: {}", e))?;

		let event_time_src: Option<Vec<u8>> = connection
			.get(&self.time_name)
			.map_err(|e| format!("redis error: {}", e))?;
		let event_time = match event_time_src {
			Some(v) => serde_json::from_slice::<EventTime>(&v)
				.map_err(|e| format!("Could not deserialize event time: {}", e))?,
			None => {
				return Ok(None);
			}
		};

		let mut windows = HashMap::new();
		let mut windows_iter: redis::Iter<Vec<u8>> = connection
			.hscan(&self.windows_name)
			.map_err(|e| format!("redis error: {}", e))?;
		while let Some(k_vec) = windows_iter.next() {
			let v_vec = windows_iter
				.next()
				.ok_or_else(|| String::from("redis returned no value for key"))?;

			let key = serde_json::from_slice::<OpenTsdbKey>(&k_vec)
				.map_err(|e| format!("Could not deserialize OpenTsdbKey: {}", e))?;
			let value = serde_json::from_slice::<WindowSet>(&v_vec)
				.map_err(|e| format!("Could not deserialize WindowSet: {}", e))?;
			windows.insert(key, value);
		}

		let mut submitted = SentList::new();
		let to_send_iter: redis::Iter<Vec<u8>> = connection
			.sscan(&self.to_send_name)
			.map_err(|e| format!("redis error: {}", e))?;
		for elem in to_send_iter {
			let (key, window) = serde_json::from_slice::<(OpenTsdbKey, Window)>(&elem)
				.map_err(|e| format!("Could not deserialize (OpenTsdbKey, Window): {}", e))?;
			submitted.to_send_enqueue(key, window);
		}

		Ok(Some(RollupState {
			windows,
			event_time,
			submitted: Arc::new(Mutex::new(submitted)),
		}))
	}
}

fn pipeline_send(
	pipeline: &mut redis::Pipeline,
	conn: &mut redis::Connection,
) -> Result<(), String> {
	pipeline
		.query::<()>(conn)
		.map_err(|err| format!("redis error: {}", err))?;
	pipeline.clear();
	Ok(())
}

fn parse_ips_ports(s: &str) -> Result<Vec<(String, u16)>, String> {
	let mut elems = vec![];
	for (i, elem) in s.split('/').enumerate() {
		let port_index = match elem.rfind(':') {
			Some(v) => v,
			None => {
				return Err(format!("element {} has no port", i + 1));
			}
		};

		let ip = &elem[0..port_index];
		let port = match elem[port_index + 1..].parse::<u16>() {
			Ok(v) => v,
			Err(err) => {
				return Err(format!("Could not parse port for element {}: {}", i, err));
			}
		};

		elems.push((ip.to_string(), port));
	}
	Ok(elems)
}

fn serialize_to_buf<'a, T: Serialize + ?Sized>(vec: &'a mut Vec<u8>, obj: &T) -> &'a [u8] {
	vec.clear();
	serde_json::to_writer(vec.by_ref(), obj).expect("Could not serialize");
	vec.as_slice()
}

/*
/// Better version of redis::Iter that returns errors
struct CursorIter<'a, T: redis::FromRedisValue> {
	batch: Vec<T>,
	cursor: Option<u64>,
	con: &'a mut (dyn redis::ConnectionLike + 'a),
	cmd: redis::Cmd,
}
impl<'a, T: redis::FromRedisValue> CursorIter<'a, T> {
	pub fn new(con: &'a mut (dyn redis::ConnectionLike + 'a), cmd: redis::Cmd) -> Self {
		Self {
			batch: vec![],
			cursor: None,
			con,
			cmd,
		}
	}
}
impl<'a, T: redis::FromRedisValue> Iterator for CursorIter<'a, T> {
	type Item = Result<T, redis::RedisError>;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			if let Some(v) = self.batch.pop() {
				return Some(v);
			}
			if self.cursor == Some(0) {
				return None;
			}

			let pcmd_res = match self.cursor.clone() {
				Some(v) => self.cmd.get_packed_command_with_cursor(v),
				None => Ok(self.cmd.get_packed_command()),
			};

			let pcmd = match pcmd_res {
				Ok(v) => v,
				Err(e) => { return Some(Err(e)); }
			};

			let rv = match self.con.req_packed_command(&pcmd) {
				Ok(v) => v,
				Err(e) => { return Some(Err(e)); }
			};

			let (cur, mut batch): (u64, Vec<T>) = match from_redis_value(&rv) {
				Ok(v) => v,
				Err(e) => { return Some(Err(e)); }
			};
			batch.reverse();

			self.cursor = Some(cur);
			self.batch = batch;
		}

	}
}
*/
