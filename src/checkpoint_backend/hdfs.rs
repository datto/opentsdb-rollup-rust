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


//! HDFS checkpoint backend

use hdfs::*;
use std::{
	collections::HashMap,
	env,
	io::{
		self,
		Write,
	},
	sync::Arc,
};

use crate::{
	checkpoint_backend::{
		read_state,
		write_state,
		CheckpointBackend,
	},
	types::*,
};

/// Checkpoint backend that writes checkpoints to a Hadoop HDFS cluster.
pub struct HdfsCheckpointBackend {
	name_node: Option<String>,
	username: Option<String>,
	dest_file: String,
	in_progress_file: String,
}
impl HdfsCheckpointBackend {
	pub fn from_opts(mut opts: HashMap<String, String>) -> Result<Self, String> {
		let name_node = opts.remove("name_node");
		let username = opts.remove("username");

		let dest_file = opts
			.remove("path")
			.map(|p| String::from(truncate_str(&p, 255 - ".in-progress".len()))) // Trim to HDFS limit
			.ok_or_else(|| String::from("Missing argument: path"))?;
		let in_progress_file = format!("{}.in-progress", dest_file);

		if !opts.is_empty() {
			let unrecognized_keys = opts
				.keys()
				.map(|s| s.as_str())
				.collect::<Vec<_>>()
				.join(",");
			return Err(format!("Unrecognized argument(s): {}", unrecognized_keys));
		}

		// Tell libhadoop's JVM to not overwrite our signals,
		// and limit it's memory usage.
		env::set_var("LIBHDFS_OPTS", "-Xrs -Xmx800m");

		return Ok(Self {
			name_node,
			username,
			dest_file,
			in_progress_file,
		});
	}

	fn connect_fs(&self) -> io::Result<HdfsConnection> {
		let mut builder = HdfsBuilder::new();
		builder.name_node(self.name_node.as_ref().map(|s| s.as_str()));
		if let Some(username) = self.username.as_ref() {
			builder.user_name(username.as_str());
		}
		let fs = builder.connect()?;
		Ok(fs)
	}
}
impl CheckpointBackend for HdfsCheckpointBackend {
	fn save(&mut self, state: &[Arc<RollupState>]) -> Result<(), String> {
		let fs = self
			.connect_fs()
			.map_err(|e| format!("Could not connect to hdfs: {}", e))?;

		let mut out_file_builder = fs
			.open_create_builder(&self.in_progress_file)
			.map_err(|e| {
				format!(
					"Could not open (create stream builder) {:?}: {}",
					self.in_progress_file, e
				)
			})?;
		out_file_builder.replication(1).map_err(|e| {
			format!(
				"Could not set replication factor for {:?}: {}",
				self.in_progress_file, e
			)
		})?;
		let mut out_file = out_file_builder
			.build()
			.map_err(|e| format!("Could not open {:?}: {}", self.in_progress_file, e))?;

		write_state(out_file.by_ref(), state)
			.map_err(|e| format!("Could not write to {:?}: {}", self.in_progress_file, e))?;
		out_file
			.sync()
			.map_err(|e| format!("Could not sync {:?}: {}", self.in_progress_file, e))?;

		// TODO: `rename` does not overwrite the destination, so we have to delete it ourselves.
		// THIS ISN'T ATOMIC
		match fs.delete(&self.dest_file, false) {
			Ok(_) => {}
			Err(ref e) if e.kind() == io::ErrorKind::NotFound => {}
			Err(ref e) if e.raw_os_error() == Some(5) => {} // Actually happens if you delete a nonexistent file
			Err(err) => {
				return Err(format!(
					"Could not delete existing checkpoint at {:?}: {}",
					self.dest_file, err
				));
			}
		};

		fs.rename(&self.in_progress_file, &self.dest_file)
			.map_err(|e| {
				format!(
					"Could not rename {:?} to {:?}: {}",
					self.in_progress_file, self.dest_file, e
				)
			})
	}

	fn load(&mut self) -> Result<Option<RollupState>, String> {
		let fs = self
			.connect_fs()
			.map_err(|e| format!("Could not connect to hdfs: {}", e))?;

		let in_file = match fs.open_read(&self.dest_file) {
			Ok(f) => f,
			Err(ref e) if e.kind() == io::ErrorKind::NotFound => {
				return Ok(None);
			}
			Err(e) => {
				return Err(format!("Could not open {:?}: {}", self.dest_file, e));
			}
		};
		read_state(in_file)
			.map(|state| Some(state))
			.map_err(|e| format!("Could not read {:?}: {}", self.dest_file, e))
	}
}

/// Truncates a string, taking into account character boundaries.
fn truncate_str(s: &str, mut len: usize) -> &str {
	if len >= s.len() {
		return s;
	}
	while !s.is_char_boundary(len) {
		len -= 1;
	}
	return &s[0..len];
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn truncate_str_test() {
		assert_eq!(truncate_str("asdf", 10), "asdf");
		assert_eq!(truncate_str("asdf", 2), "as");
		assert_eq!(truncate_str("60°C", 2), "60");
		assert_eq!(truncate_str("60°C", 3), "60");
		assert_eq!(truncate_str("60°C", 4), "60°");
	}
}
