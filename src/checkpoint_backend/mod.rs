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


//! Checkpoint backends

pub mod fs;
#[cfg(feature = "hdfs")]
pub mod hdfs;
#[cfg(feature = "redis")]
pub mod redis;

pub use crate::checkpoint_backend::fs::FsCheckpointBackend;
#[cfg(feature = "hdfs")]
pub use crate::checkpoint_backend::hdfs::HdfsCheckpointBackend;
#[cfg(feature = "redis")]
pub use crate::checkpoint_backend::redis::RedisCheckpointBackend;

use std::{
	collections::HashMap,
	sync::Arc,
};

use crate::types::*;

pub use crate::serialized::{
	read_state,
	write_state,
};

/// Backend for storing checkpoints
pub trait CheckpointBackend {
	/// Saves a checkpoint.
	///
	/// This function should be atomic, meaning it either successfully replaces
	/// the old checkpoint completely or fails and leaves the old checkpoint untouched.
	///
	/// Implementations can use the `write_state` function to compress and write the state to
	/// a `Write` object that the implementation opens.
	fn save(&mut self, state: &[Arc<RollupState>]) -> Result<(), String>;
	/// Loads a checkpoint.
	///
	/// Returns None if no checkpoint has been stored yet.
	///
	/// Implementations can use the `read_state` function to decompress and read the state
	/// from a `Read` object that the implementation opens.
	fn load(&mut self) -> Result<Option<RollupState>, String>;
}

/// Creates a backend from a string name and options
pub fn create_backend(
	typ: &str,
	opts: HashMap<String, String>,
) -> Result<Box<dyn CheckpointBackend>, String> {
	if typ == "fs" {
		return FsCheckpointBackend::from_opts(opts).map(|b| Box::new(b) as Box<_>);
	}

	if typ == "hdfs" {
		#[cfg(feature = "hdfs")]
		return HdfsCheckpointBackend::from_opts(opts).map(|b| Box::new(b) as Box<_>);
		#[cfg(not(feature = "hdfs"))]
		return Err("This opentsdb-rollup was not build with hdfs support.".into());
	}

	if typ == "redis" {
		#[cfg(feature = "redis")]
		return RedisCheckpointBackend::from_opts(opts).map(|b| Box::new(b) as Box<_>);
		#[cfg(not(feature = "redis"))]
		return Err("This opentsdb-rollup was not build with redis support.".into());
	}

	return Err(format!("Unrecognized backend: {}", typ));
}
