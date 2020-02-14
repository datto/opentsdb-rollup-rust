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


//! Local filesystem checkpoint backend

use std::{
	collections::HashMap,
	fs,
	io,
	path::PathBuf,
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

/// Backend that writes state to the local filesystem
pub struct FsCheckpointBackend {
	dest_file: PathBuf,
	in_progress_file: PathBuf,
}
impl FsCheckpointBackend {
	pub fn new(dest_file: PathBuf) -> Self {
		let mut in_progress_file = dest_file.clone();
		in_progress_file.set_extension(".in-progress");
		Self {
			dest_file,
			in_progress_file,
		}
	}

	pub fn from_opts(mut opts: HashMap<String, String>) -> Result<Self, String> {
		let dest: PathBuf = opts
			.remove("path")
			.ok_or_else(|| String::from("Missing argument: path"))?
			.into();
		if !opts.is_empty() {
			let unrecognized_keys = opts
				.keys()
				.map(|s| s.as_str())
				.collect::<Vec<_>>()
				.join(",");
			return Err(format!("Unrecognized argument(s): {}", unrecognized_keys));
		}
		return Ok(Self::new(dest));
	}
}
impl CheckpointBackend for FsCheckpointBackend {
	fn save(&mut self, state: &[Arc<RollupState>]) -> Result<(), String> {
		let out_file = fs::File::create(&self.in_progress_file)
			.map_err(|e| format!("Could not open {:?}: {}", self.in_progress_file, e))?;
		write_state(out_file, state)
			.map_err(|e| format!("Could not write to {:?}: {}", self.in_progress_file, e))?;
		fs::rename(&self.in_progress_file, &self.dest_file).map_err(|e| {
			format!(
				"Could not rename {:?} to {:?}: {}",
				self.in_progress_file, self.dest_file, e
			)
		})
	}

	fn load(&mut self) -> Result<Option<RollupState>, String> {
		let in_file = match fs::File::open(&self.dest_file) {
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
