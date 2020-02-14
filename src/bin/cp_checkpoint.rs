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


use std::{
	collections::HashMap,
	sync::Arc,
};
use structopt::StructOpt;

/// Copies a checkpoint from one backend to another.
///
/// Potentially upgrades the checkpoint in the process.
#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
struct Args {
	/// Checkpoint to copy from. See the main rollup program for usage.
	#[structopt(parse(try_from_str=opentsdb_rollup_rust::util::parse_checkpoint_opts))]
	src_backnd: (String, HashMap<String, String>),

	/// Checkpoint to copy to. See the main rollup program for usage.
	#[structopt(parse(try_from_str=opentsdb_rollup_rust::util::parse_checkpoint_opts))]
	dest_backnd: (String, HashMap<String, String>),
}

fn main() {
	if let Err(err) = run() {
		eprintln!("{}", err);
		::std::process::exit(1);
	}
}

fn run() -> Result<(), String> {
	let args = Args::from_args();

	fern::Dispatch::new()
		.format(|out, message, record| {
			out.finish(format_args!(
				"{}[{}][{}]: {}",
				chrono::Utc::now().to_rfc3339(),
				record.target(),
				record.level(),
				message,
			))
		})
		.level(log::LevelFilter::Debug)
		.chain(std::io::stderr())
		.apply()
		.expect("Could not apply logging config");

	let (src_backend_type, src_backend_params) = args.src_backnd;
	let (dest_backend_type, dest_backend_params) = args.dest_backnd;

	let mut src_backend = opentsdb_rollup_rust::checkpoint_backend::create_backend(
		&src_backend_type,
		src_backend_params,
	)
	.map_err(|e| format!("Could not initialize source backend: {}", e))?;
	let mut dest_backend = opentsdb_rollup_rust::checkpoint_backend::create_backend(
		&dest_backend_type,
		dest_backend_params,
	)
	.map_err(|e| format!("Could not initialize destination backend: {}", e))?;

	println!("Loading checkpoint from {} backend...", src_backend_type);
	let checkpoint = match src_backend.load() {
		Ok(Some(c)) => c,
		Ok(None) => {
			return Err("No checkpoint found".into());
		}
		Err(err) => {
			return Err(format!("Could not load checkpoint: {}", err));
		}
	};

	let checkpoint = vec![Arc::new(checkpoint)];

	println!("Saving checkpoint to {} backend...", dest_backend_type);
	dest_backend
		.save(&checkpoint)
		.map_err(|e| format!("Could not save checkpoint: {}", e))
}
