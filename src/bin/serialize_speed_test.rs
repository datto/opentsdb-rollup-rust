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


extern crate opentsdb_rollup_rust;

use std::{
	collections::HashMap,
	path::PathBuf,
	time::Instant,
};
use structopt::StructOpt;

/// Writes checkpoint contents to stdout
#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
struct Args {
	/// Checkpoint arguments to use. See the main rollup program for usage.
	#[structopt(parse(try_from_str=opentsdb_rollup_rust::util::parse_checkpoint_opts))]
	source_backend: (String, HashMap<String, String>),

	dest_base: PathBuf,
}

fn main() {
	if let Err(err) = run() {
		eprintln!("{}", err);
		::std::process::exit(1);
	}
}

// Could be a function taking a closure, but the concrete type for the writer would be really long so eh.
macro_rules! test_serialize {
	($name:ident($checkpoint:expr, $pathbase:expr, |$writer:ident, $ckname:ident| $block:block )) => {{
		let checkpoint = $checkpoint;
		let path = ($pathbase).with_extension(stringify!($name));

		use std::{
			fs,
			io,
			time,
			};

		let res = (|| -> Result<(), String> {
			let out_file = fs::File::create(&path)
				.map_err(|e| format!("could not create output {:?}: {}", path, e))?;

			let out_writer = io::BufWriter::new(out_file);

			let begin = time::Instant::now();
				{
				let $writer = out_writer;
				let $ckname = checkpoint;
				$block.map_err(|e: String| format!("could not finish writing: {}", e))?;
				}
			let end = time::Instant::now();
			println!(
				"Test {} took {}ms",
				stringify!($name),
				(end - begin).as_millis()
			);
			Ok(())
		})();
		if let Err(err) = res {
			eprintln!("Test {} failed: {}", stringify!($name), err);
			}
		}};
}

fn run() -> Result<(), String> {
	let args = Args::from_args();

	let (backend_type, backend_params) = args.source_backend;
	let mut backend =
		opentsdb_rollup_rust::checkpoint_backend::create_backend(&backend_type, backend_params)
			.map_err(|e| format!("Could not initialize backend: {}", e))?;

	let load_begin = Instant::now();
	let checkpoint = backend
		.load()
		.map_err(|e| format!("Could not load checkpoint: {}", e))?;
	let load_end = Instant::now();

	let checkpoint = match checkpoint {
		Some(c) => opentsdb_rollup_rust::serialized::SerializableRollupState::from(c),
		None => {
			return Err("No checkpoint found".into());
		}
	};

	println!(
		"Loaded checkpoint in {}ms",
		(load_end - load_begin).as_millis()
	);

	test_serialize!(gz(&checkpoint, &args.dest_base, |w, ck| {
		let mut gzw = flate2::write::GzEncoder::new(w, flate2::Compression::new(6));

		rmp_serde::encode::write_named(&mut gzw, ck)
			.map_err(|e| format!("could not write: {}", e))?;

		gzw.finish()
			.map_err(|e| format!("could not write: {}", e))?;
		Ok(())
	}));

	/*test_serialize!(lz4(&checkpoint, &args.dest_base, |w, ck| {
		let mut zw = lz4::EncoderBuilder::new()
			.level(4)
			.build(w)
			.map_err(|e| format!("could not create compressor: {}", e))?;

		rmp_serde::encode::write_named(&mut zw, ck)
			.map_err(|e| format!("could not write: {}", e))?;

		zw.finish()
			.1
			.map_err(|e| format!("could not write: {}", e))?;
		Ok(())
	}));*/

	test_serialize!(zstd(&checkpoint, &args.dest_base, |w, ck| {
		let mut zw =
			zstd::Encoder::new(w, 0).map_err(|e| format!("could not create compressor: {}", e))?;

		rmp_serde::encode::write_named(&mut zw, ck)
			.map_err(|e| format!("could not write: {}", e))?;

		zw.finish().map_err(|e| format!("could not write: {}", e))?;
		Ok(())
	}));

	test_serialize!(zstd_1(&checkpoint, &args.dest_base, |w, ck| {
		let mut zw =
			zstd::Encoder::new(w, 1).map_err(|e| format!("could not create compressor: {}", e))?;

		rmp_serde::encode::write_named(&mut zw, ck)
			.map_err(|e| format!("could not write: {}", e))?;

		zw.finish().map_err(|e| format!("could not write: {}", e))?;
		Ok(())
	}));

	/*test_serialize!(snappy(&checkpoint, &args.dest_base, |w, ck| {
		let mut zw = snap::Writer::new(w);

		rmp_serde::encode::write_named(&mut zw, ck)
			.map_err(|e| format!("could not write: {}", e))?;
		Ok(())
	}));*/

	Ok(())
}
