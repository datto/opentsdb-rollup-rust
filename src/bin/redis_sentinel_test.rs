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
	borrow::Cow,
	thread,
};

use opentsdb_rollup_rust::redis_sentinel::RedisSentinel;

fn main() {
	fern::Dispatch::new()
		.format(|out, message, record| {
			let current_thread = thread::current();
			let thread_name = match current_thread.name() {
				Some(v) => Cow::from(v),
				None => Cow::from(format!("{:?}", current_thread.id())),
			};

			out.finish(format_args!(
				"{}[{}][{}][{}]: {}",
				chrono::Utc::now().to_rfc3339(),
				thread_name,
				record.target(),
				record.level(),
				message,
			))
		})
		.level(log::LevelFilter::Debug)
		.chain(std::io::stderr())
		.apply()
		.expect("Could not apply logging config");

	let mut sentinel = RedisSentinel::new(
		"redis-server".to_string(),
		vec![("127.0.0.1".to_string(), 26379)],
	);

	match sentinel.master_addr() {
		Ok((ip, port)) => {
			println!("Current Master: {}:{}", ip, port);
		}
		Err(err) => {
			eprintln!("{}", err);
			::std::process::exit(1);
		}
	};
}
