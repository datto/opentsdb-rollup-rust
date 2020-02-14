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


use regex::Regex;
use serde::{
	de::{
		Deserializer,
		Error as _,
	},
	Deserialize,
};
use std::{
	fs,
	io,
	path::Path,
};

use crate::types::OpenTsdbKey;

fn de_regex<'a, D: Deserializer<'a>>(d: D) -> Result<Regex, D::Error> {
	let s = String::deserialize(d)?;
	Regex::new(&s).map_err(D::Error::custom)
}
fn match_all_regex() -> Regex {
	Regex::new(".?").unwrap()
}

#[derive(Debug, Clone, Deserialize)]
pub struct TagBlacklistItem {
	#[serde(deserialize_with = "de_regex", default = "match_all_regex")]
	pub key: Regex,
	#[serde(deserialize_with = "de_regex", default = "match_all_regex")]
	pub value: Regex,
}
impl TagBlacklistItem {
	pub fn matches(&self, key: &str, value: &str) -> bool {
		self.key.is_match(key) && self.value.is_match(value)
	}
}

/// Blacklist entry. To be removed, a metric must have its name match the
/// `match_name`, and for each item in `match_tags`, there must be a tag whose key and value
/// matches the `key` and `value` fields respectively of that item.
#[derive(Debug, Clone, Deserialize)]
pub struct BlacklistItem {
	#[serde(deserialize_with = "de_regex", default = "match_all_regex")]
	pub match_name: Regex,
	#[serde(default)]
	pub match_tags: Vec<TagBlacklistItem>,
}

impl BlacklistItem {
	pub fn matches(&self, key: &OpenTsdbKey) -> bool {
		if !self.match_name.is_match(key.name()) {
			return false;
		}
		self.match_tags
			.iter()
			.all(|item| key.tags().iter().any(|(k, v)| item.matches(k, v)))
	}
}

/// Blacklist.
///
/// If any blacklist item matches, the metric is dropped.
#[derive(Default, Debug, Clone, Deserialize)]
#[serde(transparent)]
pub struct Blacklist(pub Vec<BlacklistItem>);

impl Blacklist {
	pub fn matches(&self, key: &OpenTsdbKey) -> bool {
		self.0.iter().any(|item| item.matches(key))
	}

	pub fn read(path: &Path) -> Result<Self, String> {
		let reader = io::BufReader::new(
			fs::File::open(path)
				.map_err(|e| format!("Could not read {}: {}", path.display(), e))?,
		);
		serde_json::from_reader(reader)
			.map_err(|e| format!("Could not parse {}: {}", path.display(), e))
	}
}
