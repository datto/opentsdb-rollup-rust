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


//! Miscellaneous utility functions

use lazy_static::lazy_static;
use regex::Regex;
use std::{
	borrow::{
		Borrow,
		Cow,
		ToOwned,
	},
	cmp::Eq,
	collections::HashMap,
	hash::Hash,
};

lazy_static! {
	static ref RE_BEGIN: Regex = Regex::new(r#"^([^":]+|"(?:[^"\\]|\\.)*")(?::|\z)(.*)$"#).unwrap();
	static ref RE_UNESCAPE: Regex = Regex::new(r"\\(.)").unwrap();
	static ref RE_KV: Regex =
		Regex::new(r#"^([^"=,]+|"(?:[^"\\]|\\.)*")=([^"=,]+|"(?:[^"\\]|\\.)*")(?:,|\z)(.*)$"#)
			.unwrap();
}

/// Takes a quoted string with escapes in it and replaces escape sequences in it
/// with the real values. Or, if the string isn't quoted, returns `s` unchanged.
fn unescape_quoted<'a>(s: &'a str) -> Cow<'a, str> {
	if !s.starts_with('"') {
		return s.into();
	}
	assert!(s.ends_with('"'));
	let inner = &s[1..s.len() - 1];
	if !inner.contains('\\') {
		return inner.into();
	}
	RE_UNESCAPE.replace_all(inner, "$1").into()
}

/// Parses a string in the format `ident:key1=value1,key1=value2,...`.
///
/// We use this for passing in settings for the backend
pub fn parse_checkpoint_opts(line: &str) -> Result<(String, HashMap<String, String>), String> {
	let begin_match = RE_BEGIN
		.captures(line)
		.ok_or_else(|| "Could not parse checkpoint options".to_string())?;
	let checkpoint_backend_name = unescape_quoted(&begin_match[1]).into_owned();

	let mut result = HashMap::new();
	let mut remaining = &begin_match[2];
	while remaining != "" {
		let kv_match = RE_KV.captures(remaining).ok_or_else(|| {
			format!(
				"Could not parse key-value pair, starting at {:?}",
				remaining
			)
		})?;
		let key = unescape_quoted(&kv_match[1]).into_owned();
		let value = unescape_quoted(&kv_match[2]).into_owned();
		remaining = kv_match.get(3).unwrap().as_str();
		result.insert(key, value);
	}
	return Ok((checkpoint_backend_name, result));
}

/// Similar to std::collections::hash_map::Entry but the key can be a borrowed
/// version of the type.
pub struct Entry<'a, RefK, K, V>
where
	K: Eq + Hash + Borrow<RefK>,
	RefK: Eq + Hash + ToOwned<Owned = K> + ?Sized,
{
	hm: &'a mut HashMap<K, V>,
	k: &'a RefK,
}

impl<'a, RefK, K, V> Entry<'a, RefK, K, V>
where
	K: Eq + Hash + Borrow<RefK>,
	RefK: Eq + Hash + ToOwned<Owned = K> + ?Sized,
{
	pub fn or_insert(self, default: V) -> &'a mut V {
		self.or_insert_with(move || default)
	}

	pub fn or_insert_with<F: FnOnce() -> V>(self, default: F) -> &'a mut V {
		if !self.hm.contains_key(self.k) {
			self.hm.insert(self.k.to_owned(), (default)());
		}
		self.hm.get_mut(self.k).unwrap()
	}

	pub fn key(&self) -> &RefK {
		self.k
	}

	pub fn and_modify<F: FnOnce(&mut V)>(self, f: F) -> Self {
		if let Some(v) = self.hm.get_mut(self.k) {
			(f)(v);
		}
		return self;
	}
}
impl<'a, RefK, K, V> Entry<'a, RefK, K, V>
where
	K: Eq + Hash + Borrow<RefK>,
	RefK: Eq + Hash + ToOwned<Owned = K> + ?Sized,
	V: Default,
{
	pub fn or_insert_default(self) -> &'a mut V {
		self.or_insert_with(|| Default::default())
	}
}

/// Extension trait for HashMap that adds `entry_to_owned`, similar to `entry` but the key can be a borrowed
/// type that is only allocated if needed.
pub trait HashMapExt<K, V>
where
	K: Eq + Hash,
{
	fn entry_to_owned<'a, RefK>(&'a mut self, k: &'a RefK) -> Entry<'a, RefK, K, V>
	where
		K: Borrow<RefK>,
		RefK: Eq + Hash + ToOwned<Owned = K> + ?Sized;
}
impl<K, V> HashMapExt<K, V> for HashMap<K, V>
where
	K: Eq + Hash,
{
	fn entry_to_owned<'a, RefK>(&'a mut self, k: &'a RefK) -> Entry<'a, RefK, K, V>
	where
		K: Borrow<RefK>,
		RefK: Eq + Hash + ToOwned<Owned = K> + ?Sized,
	{
		Entry { hm: self, k }
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn unescape() {
		assert_eq!(unescape_quoted("helloworld"), "helloworld");
		assert_eq!(unescape_quoted("\"helloworld\""), "helloworld");
		assert_eq!(unescape_quoted("\"hello world\""), "hello world");
		assert_eq!(
			unescape_quoted("\"hello \\\"world\\\"\""),
			"hello \"world\""
		);
	}

	fn res(name: &str, kvs: &[(&str, &str)]) -> (String, HashMap<String, String>) {
		let name = String::from(name);
		let kvs = kvs
			.iter()
			.map(|&(k, v)| (String::from(k), String::from(v)))
			.collect::<HashMap<_, _>>();
		(name, kvs)
	}

	#[test]
	fn parse() {
		assert_eq!(parse_checkpoint_opts("fs"), Ok(res("fs", &[])));
		assert_eq!(
			parse_checkpoint_opts("fs:foo=bar"),
			Ok(res("fs", &[("foo", "bar")]))
		);
		assert_eq!(
			parse_checkpoint_opts("fs:foo=bar,dest=/mybest"),
			Ok(res("fs", &[("foo", "bar"), ("dest", "/mybest")]))
		);
		assert_eq!(
			parse_checkpoint_opts("fs:foo=\"hello \\\"world\\\"\",dest=/mybest"),
			Ok(res(
				"fs",
				&[("foo", "hello \"world\""), ("dest", "/mybest")]
			))
		);
	}
}
