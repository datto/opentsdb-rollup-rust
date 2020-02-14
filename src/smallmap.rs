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


//! Immutable mapping type backed by a flat array.

use serde::{
	de,
	ser,
	ser::SerializeMap,
	Deserialize,
	Serialize,
};
use std::{
	borrow::Borrow,
	cmp::Ordering,
	fmt,
	hash::{
		Hash,
		Hasher,
	},
	marker::PhantomData,
};

/// Immutable mapping type backed by a flat array.
///
/// More performant for maps with few elements. Elements are sorted by key.
///
/// The implementation supports multiple of the same key, though some functions
/// may work oddly in such cases. It should not be relied upon.
#[derive(Debug, Clone)]
pub struct SmallMap<K: Ord + Eq, V> {
	/// Use boxed slice rather than vec here to save some space;
	/// we don't need extra capacity for an immutable structure.
	data: Box<[(K, V)]>,
}
impl<K: Ord + Eq, V> SmallMap<K, V> {
	/// Creates a SmallMap by taking ownership of a boxed slice.
	///
	/// This is the most efficient way to create a SmallMap, since it uses a boxed slice
	/// internally.
	///
	/// The input data does not need to be sorted; this function will sort
	pub fn from_boxed_slice(mut data: Box<[(K, V)]>) -> Self {
		data.sort_unstable_by(|&(ref k1, _), &(ref k2, _)| k1.cmp(&k2));
		Self { data }
	}

	/// Creates a SmallMap by taking ownership of a `Vec`.
	///
	/// Same as `from_boxed_slice(data.into_boxed_slice())`
	pub fn from_vec(data: Vec<(K, V)>) -> Self {
		Self::from_boxed_slice(data.into_boxed_slice())
	}

	/// Gets a value for a corresponding key, or `None` if no entry exists.
	///
	/// If multiple of the same key is in the map, this implementation picks an
	/// arbitrary value.
	pub fn get<Q>(&self, key: &Q) -> Option<&V>
	where
		K: Borrow<Q>,
		Q: Ord + ?Sized,
	{
		self.data
			.binary_search_by_key(&key, |&(ref k, _)| k.borrow())
			.ok()
			.map(|index| &self.data[index].1)
	}

	/// Checks if the map contains a key.
	pub fn contains_key<Q>(&self, key: &Q) -> bool
	where
		K: Borrow<Q>,
		Q: Ord,
	{
		self.get(key).is_some()
	}

	/// Gets how many elements are in the map.
	pub fn len(&self) -> usize {
		self.data.len()
	}

	/// Iterates over the collection, in order from least to greatest.
	pub fn iter<'a>(&'a self) -> impl Iterator<Item = (&'a K, &'a V)> {
		self.data.iter().map(|&(ref k, ref v)| (k, v))
	}

	/// Consumes the SmallMap and returns the underlying storage.
	pub fn into_boxed_slice(self) -> Box<[(K, V)]> {
		self.data
	}
}

impl<K: Ord + Eq, V> std::iter::FromIterator<(K, V)> for SmallMap<K, V> {
	fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
		return SmallMap::from_vec(iter.into_iter().collect::<Vec<_>>());
	}
}

impl<K: Ord + Eq, V> std::iter::IntoIterator for SmallMap<K, V> {
	type IntoIter = std::vec::IntoIter<(K, V)>;
	type Item = (K, V);

	fn into_iter(self) -> Self::IntoIter {
		Vec::from(self.data).into_iter()
	}
}

impl<K: Ord + Eq, V> Default for SmallMap<K, V> {
	fn default() -> Self {
		Self::from_vec(vec![])
	}
}

impl<K: Ord + Eq, V: PartialOrd> PartialOrd for SmallMap<K, V> {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		self.iter().partial_cmp(other.iter())
	}
}

impl<K: Ord + Eq, V: Ord> Ord for SmallMap<K, V> {
	fn cmp(&self, other: &Self) -> Ordering {
		self.iter().cmp(other.iter())
	}
}

impl<K: Ord + Eq + Serialize, V: Serialize> Serialize for SmallMap<K, V> {
	fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		let mut map = serializer.serialize_map(Some(self.len()))?;
		for (k, v) in self.iter() {
			map.serialize_entry(k, v)?;
		}
		map.end()
	}
}

impl<'de, K: Ord + Eq + Deserialize<'de>, V: Deserialize<'de>> Deserialize<'de> for SmallMap<K, V> {
	fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		deserializer.deserialize_map(DeVisitor {
			_ph: Default::default(),
		})
	}
}
struct DeVisitor<K: Ord + Eq, V> {
	_ph: PhantomData<fn() -> SmallMap<K, V>>,
}
impl<'de, K: Ord + Eq + Deserialize<'de>, V: Deserialize<'de>> de::Visitor<'de>
	for DeVisitor<K, V>
{
	type Value = SmallMap<K, V>;

	fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("a map")
	}

	fn visit_map<M: de::MapAccess<'de>>(self, mut access: M) -> Result<Self::Value, M::Error> {
		let mut vec = Vec::with_capacity(access.size_hint().unwrap_or(0));
		while let Some((k, v)) = access.next_entry()? {
			vec.push((k, v));
		}
		Ok(SmallMap::from_vec(vec))
	}
}
impl<K: Ord + Eq, V: PartialEq> PartialEq for SmallMap<K, V> {
	fn eq(&self, other: &Self) -> bool {
		if self.len() != other.len() {
			return false;
		}
		self.iter()
			.zip(other.iter())
			.all(|((k1, v1), (k2, v2))| k1 == k2 && v1 == v2)
	}
}
impl<K: Ord + Eq, V: Eq> Eq for SmallMap<K, V> {}

impl<K: Ord + Eq + Hash, V: Hash> Hash for SmallMap<K, V> {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.len().hash(state);
		for (k, v) in self.iter() {
			k.hash(state);
			v.hash(state);
		}
	}
}
