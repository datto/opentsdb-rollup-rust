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


use serde::ser::{
	self,
	SerializeMap,
};

/// Wrapper for a (k,v) iterator that serializes all elements as a map
pub(crate) struct SerMapIter<I, K, V>(I, Option<usize>, std::marker::PhantomData<(K, V)>)
where
	I: IntoIterator<Item = (K, V)> + Clone,
	K: ser::Serialize,
	V: ser::Serialize;
impl<I, K, V> SerMapIter<I, K, V>
where
	I: IntoIterator<Item = (K, V)> + Clone,
	K: ser::Serialize,
	V: ser::Serialize,
{
	/// Wraps an iterator, assuming it returns `size` elements.
	pub fn with_size(iter: I, size: usize) -> Self {
		Self(iter, Some(size), Default::default())
	}
}
impl<I, K, V> From<I> for SerMapIter<I, K, V>
where
	I: IntoIterator<Item = (K, V)> + Clone,
	K: ser::Serialize,
	V: ser::Serialize,
{
	fn from(v: I) -> Self {
		Self(v, None, Default::default())
	}
}
impl<I, K, V> ser::Serialize for SerMapIter<I, K, V>
where
	I: IntoIterator<Item = (K, V)> + Clone,
	K: ser::Serialize,
	V: ser::Serialize,
{
	fn serialize<S: ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		let iter = self.0.clone().into_iter();

		// Get size of iter, either from the explicit value or from the iterator's hint.
		let size = self.1.clone().or_else(|| {
			let (min, max) = iter.size_hint();
			if Some(min) == max {
				max
			} else {
				None
			}
		});

		// Serialize
		let mut map = serializer.serialize_map(size)?;
		for (k, v) in iter {
			map.serialize_key(&k)?;
			map.serialize_value(&v)?;
		}
		map.end()
	}
}
