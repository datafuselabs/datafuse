// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ptr::NonNull;

use common_exception::Result;
use common_hashtable::DictionaryKeys;
use common_hashtable::FastHash;

use super::utils::serialize_columns;
use crate::types::DataType;
use crate::Column;
use crate::HashMethod;
use crate::KeysState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashMethodDictionarySerializer {
    pub dict_keys: usize,
}

impl HashMethod for HashMethodDictionarySerializer {
    type HashKey = DictionaryKeys;
    type HashKeyIter<'a> = std::slice::Iter<'a, DictionaryKeys>;

    fn name(&self) -> String {
        "DictionarySerializer".to_string()
    }

    fn build_keys_state(
        &self,
        group_columns: &[(Column, DataType)],
        num_rows: usize,
    ) -> Result<KeysState> {
        // fixed type serialize one column to dictionary
        let mut dictionary_columns = Vec::with_capacity(group_columns.len());
        let mut other_columns = Vec::new();
        for (group_column, _) in group_columns {
            match group_column {
                Column::String(v) | Column::Variant(v) | Column::Bitmap(v) => {
                    debug_assert_eq!(v.len(), num_rows);
                    dictionary_columns.push(v.clone());
                }
                _ => other_columns.push(group_column.clone()),
            }
        }

        if !other_columns.is_empty() {
            // The serialize_size is equal to the number of bytes required by serialization.
            let mut serialize_size = 0;
            for column in other_columns.iter() {
                serialize_size += column.serialize_size();
            }
            dictionary_columns.push(serialize_columns(&other_columns, num_rows, serialize_size));
        }

        let mut keys = Vec::with_capacity(num_rows * dictionary_columns.len());
        let mut points = Vec::with_capacity(num_rows * dictionary_columns.len());

        for row in 0..num_rows {
            let start = points.len();

            for dictionary_column in &dictionary_columns {
                points.push(NonNull::from(unsafe {
                    dictionary_column.index_unchecked(row)
                }));
            }

            keys.push(DictionaryKeys::create(&points[start..]))
        }

        Ok(KeysState::Dictionary {
            dictionaries: keys,
            keys_point: points,
            columns: dictionary_columns,
        })
    }

    fn build_keys_iter<'a>(&self, keys_state: &'a KeysState) -> Result<Self::HashKeyIter<'a>> {
        match keys_state {
            KeysState::Dictionary { dictionaries, .. } => Ok(dictionaries.iter()),
            _ => unreachable!(),
        }
    }

    fn build_keys_iter_and_hashes<'a>(
        &self,
        keys_state: &'a KeysState,
    ) -> Result<(Self::HashKeyIter<'a>, Vec<u64>)> {
        match keys_state {
            KeysState::Dictionary { dictionaries, .. } => {
                let mut hashes = Vec::with_capacity(dictionaries.len());
                hashes.extend(dictionaries.iter().map(|key| key.fast_hash()));
                Ok((dictionaries.iter(), hashes))
            }
            _ => unreachable!(),
        }
    }
}
