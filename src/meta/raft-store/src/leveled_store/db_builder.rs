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

//! A builder to build a [`DB`] from a series of sorted key-value.

use std::io;
use std::path::Path;

use databend_common_meta_types::sys_data::SysData;
use futures::Stream;
use futures_util::TryStreamExt;
use rotbl::v001::Rotbl;
use rotbl::v001::RotblMeta;
use rotbl::v001::SeqMarked;

/// Builds a snapshot from series of key-value in `(String, SeqMarked)`
pub(crate) struct DBBuilder {
    path: String,
    rotbl_builder: rotbl::v001::Builder,
}

impl DBBuilder {
    pub fn new<P: AsRef<Path>>(
        path: P,
        rotbl_config: rotbl::v001::Config,
    ) -> Result<Self, io::Error> {
        let p = path.as_ref().to_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid path: {:?}", path.as_ref()),
            )
        })?;

        let inner = rotbl::v001::Builder::new(rotbl_config, path.as_ref())?;

        let b = Self {
            path: p.to_string(),
            rotbl_builder: inner,
        };

        Ok(b)
    }

    #[allow(dead_code)]
    pub fn new_with_default_config<P: AsRef<Path>>(path: P) -> Result<Self, io::Error> {
        let mut config = rotbl::v001::Config::default();
        config.fill_default_values();
        Self::new(path, config)
    }

    /// Append a key-value pair to the builder, the keys must be sorted.
    pub fn append_kv(&mut self, k: String, v: SeqMarked) -> Result<(), io::Error> {
        self.rotbl_builder.append_kv(k, v)
    }

    #[allow(dead_code)]
    pub async fn append_kv_stream(
        &mut self,
        mut strm: impl Stream<Item = Result<(String, SeqMarked), io::Error>>,
    ) -> Result<(), io::Error> {
        let mut strm = std::pin::pin!(strm);
        while let Some((k, v)) = strm.try_next().await? {
            self.append_kv(k, v)?;
        }
        Ok(())
    }

    /// Flush the data to disk and return a read-only table [`Rotbl`] instance.
    pub fn flush(self, sys_data: SysData) -> Result<Rotbl, io::Error> {
        let meta = serde_json::to_string(&sys_data).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("serialize sys_data failed: {}", e),
            )
        })?;

        // the first arg `seq` is not used.
        let rotbl_meta = RotblMeta::new(0, meta);

        let r = self.rotbl_builder.commit(rotbl_meta)?;
        Ok(r)
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}
