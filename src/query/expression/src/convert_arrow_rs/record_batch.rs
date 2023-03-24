// Copyright 2023 Datafuse Labs.
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

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::ArrowError;

use crate::DataBlock;
use crate::DataSchema;

impl DataBlock {
    pub fn to_record_batch(self, data_schema: &DataSchema) -> Result<RecordBatch, ArrowError> {
        let mut arrays = Vec::with_capacity(self.columns().len());
        for entry in self.convert_to_full().columns() {
            let column = entry.value.to_owned().into_column().unwrap();
            arrays.push(column.into_arrow_rs()?)
        }
        let schema = Arc::new(data_schema.into());
        RecordBatch::try_new(schema, arrays)
    }
}
