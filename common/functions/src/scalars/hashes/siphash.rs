// Copyright 2021 Datafuse Labs.
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

use std::collections::hash_map::DefaultHasher;
use std::fmt;

use common_datavalues::prelude::*;
use common_datavalues::DataTypeAndNullable;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct SipHashFunction {
    display_name: String,
}

impl SipHashFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(SipHashFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

impl Function for SipHashFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[DataTypeAndNullable]) -> Result<DataTypeAndNullable> {
        let nullable = args.iter().any(|arg| arg.is_nullable());

        let data_type = match args[0].data_type() {
            DataType::Int8(_)
            | DataType::Int16(_)
            | DataType::Int32(_)
            | DataType::Int64(_)
            | DataType::UInt8(_)
            | DataType::UInt16(_)
            | DataType::UInt32(_)
            | DataType::UInt64(_)
            | DataType::Float32(_)
            | DataType::Float64(_)
            | DataType::Date16(_)
            | DataType::Date32(_)
            | DataType::DateTime32(_, _)
            | DataType::String(_) => Ok(DataType::UInt64(nullable)),
            _ => Result::Err(ErrorCode::BadArguments(format!(
                "Function Error: {} does not support {} type parameters",
                self.display_name, args[0]
            ))),
        }?;

        Ok(DataTypeAndNullable::create(&data_type, nullable))
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let series = columns[0].column().to_minimal_array()?;
        let hasher = DFHasher::SipHasher(DefaultHasher::new());
        let res: DataColumn = series.vec_hash(hasher)?.into();
        Ok(res.resize_constant(input_rows))
    }
}

impl fmt::Display for SipHashFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
