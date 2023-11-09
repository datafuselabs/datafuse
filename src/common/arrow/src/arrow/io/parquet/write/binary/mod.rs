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

mod basic;
mod nested;

pub use basic::array_to_page;
pub(crate) use basic::build_statistics;
pub(super) use basic::encode_delta;
pub(crate) use basic::encode_plain;
pub(super) use basic::ord_binary;
pub use nested::array_to_page as nested_array_to_page;
