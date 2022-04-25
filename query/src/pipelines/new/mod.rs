// Copyright 2022 Datafuse Labs.
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

// pub mod executor;

// #[cfg(test)]
pub mod executor;

// #[not(cfg(test))]
// mod executor;

mod pipe;
mod pipeline;
mod pipeline_builder;
pub mod processors;
mod profiling;
mod unsafe_cell_wrap;

pub use pipe::NewPipe;
pub use pipe::SourcePipeBuilder;
pub use pipeline::NewPipeline;
pub use pipeline_builder::QueryPipelineBuilder;
pub use profiling::ExecutorProfiling;
pub use profiling::FuseSourceTracker;
pub use profiling::FuseTableProcessInfo;
pub use profiling::ProcessInfo;
