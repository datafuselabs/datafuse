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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Write;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::RwLock;

#[cfg(target_os = "linux")]
use crate::exception_backtrace_elf::Location;

// 0: not specified 1: disable 2: enable
pub static USER_SET_ENABLE_BACKTRACE: AtomicUsize = AtomicUsize::new(0);

pub fn set_backtrace(switch: bool) {
    if switch {
        USER_SET_ENABLE_BACKTRACE.store(2, Ordering::Relaxed);
    } else {
        USER_SET_ENABLE_BACKTRACE.store(1, Ordering::Relaxed);
    }
}

fn enable_rust_backtrace() -> bool {
    match USER_SET_ENABLE_BACKTRACE.load(Ordering::Relaxed) {
        0 => {}
        1 => return false,
        _ => return true,
    }

    let enabled = match std::env::var("RUST_LIB_BACKTRACE") {
        Ok(s) => s != "0",
        Err(_) => match std::env::var("RUST_BACKTRACE") {
            Ok(s) => s != "0",
            Err(_) => false,
        },
    };

    USER_SET_ENABLE_BACKTRACE.store(enabled as usize + 1, Ordering::Relaxed);
    enabled
}

pub fn capture() -> StackTrace {
    let instance = std::time::Instant::now();
    let stack_trace = StackTrace::capture();
    log::info!(
        "capture stack trace elapsed:{:?}, {:?}",
        instance.elapsed(),
        std::thread::current().name()
    );
    stack_trace
}

#[cfg(target_os = "linux")]
pub struct ResolvedStackFrame {
    pub virtual_address: usize,
    pub physical_address: usize,
    pub symbol: String,
    pub inlined: bool,
    pub location: Option<Location>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub enum StackFrame {
    #[cfg(target_os = "linux")]
    Ip(usize),
    #[cfg(not(target_os = "linux"))]
    Backtrace(backtrace::BacktraceFrame),
}

impl Eq for StackFrame {}

impl PartialEq for StackFrame {
    fn eq(&self, other: &Self) -> bool {
        #[cfg(target_os = "linux")]
        {
            let StackFrame::Ip(addr) = &self;
            let StackFrame::Ip(other_addr) = &other;
            addr == other_addr
        }

        #[cfg(not(target_os = "linux"))]
        {
            let StackFrame::Backtrace(addr) = &self;
            let StackFrame::Backtrace(other_addr) = &other;
            addr.ip() == other_addr.ip()
        }
    }
}

impl Hash for StackFrame {
    fn hash<H: Hasher>(&self, state: &mut H) {
        #[cfg(target_os = "linux")]
        {
            let StackFrame::Ip(addr) = &self;
            addr.hash(state);
        }

        #[cfg(not(target_os = "linux"))]
        {
            let StackFrame::Backtrace(addr) = &self;
            addr.ip().hash(state)
        }
    }
}

//
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct StackTrace {
    pub(crate) frames: Vec<StackFrame>,
}

impl Eq for StackTrace {}

impl PartialEq for StackTrace {
    fn eq(&self, other: &Self) -> bool {
        self.frames == other.frames
    }
}

impl StackTrace {
    pub fn capture() -> StackTrace {
        let mut frames = Vec::with_capacity(50);
        Self::capture_frames(&mut frames);
        StackTrace { frames }
    }

    pub fn no_capture() -> StackTrace {
        StackTrace { frames: vec![] }
    }

    #[cfg(not(target_os = "linux"))]
    fn capture_frames(frames: &mut Vec<StackFrame>) {
        unsafe {
            backtrace::trace_unsynchronized(|frame| {
                frames.push(StackFrame::Backtrace(backtrace::BacktraceFrame::from(
                    frame.clone(),
                )));
                frames.len() != frames.capacity()
            });
        }
    }

    #[cfg(target_os = "linux")]
    fn capture_frames(frames: &mut Vec<StackFrame>) {
        // Safety:
        unsafe {
            backtrace::trace_unsynchronized(|frame| {
                frames.push(StackFrame::Ip(frame.ip() as usize));
                frames.len() != frames.capacity()
            });
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn fmt_frames(&self, display_text: &mut String, address: bool) -> std::fmt::Result {
        let mut frames = std::vec::Vec::with_capacity(self.frames.len());
        for frame in &self.frames {
            let StackFrame::Backtrace(frame) = frame;
            frames.push(frame.clone());
        }

        let mut backtrace = backtrace::Backtrace::from(frames);

        if !address {
            backtrace.resolve();
        }

        writeln!(display_text, "{:?}", backtrace)
    }

    #[cfg(target_os = "linux")]
    fn fmt_frames(&self, f: &mut String, address: bool) -> std::fmt::Result {
        let mut idx = 0;
        crate::exception_backtrace_elf::LibraryManager::instance().resolve_frames(
            &self.frames,
            address,
            |frame| {
                write!(f, "{:4}: {}", idx, frame.symbol)?;

                if frame.inlined {
                    write!(f, "[inlined]")?;
                } else if frame.physical_address != frame.virtual_address {
                    write!(f, "@{:x}", frame.physical_address)?;
                }

                #[allow(clippy::writeln_empty_string)]
                writeln!(f, "")?;
                if let Some(location) = frame.location {
                    write!(f, "             at {}", location.file)?;

                    if let Some(line) = location.line {
                        write!(f, ":{}", line)?;

                        if let Some(column) = location.column {
                            write!(f, ":{}", column)?;
                        }
                    }

                    #[allow(clippy::writeln_empty_string)]
                    writeln!(f, "")?;
                }

                idx += 1;
                Ok(())
            },
        )
    }
}

#[allow(clippy::type_complexity)]
static STACK_CACHE: LazyLock<RwLock<HashMap<Vec<StackFrame>, Arc<Mutex<Option<Arc<String>>>>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

impl Debug for StackTrace {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let instance = std::time::Instant::now();

        let mut display_text = {
            let read_guard = STACK_CACHE.read().unwrap_or_else(PoisonError::into_inner);
            read_guard.get(&self.frames).cloned()
        };

        if display_text.is_none() {
            let mut guard = STACK_CACHE.write().unwrap_or_else(PoisonError::into_inner);

            display_text = Some(match guard.entry(self.frames.clone()) {
                Entry::Occupied(v) => v.get().clone(),
                Entry::Vacant(v) => v.insert(Arc::new(Mutex::new(None))).clone(),
            });
        }

        let display_text_lock = display_text.as_ref().unwrap();
        let mut display_guard = display_text_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        if display_guard.is_none() {
            let mut display_text = String::new();

            self.fmt_frames(&mut display_text, !enable_rust_backtrace())?;
            *display_guard = Some(Arc::new(display_text));
        }

        let display_text = display_guard.as_ref().unwrap().clone();
        drop(display_guard);

        writeln!(f, "{}", display_text)?;
        log::info!(
            "format stack trace elapsed: {:?}, {:?}",
            instance.elapsed(),
            std::thread::current().name()
        );
        Ok(())
    }
}
