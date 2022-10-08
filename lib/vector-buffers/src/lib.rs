//! The Vector Core buffer
//!
//! This library implements a channel like functionality, one variant which is
//! solely in-memory and the other that is on-disk. Both variants are bounded.

#![deny(clippy::all)]
#![deny(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::type_complexity)] // long-types happen, especially in async code
#![allow(clippy::must_use_candidate)]

#[macro_use]
extern crate tracing;

mod acknowledgements;
pub use acknowledgements::{Ackable, Acker};

mod buffer_usage_data;

pub mod config;
pub use config::{BufferConfig, BufferType};
use encoding::Encodable;

pub mod encoding;

mod internal_events;

#[cfg(test)]
pub mod test;
pub mod topology;

pub(crate) mod variants;

use std::fmt::Debug;

#[cfg(test)]
use quickcheck::{Arbitrary, Gen};
use serde::{Deserialize, Serialize};
use vector_common::byte_size_of::ByteSizeOf;

#[derive(Deserialize, Serialize, Debug, PartialEq, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum WhenFull {
    Block,
    DropNewest,
    Overflow,
}

impl Default for WhenFull {
    fn default() -> Self {
        WhenFull::Block
    }
}

#[cfg(test)]
impl Arbitrary for WhenFull {
    fn arbitrary(g: &mut Gen) -> Self {
        // TODO: We explicitly avoid generating "overflow" as a possible value because nothing yet
        // supports handling it, and will be defaulted to to using "block" if they encounter
        // "overflow".  Thus, there's no reason to emit it here... yet.
        if bool::arbitrary(g) {
            WhenFull::Block
        } else {
            WhenFull::DropNewest
        }
    }
}

/// An item that can be buffered.
///
/// This supertrait serves as the base trait for any item that can be pushed into a buffer.
pub trait Bufferable:
    ByteSizeOf + Encodable + EventCount + Debug + Send + Sync + Unpin + Sized + 'static
{
}

// Blanket implementation for anything that is already bufferable.
impl<T> Bufferable for T where
    T: ByteSizeOf + Encodable + EventCount + Debug + Send + Sync + Unpin + Sized + 'static
{
}

pub trait EventCount {
    fn event_count(&self) -> usize;
}

impl<T> EventCount for Vec<T>
where
    T: EventCount,
{
    fn event_count(&self) -> usize {
        self.iter().map(EventCount::event_count).sum()
    }
}

impl<'a, T> EventCount for &'a T
where
    T: EventCount,
{
    fn event_count(&self) -> usize {
        (*self).event_count()
    }
}

#[track_caller]
pub(crate) fn spawn_named<T>(
    task: impl std::future::Future<Output = T> + Send + 'static,
    _name: &str,
) -> tokio::task::JoinHandle<T>
where
    T: Send + 'static,
{
    #[cfg(tokio_unstable)]
    return tokio::task::Builder::new().name(_name).spawn(task);

    #[cfg(not(tokio_unstable))]
    tokio::spawn(task)
}
