//! A collection of formats that can be used to convert from structured events
//! to byte frames.

#![deny(missing_docs)]

mod json;
mod logfmt;
mod native;
mod native_json;
mod raw_message;
mod text;

use std::fmt::Debug;

use dyn_clone::DynClone;
pub use json::{JsonSerializer, JsonSerializerConfig};
pub use logfmt::{LogfmtSerializer, LogfmtSerializerConfig};
pub use native::{NativeSerializer, NativeSerializerConfig};
pub use native_json::{NativeJsonSerializer, NativeJsonSerializerConfig};
pub use raw_message::{RawMessageSerializer, RawMessageSerializerConfig};
pub use text::{TextSerializer, TextSerializerConfig};
use vector_core::event::Event;

/// Serialize a structured event into a byte frame.
pub trait Serializer:
    tokio_util::codec::Encoder<Event, Error = vector_core::Error> + DynClone + Debug + Send + Sync
{
}

/// Default implementation for `Serializer`s that implement
/// `tokio_util::codec::Encoder`.
impl<Encoder> Serializer for Encoder where
    Encoder:
        tokio_util::codec::Encoder<Event, Error = vector_core::Error> + Clone + Debug + Send + Sync
{
}

dyn_clone::clone_trait_object!(Serializer);
