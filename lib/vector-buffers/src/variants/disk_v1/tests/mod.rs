use std::{num::NonZeroU64, path::Path};

use tokio_test::{assert_pending, task::spawn};
use tracing::{Metadata, Span};

use super::{open, Reader, Writer};
use crate::{
    buffer_usage_data::BufferUsageHandle, test::common::install_tracing_helpers,
    variants::disk_v1::reader::FLUSH_INTERVAL, Acker, Bufferable, WhenFull,
};

mod acknowledgements;
mod basic;
mod event_count;
mod naming;
mod size_limits;

// Default of 1GB.
const DEFAULT_DISK_BUFFER_V1_SIZE_BYTES: NonZeroU64 =
    unsafe { NonZeroU64::new_unchecked(1024 * 1024 * 1024) };

pub(crate) fn create_default_buffer_v1<P, R>(data_dir: P) -> (Writer<R>, Reader<R>, Acker)
where
    P: AsRef<Path>,
    R: Bufferable + Clone,
{
    let usage_handle = BufferUsageHandle::noop(WhenFull::Block);
    open(
        data_dir.as_ref(),
        "disk_buffer_v1",
        DEFAULT_DISK_BUFFER_V1_SIZE_BYTES,
        false,
        usage_handle,
    )
    .expect("should not fail to create buffer")
}

pub(crate) fn create_default_buffer_v1_with_usage<P, R>(
    data_dir: P,
) -> (Writer<R>, Reader<R>, Acker, BufferUsageHandle)
where
    P: AsRef<Path>,
    R: Bufferable + Clone,
{
    let usage_handle = BufferUsageHandle::noop(WhenFull::Block);
    let (writer, reader, acker) = open(
        data_dir.as_ref(),
        "disk_buffer_v1",
        DEFAULT_DISK_BUFFER_V1_SIZE_BYTES,
        false,
        usage_handle.clone(),
    )
    .expect("should not fail to create buffer");

    (writer, reader, acker, usage_handle)
}

pub(crate) fn create_default_buffer_v1_with_max_buffer_size<P, R>(
    data_dir: P,
    max_buffer_size: u64,
) -> (Writer<R>, Reader<R>, Acker)
where
    P: AsRef<Path>,
    R: Bufferable + Clone,
{
    let max_buffer_size =
        NonZeroU64::new(max_buffer_size).expect("max buffer size must be non-zero");
    let usage_handle = BufferUsageHandle::noop(WhenFull::Block);
    let (writer, reader, acker) = open(
        data_dir.as_ref(),
        "disk_buffer_v1",
        max_buffer_size,
        false,
        usage_handle,
    )
    .expect("should not fail to create buffer");

    (writer, reader, acker)
}

async fn drive_reader_to_flush<T: Bufferable>(reader: &mut Reader<T>) {
    tokio::time::advance(FLUSH_INTERVAL).await;

    // If we're currently in a span, we drive the `next` call until we definitively trigger a
    // `flush`, otherwise, we just call it once.
    let assertion_registry = install_tracing_helpers();
    let flush_assert = Span::current()
        .metadata()
        .map(Metadata::name)
        .map(|parent_name| {
            assertion_registry
                .build()
                .with_name("flush")
                .with_parent_name(parent_name)
                .was_closed()
                .finalize()
        });

    let mut staged_read = spawn(reader.next());
    if let Some(assertion) = flush_assert {
        while !assertion.try_assert() {
            assert_pending!(staged_read.poll());

            // We advance the time again because otherwise we won't be able to actually call `flush` again.
            tokio::time::advance(FLUSH_INTERVAL).await;
        }
    } else {
        assert_pending!(staged_read.poll());
    }
}

#[macro_export]
macro_rules! assert_reader_writer_v1_positions {
    ($reader:expr, $writer:expr, $expected_reader:expr, $expected_writer:expr) => {{
        let reader_offset = $reader.read_offset;
        let writer_offset = $writer.offset.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            reader_offset, $expected_reader,
            "expected reader offset of {}, got {} instead",
            $expected_reader, reader_offset
        );
        assert_eq!(
            writer_offset, $expected_writer,
            "expected writer offset of {}, got {} instead",
            $expected_writer, writer_offset
        );
    }};
}

#[macro_export]
macro_rules! assert_reader_v1_delete_position {
    ($reader:expr, $expected_reader:expr) => {{
        let delete_offset = $reader.delete_offset;
        assert_eq!(
            delete_offset, $expected_reader,
            "expected delete offset of {}, got {} instead",
            $expected_reader, delete_offset
        );
    }};
}

#[macro_export]
macro_rules! assert_buffer_usage_metrics {
    ($usage:expr, @asserts ($($field:ident => $expected:expr),*) $(,)?) => {{
        let usage_snapshot = $usage.snapshot();
        $(
            assert_eq!($expected, usage_snapshot.$field);
        )*
    }};
    ($usage:expr, @asserts ($($field:ident => $expected:expr),*), recv_events => $recv_events:expr, $($tail:tt)*) => {{
        assert_buffer_usage_metrics!(
            $usage,
            @asserts ($($field => $expected,)* received_event_count => $recv_events),
            $($tail)*
        );
    }};
    ($usage:expr, @asserts ($($field:ident => $expected:expr),*), recv_bytes => $recv_bytes:expr, $($tail:tt)*) => {{
        assert_buffer_usage_metrics!(
            $usage,
            @asserts ($($field => $expected,)* received_byte_size => $recv_bytes),
            $($tail)*
        );
    }};
    ($usage:expr, @asserts ($($field:ident => $expected:expr),*), sent_events => $sent_events:expr, $($tail:tt)*) => {{
        assert_buffer_usage_metrics!(
            $usage,
            @asserts ($($field => $expected,)* sent_event_count => $sent_events),
            $($tail)*
        );
    }};
    ($usage:expr, @asserts ($($field:ident => $expected:expr),*), sent_bytes => $sent_bytes:expr, $($tail:tt)*) => {{
        assert_buffer_usage_metrics!(
            $usage,
            @asserts ($($field => $expected,)* sent_byte_size => $sent_bytes),
            $($tail)*
        );
    }};
    ($usage:expr, @asserts ($($field:ident => $expected:expr),*), none_sent, $($tail:tt)*) => {{
        assert_buffer_usage_metrics!(
            $usage,
            @asserts ($($field => $expected,)* sent_event_count => 0, sent_byte_size => 0),
            $($tail)*
        );
    }};
    ($usage:expr, empty) => {{
        let usage_snapshot = $usage.snapshot();
        assert_eq!(0, usage_snapshot.received_event_count);
        assert_eq!(0, usage_snapshot.received_byte_size);
        assert_eq!(0, usage_snapshot.sent_event_count);
        assert_eq!(0, usage_snapshot.sent_byte_size);
    }};
    ($usage:expr, $($tail:tt)*) => {{
        assert_buffer_usage_metrics!($usage, @asserts (), $($tail)*);
    }};
}
