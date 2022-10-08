use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use tokio_util::codec::Encoder;
use vector_common::encode_logfmt;
use vector_core::{config::DataType, event::Event, schema};

/// Config used to build a `LogfmtSerializer`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct LogfmtSerializerConfig;

impl LogfmtSerializerConfig {
    /// Creates a new `LogfmtSerializerConfig`.
    pub const fn new() -> Self {
        Self
    }

    /// Build the `LogfmtSerializer` from this configuration.
    pub const fn build(&self) -> LogfmtSerializer {
        LogfmtSerializer
    }

    /// The data type of events that are accepted by `JsonSerializer`.
    pub fn input_type(&self) -> DataType {
        DataType::Log
    }

    /// The schema required by the serializer.
    pub fn schema_requirement(&self) -> schema::Requirement {
        // While technically we support `Value` variants that can't be losslessly serialized to
        // logfmt, we don't want to enforce that limitation to users yet.
        schema::Requirement::empty()
    }
}

/// Serializer that converts an `Event` to bytes using the logfmt format.
#[derive(Debug, Clone)]
pub struct LogfmtSerializer;

impl LogfmtSerializer {
    /// Creates a new `LogfmtSerializer`.
    pub const fn new() -> Self {
        Self
    }
}

impl Encoder<Event> for LogfmtSerializer {
    type Error = vector_core::Error;

    fn encode(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        let log = event.as_log();
        let string = encode_logfmt::to_string(log.as_map())?;
        buffer.extend_from_slice(string.as_bytes());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use vector_common::btreemap;
    use vector_core::event::Value;

    #[test]
    fn serialize_logfmt() {
        let event = Event::from(btreemap! {
            "foo" => Value::from("bar")
        });
        let mut serializer = LogfmtSerializer::new();
        let mut bytes = BytesMut::new();

        serializer.encode(event, &mut bytes).unwrap();

        assert_eq!(bytes.freeze(), "foo=bar");
    }
}
