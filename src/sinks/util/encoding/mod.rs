//! Encoding related code.
//!
//! You'll find three encoding configuration types that can be used:
//!   * [`EncodingConfig<E>`]
//!   * [`EncodingConfigWithDefault<E>`]
//!   * [`EncodingConfigFixed<E>`]
//!
//! These configurations wrap up common fields that can be used via [`EncodingConfiguration`] to
//! provide filtering of fields as events are encoded.  As well, from the name, and from the type
//! `E`, they define an actual codec to use for encoding the event once any configured field rules
//! have been applied.  The codec type parameter is generic and not constrained directly, but to use
//! it with the common encoding infrastructure, you'll likely want to look at [`StandardEncodings`]
//! and [`Encoder`] to understand how it all comes together.
//!
//! ## Configuration types
//!
//! ###  [`EncodingConfig<E>`]
//!
//! This configuration type is the most common: it requires a codec to be specified in all cases,
//! and so is useful when we want the user to choose a specific codec i.e. JSON vs text.
//!
//! ### [`EncodingConfigWithDefault<E>`]
//!
//! This configuration type is practically identical to [`EncodingConfigWithDefault<E>`], except it
//! will use the `Default` implementation of `E` to create the codec if a value isn't specified in
//! the configuration when deserialized.  Similarly, it won't write the codec during serialization
//! if it's already the default value.  This is good when there's an obvious default codec to use,
//! but you still want to provide the ability to change it.
//!
//! ### [`EncodingConfigFixed<E>`]
//!
//! This configuration type is specialized.  It is typically only required when the codec for a
//! given sink is fixed.  An example of this is the Datadog Archives sink, where all output files
//! must be encoded via JSON.  There's no reason for us to make a user specify that in a
//! configuration every time, and on the flip side, there's no way or reason for them to pass in
//! anything other than JSON, so we simply skip serializing and deserializing the codec altogether.
//!
//! This requires that `E` implement `Default`, as we always use the `Default` value when deserializing.
//!
//! ## Using a configuration
//!
//! Using one of the encoding configuration types involves utilizing their implementation of the
//! [`EncodingConfiguration`] trait which defines default methods for interpreting the configuration
//! of the encoding -- "only fields", "timestamp format", etc -- and applying it to a given [`Event`].
//!
//! This can be done simply by calling [`EncodingConfiguration::apply_rules`] on an [`Event`], which
//! applies all configured rules.  This should be done before actual encoding the event via the
//! specific codec.  If you're taking advantage of the implementations of [`Encoder<T>`]  for
//! [`EncodingConfiguration`], this is handled automatically for you.
//!
//! ## Implementation notes
//!
//! You may wonder why we have three different types! **Great question.** `serde` works with the
//! static `*SinkConfig` types when it deserializes our configuration. This means `serde` needs to
//! statically be aware if there is a default for some given `E` of the config. Since
//! We don't require `E: Default` we can't always assume that, so we need to create statically
//! distinct types! Having [`EncodingConfigWithDefault`] is a relatively straightforward way to
//! accomplish this without a bunch of magic.  [`EncodingConfigFixed`] goes a step further and
//! provides a way to force a codec, disallowing an override from being specified.
mod adapter;
mod codec;
mod config;
mod fixed;
mod with_default;

use std::{fmt::Debug, io};

pub use adapter::{
    EncodingConfigAdapter, EncodingConfigMigrator, EncodingConfigWithFramingAdapter,
    EncodingConfigWithFramingMigrator, Transformer,
};
use bytes::BytesMut;
pub use codec::{
    as_tracked_write, StandardEncodings, StandardEncodingsMigrator,
    StandardEncodingsWithFramingMigrator, StandardJsonEncoding, StandardTextEncoding,
};
use codecs::encoding::Framer;
pub use config::EncodingConfig;
pub use fixed::EncodingConfigFixed;
use lookup::lookup_v2::{parse_path, OwnedPath};
use serde::{Deserialize, Serialize};
use tokio_util::codec::Encoder as _;
pub use with_default::EncodingConfigWithDefault;

use crate::{
    event::{Event, LogEvent, MaybeAsLogMut, Value},
    Result,
};

pub trait Encoder<T> {
    /// Encodes the input into the provided writer.
    ///
    /// # Errors
    ///
    /// If an I/O error is encountered while encoding the input, an error variant will be returned.
    fn encode_input(&self, input: T, writer: &mut dyn io::Write) -> io::Result<usize>;

    /// Encodes the input into a String.
    ///
    /// # Errors
    ///
    /// If an I/O error is encountered while encoding the input, an error variant will be returned.
    fn encode_input_to_string(&self, input: T) -> io::Result<String> {
        let mut buffer = vec![];
        self.encode_input(input, &mut buffer)?;
        Ok(String::from_utf8_lossy(&buffer).to_string())
    }
}

impl Encoder<Vec<Event>> for (Transformer, crate::codecs::Encoder<Framer>) {
    fn encode_input(
        &self,
        mut events: Vec<Event>,
        writer: &mut dyn io::Write,
    ) -> io::Result<usize> {
        let mut encoder = self.1.clone();
        let mut bytes_written = 0;
        let batch_prefix = encoder.batch_prefix();
        writer.write_all(batch_prefix)?;
        bytes_written += batch_prefix.len();
        if let Some(last) = events.pop() {
            for mut event in events {
                self.0.transform(&mut event);
                let mut bytes = BytesMut::new();
                encoder
                    .encode(event, &mut bytes)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                writer.write_all(&bytes)?;
                bytes_written += bytes.len();
            }
            let mut event = last;
            self.0.transform(&mut event);
            let mut bytes = BytesMut::new();
            encoder
                .serialize(event, &mut bytes)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            writer.write_all(&bytes)?;
            bytes_written += bytes.len();
        }
        let batch_suffix = encoder.batch_suffix();
        writer.write_all(batch_suffix)?;
        bytes_written += batch_suffix.len();

        Ok(bytes_written)
    }
}

impl Encoder<Event> for (Transformer, crate::codecs::Encoder<()>) {
    fn encode_input(&self, mut event: Event, writer: &mut dyn io::Write) -> io::Result<usize> {
        let mut encoder = self.1.clone();
        self.0.transform(&mut event);
        let mut bytes = BytesMut::new();
        encoder
            .serialize(event, &mut bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        writer.write_all(&bytes)?;
        Ok(bytes.len())
    }
}

/// The behavior of a encoding configuration.
pub trait EncodingConfiguration {
    type Codec;
    // Required Accessors

    fn codec(&self) -> &Self::Codec;
    fn schema(&self) -> &Option<String>;
    fn only_fields(&self) -> &Option<Vec<OwnedPath>>;
    fn except_fields(&self) -> &Option<Vec<String>>;
    fn timestamp_format(&self) -> &Option<TimestampFormat>;

    fn apply_only_fields(&self, log: &mut LogEvent) {
        if let Some(only_fields) = &self.only_fields() {
            let mut to_remove = log
                .keys()
                .filter(|field| {
                    let field_path = parse_path(field);
                    !only_fields
                        .iter()
                        .any(|only| field_path.segments.starts_with(&only.segments[..]))
                })
                .collect::<Vec<_>>();

            // reverse sort so that we delete array elements at the end first rather than
            // the start so that any `nulls` at the end are dropped and empty arrays are
            // pruned
            to_remove.sort_by(|a, b| b.cmp(a));

            for removal in to_remove {
                log.remove_prune(removal.as_str(), true);
            }
        }
    }
    fn apply_except_fields(&self, log: &mut LogEvent) {
        if let Some(except_fields) = &self.except_fields() {
            for field in except_fields {
                log.remove(field.as_str());
            }
        }
    }
    fn apply_timestamp_format(&self, log: &mut LogEvent) {
        if let Some(timestamp_format) = &self.timestamp_format() {
            match timestamp_format {
                TimestampFormat::Unix => {
                    let mut unix_timestamps = Vec::new();
                    for (k, v) in log.all_fields() {
                        if let Value::Timestamp(ts) = v {
                            unix_timestamps.push((k.clone(), Value::Integer(ts.timestamp())));
                        }
                    }
                    for (k, v) in unix_timestamps {
                        log.insert(k.as_str(), v);
                    }
                }
                // RFC3339 is the default serialization of a timestamp.
                TimestampFormat::Rfc3339 => (),
            }
        }
    }

    /// Check that the configuration is valid.
    ///
    /// If an error is returned, the entire encoding configuration should be considered inoperable.
    ///
    /// For example, this checks if `except_fields` and `only_fields` items are mutually exclusive.
    fn validate(&self) -> Result<()> {
        validate_fields(
            self.only_fields().as_deref(),
            self.except_fields().as_deref(),
        )
    }

    /// Apply the EncodingConfig rules to the provided event.
    ///
    /// Currently, this is idempotent.
    fn apply_rules<T>(&self, event: &mut T)
    where
        T: MaybeAsLogMut,
    {
        // No rules are currently applied to metrics
        if let Some(log) = event.maybe_as_log_mut() {
            // Ordering in here should not matter.
            self.apply_except_fields(log);
            self.apply_only_fields(log);
            self.apply_timestamp_format(log);
        }
    }
}

/// Check if `except_fields` and `only_fields` items are mutually exclusive.
///
/// If an error is returned, the entire encoding configuration should be considered inoperable.
pub fn validate_fields(
    only_fields: Option<&[OwnedPath]>,
    except_fields: Option<&[String]>,
) -> Result<()> {
    if let (Some(only_fields), Some(except_fields)) = (only_fields, except_fields) {
        if except_fields.iter().any(|f| {
            let path_iter = parse_path(f);
            only_fields.iter().any(|v| v == &path_iter)
        }) {
            return Err("`except_fields` and `only_fields` should be mutually exclusive.".into());
        }
    }
    Ok(())
}

// These types of traits will likely move into some kind of event container once the
// event layout is refactored, but trying it out here for now.
// Ideally this would return an iterator, but that's not the easiest thing to make generic
pub trait VisitLogMut {
    fn visit_logs_mut<F>(&mut self, func: F)
    where
        F: Fn(&mut LogEvent);
}

impl<T> VisitLogMut for Vec<T>
where
    T: VisitLogMut,
{
    fn visit_logs_mut<F>(&mut self, func: F)
    where
        F: Fn(&mut LogEvent),
    {
        for item in self {
            item.visit_logs_mut(&func);
        }
    }
}

impl VisitLogMut for Event {
    fn visit_logs_mut<F>(&mut self, func: F)
    where
        F: Fn(&mut LogEvent),
    {
        if let Event::Log(log_event) = self {
            func(log_event)
        }
    }
}
impl VisitLogMut for LogEvent {
    fn visit_logs_mut<F>(&mut self, func: F)
    where
        F: Fn(&mut LogEvent),
    {
        func(self);
    }
}

impl<E, T> Encoder<T> for E
where
    E: EncodingConfiguration,
    E::Codec: Encoder<T>,
    T: VisitLogMut,
{
    fn encode_input(&self, mut input: T, writer: &mut dyn io::Write) -> io::Result<usize> {
        input.visit_logs_mut(|log| {
            self.apply_rules(log);
        });
        self.codec().encode_input(input, writer)
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TimestampFormat {
    Unix,
    Rfc3339,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use codecs::{
        CharacterDelimitedEncoder, JsonSerializer, NewlineDelimitedEncoder, TextSerializer,
    };
    use indoc::indoc;

    use super::*;
    use crate::{config::log_schema, sinks::util::encoding::Transformer};

    #[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone)]
    enum TestEncoding {
        Snoot,
        Boop,
    }

    #[derive(Deserialize, Serialize, Debug)]
    #[serde(deny_unknown_fields)]
    struct TestConfig {
        encoding: EncodingConfig<TestEncoding>,
    }

    const TOML_SIMPLE_STRING: &str = r#"encoding = "Snoot""#;

    #[test]
    fn config_string() {
        let config: TestConfig = toml::from_str(TOML_SIMPLE_STRING).unwrap();
        config.encoding.validate().unwrap();
        assert_eq!(config.encoding.codec(), &TestEncoding::Snoot);
    }

    const TOML_SIMPLE_STRUCT: &str = indoc! {r#"
        encoding.codec = "Snoot"
        encoding.except_fields = ["Doop"]
        encoding.only_fields = ["Boop"]
    "#};

    #[test]
    fn config_struct() {
        let config: TestConfig = toml::from_str(TOML_SIMPLE_STRUCT).unwrap();
        config.encoding.validate().unwrap();
        assert_eq!(config.encoding.codec, TestEncoding::Snoot);
        assert_eq!(config.encoding.except_fields, Some(vec!["Doop".into()]));
        assert_eq!(config.encoding.only_fields, Some(vec![parse_path("Boop")]));
    }

    const TOML_EXCLUSIVITY_VIOLATION: &str = indoc! {r#"
        encoding.codec = "Snoot"
        encoding.except_fields = ["Doop"]
        encoding.only_fields = ["Doop"]
    "#};

    #[test]
    fn exclusivity_violation() {
        let config: std::result::Result<TestConfig, _> = toml::from_str(TOML_EXCLUSIVITY_VIOLATION);
        assert!(config.is_err())
    }

    const TOML_EXCEPT_FIELD: &str = indoc! {r#"
        encoding.codec = "Snoot"
        encoding.except_fields = ["a.b.c", "b", "c[0].y", "d\\.z", "e"]
    "#};

    #[test]
    fn test_except() {
        let config: TestConfig = toml::from_str(TOML_EXCEPT_FIELD).unwrap();
        config.encoding.validate().unwrap();
        let mut event = Event::new_empty_log();
        {
            let log = event.as_mut_log();
            log.insert("a", 1);
            log.insert("a.b", 1);
            log.insert("a.b.c", 1);
            log.insert("a.b.d", 1);
            log.insert("b[0]", 1);
            log.insert("b[1].x", 1);
            log.insert("c[0].x", 1);
            log.insert("c[0].y", 1);
            log.insert("d\\.z", 1);
            log.insert("e.a", 1);
            log.insert("e.b", 1);
        }
        config.encoding.apply_rules(&mut event);
        assert!(!event.as_mut_log().contains("a.b.c"));
        assert!(!event.as_mut_log().contains("b"));
        assert!(!event.as_mut_log().contains("b[1].x"));
        assert!(!event.as_mut_log().contains("c[0].y"));
        assert!(!event.as_mut_log().contains("d\\.z"));
        assert!(!event.as_mut_log().contains("e.a"));

        assert!(event.as_mut_log().contains("a.b.d"));
        assert!(event.as_mut_log().contains("c[0].x"));
    }

    const TOML_ONLY_FIELD: &str = indoc! {r#"
        encoding.codec = "Snoot"
        encoding.only_fields = ["a.b.c", "b", "c[0].y", "g\\.z"]
    "#};

    #[test]
    fn test_only() {
        let config: TestConfig = toml::from_str(TOML_ONLY_FIELD).unwrap();
        config.encoding.validate().unwrap();
        let mut event = Event::new_empty_log();
        {
            let log = event.as_mut_log();
            log.insert("a", 1);
            log.insert("a.b", 1);
            log.insert("a.b.c", 1);
            log.insert("a.b.d", 1);
            log.insert("b[0]", 1);
            log.insert("b[1].x", 1);
            log.insert("c[0].x", 1);
            log.insert("c[0].y", 1);
            log.insert("d.y", 1);
            log.insert("d.z", 1);
            log.insert("e[0]", 1);
            log.insert("e[1]", 1);
            log.insert("\"f.z\"", 1);
            log.insert("\"g.z\"", 1);
            log.insert("h", BTreeMap::new());
            log.insert("i", Vec::<Value>::new());
        }
        config.encoding.apply_rules(&mut event);
        assert!(event.as_mut_log().contains("a.b.c"));
        assert!(event.as_mut_log().contains("b"));
        assert!(event.as_mut_log().contains("b[1].x"));
        assert!(event.as_mut_log().contains("c[0].y"));
        assert!(event.as_mut_log().contains("\"g.z\""));

        assert!(!event.as_mut_log().contains("a.b.d"));
        assert!(!event.as_mut_log().contains("c[0].x"));
        assert!(!event.as_mut_log().contains("d"));
        assert!(!event.as_mut_log().contains("e"));
        assert!(!event.as_mut_log().contains("f"));
        assert!(!event.as_mut_log().contains("h"));
        assert!(!event.as_mut_log().contains("i"));
    }

    const TOML_TIMESTAMP_FORMAT: &str = indoc! {r#"
        encoding.codec = "Snoot"
        encoding.timestamp_format = "unix"
    "#};

    #[test]
    fn test_timestamp() {
        let config: TestConfig = toml::from_str(TOML_TIMESTAMP_FORMAT).unwrap();
        config.encoding.validate().unwrap();
        let mut event = Event::from("Demo");
        let timestamp = event
            .as_mut_log()
            .get(log_schema().timestamp_key())
            .unwrap()
            .clone();
        let timestamp = timestamp.as_timestamp().unwrap();
        event
            .as_mut_log()
            .insert("another", Value::Timestamp(*timestamp));

        config.encoding.apply_rules(&mut event);

        match event
            .as_mut_log()
            .get(log_schema().timestamp_key())
            .unwrap()
        {
            Value::Integer(_) => {}
            e => panic!(
                "Timestamp was not transformed into a Unix timestamp. Was {:?}",
                e
            ),
        }
        match event.as_mut_log().get("another").unwrap() {
            Value::Integer(_) => {}
            e => panic!(
                "Timestamp was not transformed into a Unix timestamp. Was {:?}",
                e
            ),
        }
    }

    #[test]
    fn test_encode_batch_json_empty() {
        let encoding = (
            Transformer::default(),
            crate::codecs::Encoder::<Framer>::new(
                CharacterDelimitedEncoder::new(b',').into(),
                JsonSerializer::new().into(),
            ),
        );

        let mut writer = Vec::new();
        let written = encoding.encode_input(vec![], &mut writer).unwrap();
        assert_eq!(written, 2);

        assert_eq!(String::from_utf8(writer).unwrap(), "[]");
    }

    #[test]
    fn test_encode_batch_json_single() {
        let encoding = (
            Transformer::default(),
            crate::codecs::Encoder::<Framer>::new(
                CharacterDelimitedEncoder::new(b',').into(),
                JsonSerializer::new().into(),
            ),
        );

        let mut writer = Vec::new();
        let written = encoding
            .encode_input(
                vec![Event::from(BTreeMap::from([(
                    String::from("key"),
                    Value::from("value"),
                )]))],
                &mut writer,
            )
            .unwrap();
        assert_eq!(written, 17);

        assert_eq!(String::from_utf8(writer).unwrap(), r#"[{"key":"value"}]"#);
    }

    #[test]
    fn test_encode_batch_json_multiple() {
        let encoding = (
            Transformer::default(),
            crate::codecs::Encoder::<Framer>::new(
                CharacterDelimitedEncoder::new(b',').into(),
                JsonSerializer::new().into(),
            ),
        );

        let mut writer = Vec::new();
        let written = encoding
            .encode_input(
                vec![
                    Event::from(BTreeMap::from([(
                        String::from("key"),
                        Value::from("value1"),
                    )])),
                    Event::from(BTreeMap::from([(
                        String::from("key"),
                        Value::from("value2"),
                    )])),
                    Event::from(BTreeMap::from([(
                        String::from("key"),
                        Value::from("value3"),
                    )])),
                ],
                &mut writer,
            )
            .unwrap();
        assert_eq!(written, 52);

        assert_eq!(
            String::from_utf8(writer).unwrap(),
            r#"[{"key":"value1"},{"key":"value2"},{"key":"value3"}]"#
        );
    }

    #[test]
    fn test_encode_batch_ndjson_empty() {
        let encoding = (
            Transformer::default(),
            crate::codecs::Encoder::<Framer>::new(
                NewlineDelimitedEncoder::new().into(),
                JsonSerializer::new().into(),
            ),
        );

        let mut writer = Vec::new();
        let written = encoding.encode_input(vec![], &mut writer).unwrap();
        assert_eq!(written, 0);

        assert_eq!(String::from_utf8(writer).unwrap(), "");
    }

    #[test]
    fn test_encode_batch_ndjson_single() {
        let encoding = (
            Transformer::default(),
            crate::codecs::Encoder::<Framer>::new(
                NewlineDelimitedEncoder::new().into(),
                JsonSerializer::new().into(),
            ),
        );

        let mut writer = Vec::new();
        let written = encoding
            .encode_input(
                vec![Event::from(BTreeMap::from([(
                    String::from("key"),
                    Value::from("value"),
                )]))],
                &mut writer,
            )
            .unwrap();
        assert_eq!(written, 15);

        assert_eq!(String::from_utf8(writer).unwrap(), r#"{"key":"value"}"#);
    }

    #[test]
    fn test_encode_batch_ndjson_multiple() {
        let encoding = (
            Transformer::default(),
            crate::codecs::Encoder::<Framer>::new(
                NewlineDelimitedEncoder::new().into(),
                JsonSerializer::new().into(),
            ),
        );

        let mut writer = Vec::new();
        let written = encoding
            .encode_input(
                vec![
                    Event::from(BTreeMap::from([(
                        String::from("key"),
                        Value::from("value1"),
                    )])),
                    Event::from(BTreeMap::from([(
                        String::from("key"),
                        Value::from("value2"),
                    )])),
                    Event::from(BTreeMap::from([(
                        String::from("key"),
                        Value::from("value3"),
                    )])),
                ],
                &mut writer,
            )
            .unwrap();
        assert_eq!(written, 50);

        assert_eq!(
            String::from_utf8(writer).unwrap(),
            "{\"key\":\"value1\"}\n{\"key\":\"value2\"}\n{\"key\":\"value3\"}"
        );
    }

    #[test]
    fn test_encode_event_json() {
        let encoding = (
            Transformer::default(),
            crate::codecs::Encoder::<()>::new(JsonSerializer::new().into()),
        );

        let mut writer = Vec::new();
        let written = encoding
            .encode_input(
                Event::from(BTreeMap::from([(
                    String::from("key"),
                    Value::from("value"),
                )])),
                &mut writer,
            )
            .unwrap();
        assert_eq!(written, 15);

        assert_eq!(String::from_utf8(writer).unwrap(), r#"{"key":"value"}"#);
    }

    #[test]
    fn test_encode_event_text() {
        let encoding = (
            Transformer::default(),
            crate::codecs::Encoder::<()>::new(TextSerializer::new().into()),
        );

        let mut writer = Vec::new();
        let written = encoding
            .encode_input(
                Event::from(BTreeMap::from([(
                    String::from("message"),
                    Value::from("value"),
                )])),
                &mut writer,
            )
            .unwrap();
        assert_eq!(written, 5);

        assert_eq!(String::from_utf8(writer).unwrap(), r#"value"#);
    }
}
