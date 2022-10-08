use std::{collections::HashMap, str};

use bytes::Bytes;
use grok::Pattern;
use lookup::lookup_v2::{parse_path, OwnedPath};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use vector_common::TimeZone;

use crate::{
    config::{
        log_schema, DataType, Input, Output, TransformConfig, TransformContext,
        TransformDescription,
    },
    event::{Event, Value},
    internal_events::{ParserConversionError, ParserMatchError, ParserMissingFieldError},
    schema,
    transforms::{FunctionTransform, OutputBuffer, Transform},
    types::{parse_conversion_map, Conversion},
};

#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("Invalid grok pattern: {}", source))]
    InvalidGrok { source: grok::Error },
}

#[derive(Deserialize, Serialize, Debug, Derivative, Clone)]
#[serde(deny_unknown_fields, default)]
#[derivative(Default)]
pub struct GrokParserConfig {
    pub pattern: String,
    pub field: Option<String>,
    #[derivative(Default(value = "true"))]
    pub drop_field: bool,
    pub types: HashMap<String, String>,
    pub timezone: Option<TimeZone>,
}

inventory::submit! {
    TransformDescription::new::<GrokParserConfig>("grok_parser")
}

impl_generate_config_from_default!(GrokParserConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "grok_parser")]
impl TransformConfig for GrokParserConfig {
    async fn build(&self, context: &TransformContext) -> crate::Result<Transform> {
        let field = self
            .field
            .clone()
            .unwrap_or_else(|| log_schema().message_key().into());

        let mut grok = grok::Grok::with_patterns();

        let timezone = self.timezone.unwrap_or(context.globals.timezone);
        let types = parse_conversion_map(&self.types, timezone)?;

        Ok(grok
            .compile(&self.pattern, true)
            .map(|p| GrokParser {
                pattern: self.pattern.clone(),
                pattern_built: p,
                field: field.clone(),
                drop_field: self.drop_field,
                types,
                paths: HashMap::new(),
            })
            .map(Transform::function)
            .context(InvalidGrokSnafu)?)
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn outputs(&self, _: &schema::Definition) -> Vec<Output> {
        vec![Output::default(DataType::Log)]
    }

    fn enable_concurrency(&self) -> bool {
        true
    }

    fn transform_type(&self) -> &'static str {
        "grok_parser"
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct GrokParser {
    #[derivative(Debug = "ignore")]
    pattern_built: Pattern,
    pattern: String,
    field: String,
    drop_field: bool,
    types: HashMap<String, Conversion>,
    paths: HashMap<String, OwnedPath>,
}

impl Clone for GrokParser {
    fn clone(&self) -> Self {
        Self {
            pattern_built: grok::Grok::with_patterns().compile(&self.pattern, true)
                .expect("Panicked while cloning an already valid Grok parser. For some reason, the pattern could not be built again."),
            pattern: self.pattern.clone(),
            field: self.field.clone(),
            drop_field: self.drop_field,
            types: self.types.clone(),
            paths: self.paths.clone(),
        }
    }
}

impl FunctionTransform for GrokParser {
    fn transform(&mut self, output: &mut OutputBuffer, event: Event) {
        let mut event = event.into_log();
        let value = event.get(self.field.as_str()).map(|s| s.to_string_lossy());

        if let Some(value) = value {
            if let Some(matches) = self.pattern_built.match_against(&value) {
                let drop_field = self.drop_field && matches.get(&self.field).is_none();
                for (name, value) in matches.iter() {
                    let conv = self.types.get(name).unwrap_or(&Conversion::Bytes);
                    match conv.convert::<Value>(Bytes::copy_from_slice(value.as_bytes())) {
                        Ok(value) => {
                            if let Some(path) = self.paths.get(name) {
                                event.insert(path, value);
                            } else {
                                let path = parse_path(name);
                                self.paths.insert(name.to_string(), path.clone());
                                event.insert(&path, value);
                            }
                        }
                        Err(error) => emit!(ParserConversionError { name, error }),
                    }
                }

                if drop_field {
                    event.remove(self.field.as_str());
                }
            } else {
                emit!(ParserMatchError {
                    value: value.as_ref()
                });
            }
        } else {
            emit!(ParserMissingFieldError {
                field: self.field.as_ref()
            });
        }

        output.push(Event::Log(event));
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::GrokParserConfig;
    use crate::{
        config::{log_schema, TransformConfig, TransformContext},
        event::{self, Event, LogEvent},
        transforms::OutputBuffer,
    };

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<GrokParserConfig>();
    }

    async fn parse_log(
        event: &str,
        pattern: &str,
        field: Option<&str>,
        drop_field: bool,
        types: &[(&str, &str)],
    ) -> LogEvent {
        let event = Event::from(event);
        let metadata = event.metadata().clone();
        let mut parser = GrokParserConfig {
            pattern: pattern.into(),
            field: field.map(|s| s.into()),
            drop_field,
            types: types.iter().map(|&(k, v)| (k.into(), v.into())).collect(),
            timezone: Default::default(),
        }
        .build(&TransformContext::default())
        .await
        .unwrap();
        let parser = parser.as_function();

        let mut buf = OutputBuffer::with_capacity(1);
        parser.transform(&mut buf, event);
        let result = buf.first().unwrap().into_log();
        assert_eq!(result.metadata(), &metadata);
        result
    }

    #[tokio::test]
    async fn grok_parser_adds_parsed_fields_to_event() {
        let event = parse_log(
            r#"109.184.11.34 - - [12/Dec/2015:18:32:56 +0100] "GET /administrator/ HTTP/1.1" 200 4263"#,
            "%{HTTPD_COMMONLOG}",
            None,
            true,
            &[],
        ).await;

        let expected = json!({
            "clientip": "109.184.11.34",
            "ident": "-",
            "auth": "-",
            "timestamp": "12/Dec/2015:18:32:56 +0100",
            "verb": "GET",
            "request": "/administrator/",
            "httpversion": "1.1",
            "rawrequest": "",
            "response": "200",
            "bytes": "4263",
        });

        assert_eq!(expected, serde_json::to_value(&event.all_fields()).unwrap());
    }

    #[tokio::test]
    async fn grok_parser_does_nothing_on_no_match() {
        let event = parse_log(
            r#"Help I'm stuck in an HTTP server"#,
            "%{HTTPD_COMMONLOG}",
            None,
            true,
            &[],
        )
        .await;

        assert_eq!(2, event.keys().count());
        assert_eq!(
            event::Value::from("Help I'm stuck in an HTTP server"),
            event[log_schema().message_key()]
        );
        assert!(!event[log_schema().timestamp_key()]
            .to_string_lossy()
            .is_empty());
    }

    #[tokio::test]
    async fn grok_parser_can_not_drop_parsed_field() {
        let event = parse_log(
            r#"109.184.11.34 - - [12/Dec/2015:18:32:56 +0100] "GET /administrator/ HTTP/1.1" 200 4263"#,
            "%{HTTPD_COMMONLOG}",
            None,
            false,
            &[],
        ).await;

        let expected = json!({
            "clientip": "109.184.11.34",
            "ident": "-",
            "auth": "-",
            "timestamp": "12/Dec/2015:18:32:56 +0100",
            "verb": "GET",
            "request": "/administrator/",
            "httpversion": "1.1",
            "rawrequest": "",
            "response": "200",
            "bytes": "4263",
            "message": r#"109.184.11.34 - - [12/Dec/2015:18:32:56 +0100] "GET /administrator/ HTTP/1.1" 200 4263"#,
        });

        assert_eq!(expected, serde_json::to_value(&event.all_fields()).unwrap());
    }

    #[tokio::test]
    async fn grok_parser_does_nothing_on_missing_field() {
        let event = parse_log(
            "i am the only field",
            "^(?<foo>.*)",
            Some("bar"),
            false,
            &[],
        )
        .await;

        assert_eq!(2, event.keys().count());
        assert_eq!(
            event::Value::from("i am the only field"),
            event[log_schema().message_key()]
        );
        assert!(!event[log_schema().timestamp_key()]
            .to_string_lossy()
            .is_empty());
    }

    #[tokio::test]
    async fn grok_parser_coerces_types() {
        let event = parse_log(
            r#"109.184.11.34 - - [12/Dec/2015:18:32:56 +0100] "GET /administrator/ HTTP/1.1" 200 4263"#,
            "%{HTTPD_COMMONLOG}",
            None,
            true,
            &[("response", "int"), ("bytes", "int")],
        ).await;

        let expected = json!({
            "clientip": "109.184.11.34",
            "ident": "-",
            "auth": "-",
            "timestamp": "12/Dec/2015:18:32:56 +0100",
            "verb": "GET",
            "request": "/administrator/",
            "httpversion": "1.1",
            "rawrequest": "",
            "response": 200,
            "bytes": 4263,
        });

        assert_eq!(expected, serde_json::to_value(&event.all_fields()).unwrap());
    }

    #[tokio::test]
    async fn grok_parser_does_not_drop_parsed_message_field() {
        let event = parse_log(
            "12/Dec/2015:18:32:56 +0100 42",
            "%{HTTPDATE:timestamp} %{NUMBER:message}",
            None,
            true,
            &[],
        )
        .await;

        let expected = json!({
            "timestamp": "12/Dec/2015:18:32:56 +0100",
            "message": "42",
        });

        assert_eq!(expected, serde_json::to_value(&event.all_fields()).unwrap());
    }
}
