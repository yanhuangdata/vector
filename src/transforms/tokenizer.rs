use std::{collections::HashMap, str};

use bytes::Bytes;
use lookup::lookup_v2::{parse_path, OwnedPath};
use serde::{Deserialize, Serialize};
use vector_common::{tokenize::parse, TimeZone};

use crate::{
    config::{DataType, Input, Output, TransformConfig, TransformContext, TransformDescription},
    event::{Event, Value},
    internal_events::{ParserConversionError, ParserMissingFieldError},
    schema,
    transforms::{FunctionTransform, OutputBuffer, Transform},
    types::{parse_check_conversion_map, Conversion},
};

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct TokenizerConfig {
    pub field_names: Vec<String>,
    pub field: Option<String>,
    pub drop_field: bool,
    pub types: HashMap<String, String>,
    pub timezone: Option<TimeZone>,
}

inventory::submit! {
    TransformDescription::new::<TokenizerConfig>("tokenizer")
}

impl_generate_config_from_default!(TokenizerConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "tokenizer")]
impl TransformConfig for TokenizerConfig {
    async fn build(&self, context: &TransformContext) -> crate::Result<Transform> {
        let field = self
            .field
            .clone()
            .unwrap_or_else(|| crate::config::log_schema().message_key().to_string());

        let timezone = self.timezone.unwrap_or(context.globals.timezone);
        let types = parse_check_conversion_map(&self.types, &self.field_names, timezone)?;

        // don't drop the source field if it's getting overwritten by a parsed value
        let drop_field = self.drop_field && !self.field_names.iter().any(|f| **f == *field);

        Ok(Transform::function(Tokenizer::new(
            self.field_names.clone(),
            field,
            drop_field,
            types,
        )))
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
        "tokenizer"
    }
}

#[derive(Clone, Debug)]
pub struct Tokenizer {
    field_names: Vec<(String, OwnedPath, Conversion)>,
    field: String,
    drop_field: bool,
}

impl Tokenizer {
    pub fn new(
        field_names: Vec<String>,
        field: String,
        drop_field: bool,
        types: HashMap<String, Conversion>,
    ) -> Self {
        let field_names = field_names
            .into_iter()
            .map(|name| {
                let conversion = types.get(&name).unwrap_or(&Conversion::Bytes).clone();
                let path = parse_path(&name);
                (name, path, conversion)
            })
            .collect();

        Self {
            field_names,
            field,
            drop_field,
        }
    }
}

impl FunctionTransform for Tokenizer {
    fn transform(&mut self, output: &mut OutputBuffer, mut event: Event) {
        let value = event
            .as_log()
            .get(self.field.as_str())
            .map(|s| s.to_string_lossy());

        if let Some(value) = &value {
            for ((name, path, conversion), value) in
                self.field_names.iter().zip(parse(value).into_iter())
            {
                match conversion.convert::<Value>(Bytes::copy_from_slice(value.as_bytes())) {
                    Ok(value) => {
                        event.as_mut_log().insert(path, value);
                    }
                    Err(error) => {
                        emit!(ParserConversionError { name, error });
                    }
                }
            }
            if self.drop_field {
                event.as_mut_log().remove(self.field.as_str());
            }
        } else {
            emit!(ParserMissingFieldError { field: &self.field });
        };

        output.push(event)
    }
}

#[cfg(test)]
mod tests {
    use ordered_float::NotNan;

    use super::TokenizerConfig;
    use crate::{
        config::{TransformConfig, TransformContext},
        event::{Event, LogEvent, Value},
        transforms::OutputBuffer,
    };

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<TokenizerConfig>();
    }

    async fn parse_log(
        text: &str,
        fields: &str,
        field: Option<&str>,
        drop_field: bool,
        types: &[(&str, &str)],
    ) -> LogEvent {
        let event = Event::from(text);
        let field_names = fields.split(' ').map(|s| s.into()).collect::<Vec<String>>();
        let field = field.map(|f| f.into());
        let mut parser = TokenizerConfig {
            field_names,
            field,
            drop_field,
            types: types.iter().map(|&(k, v)| (k.into(), v.into())).collect(),
            timezone: Default::default(),
        }
        .build(&TransformContext::default())
        .await
        .unwrap();
        let parser = parser.as_function();

        let metadata = event.metadata().clone();
        let mut buf = OutputBuffer::with_capacity(1);
        parser.transform(&mut buf, event);
        let result = buf.into_events().next().unwrap().into_log();
        assert_eq!(result.metadata(), &metadata);
        result
    }

    #[tokio::test]
    async fn tokenizer_adds_parsed_field_to_event() {
        let log = parse_log("1234 5678", "status time", None, false, &[]).await;

        assert_eq!(log["status"], "1234".into());
        assert_eq!(log["time"], "5678".into());
        assert!(log.get("message").is_some());
    }

    #[tokio::test]
    async fn tokenizer_does_drop_parsed_field() {
        let log = parse_log("1234 5678", "status time", Some("message"), true, &[]).await;

        assert_eq!(log["status"], "1234".into());
        assert_eq!(log["time"], "5678".into());
        assert!(log.get("message").is_none());
    }

    #[tokio::test]
    async fn tokenizer_does_not_drop_same_name_parsed_field() {
        let log = parse_log("1234 yes", "status message", Some("message"), true, &[]).await;

        assert_eq!(log["status"], "1234".into());
        assert_eq!(log["message"], "yes".into());
    }

    #[tokio::test]
    async fn tokenizer_coerces_fields_to_types() {
        let log = parse_log(
            "1234 yes 42.3 word",
            "code flag number rest",
            None,
            false,
            &[("flag", "bool"), ("code", "integer"), ("number", "float")],
        )
        .await;

        assert_eq!(log["number"], Value::Float(NotNan::new(42.3).unwrap()));
        assert_eq!(log["flag"], Value::Boolean(true));
        assert_eq!(log["code"], Value::Integer(1234_i64));
        assert_eq!(log["rest"], Value::Bytes("word".into()));
    }

    #[tokio::test]
    async fn tokenizer_keeps_dash_as_dash() {
        let log = parse_log(
            "1234 - foo",
            "code who why",
            None,
            false,
            &[("code", "integer"), ("who", "string"), ("why", "string")],
        )
        .await;
        assert_eq!(log["code"], Value::Integer(1234_i64));
        assert_eq!(log["who"], Value::Bytes("-".into()));
        assert_eq!(log["why"], Value::Bytes("foo".into()));
    }
}
