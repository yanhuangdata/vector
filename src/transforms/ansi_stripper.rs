use serde::{Deserialize, Serialize};

use crate::{
    config::{
        DataType, GenerateConfig, Input, Output, TransformConfig, TransformContext,
        TransformDescription,
    },
    event::{Event, Value},
    internal_events::{
        AnsiStripperError, AnsiStripperFieldInvalidError, AnsiStripperFieldMissingError,
    },
    schema,
    transforms::{FunctionTransform, OutputBuffer, Transform},
    Result,
};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct AnsiStripperConfig {
    field: Option<String>,
}

inventory::submit! {
    TransformDescription::new::<AnsiStripperConfig>("ansi_stripper")
}

impl GenerateConfig for AnsiStripperConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self { field: None }).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "ansi_stripper")]
impl TransformConfig for AnsiStripperConfig {
    async fn build(&self, _context: &TransformContext) -> Result<Transform> {
        let field = self
            .field
            .clone()
            .unwrap_or_else(|| crate::config::log_schema().message_key().into());

        Ok(Transform::function(AnsiStripper { field }))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn outputs(&self, _: &schema::Definition) -> Vec<Output> {
        vec![Output::default(DataType::Log)]
    }

    fn transform_type(&self) -> &'static str {
        "ansi_stripper"
    }
}

#[derive(Clone, Debug)]
pub(self) struct AnsiStripper {
    field: String,
}

impl FunctionTransform for AnsiStripper {
    fn transform(&mut self, output: &mut OutputBuffer, mut event: Event) {
        let log = event.as_mut_log();

        match log.get_mut(self.field.as_str()) {
            None => emit!(AnsiStripperFieldMissingError { field: &self.field }),
            Some(Value::Bytes(ref mut bytes)) => {
                match strip_ansi_escapes::strip(&bytes) {
                    Ok(b) => *bytes = b.into(),
                    Err(error) => emit!(AnsiStripperError {
                        field: &self.field,
                        error
                    }),
                };
            }
            _ => emit!(AnsiStripperFieldInvalidError { field: &self.field }),
        }

        output.push(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{event::LogEvent, transforms::test::transform_one};

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<AnsiStripperConfig>();
    }

    macro_rules! assert_foo_bar {
        ($($in:expr),* $(,)?) => {
            $(
                let mut transform = AnsiStripper {
                    field: "message".into(),
                };

                let log = LogEvent::from($in);
                let mut expected = log.clone();
                expected.insert("message", "foo bar");

                let event = transform_one(&mut transform, log.into()).unwrap();
                assert_eq!(event.into_log(), expected);
            )+
        };
    }

    #[test]
    fn ansi_stripper_transform() {
        assert_foo_bar![
            "\x1b[3;4Hfoo bar",
            "\x1b[3;4ffoo bar",
            "\x1b[3Afoo bar",
            "\x1b[3Bfoo bar",
            "\x1b[3Cfoo bar",
            "\x1b[3Dfoo bar",
            "\x1b[sfoo bar",
            "\x1b[ufoo bar",
            "\x1b[2Jfoo bar",
            "\x1b[Kfoo bar",
            "\x1b[32mfoo\x1b[m bar",
            "\x1b[46mfoo\x1b[0m bar",
            "foo \x1b[46;41mbar",
            "\x1b[=3hfoo bar",
            "\x1b[=3lfoo bar",
            "foo bar",
        ];
    }
}
