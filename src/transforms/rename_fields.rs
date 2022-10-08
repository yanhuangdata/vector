use indexmap::map::IndexMap;
use serde::{Deserialize, Serialize};

use crate::{
    config::{
        DataType, GenerateConfig, Input, Output, TransformConfig, TransformContext,
        TransformDescription,
    },
    event::Event,
    internal_events::{FieldOverwritten, RenameFieldsFieldDoesNotExist},
    schema,
    serde::Fields,
    transforms::{FunctionTransform, OutputBuffer, Transform},
};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct RenameFieldsConfig {
    pub fields: Fields<String>,
    drop_empty: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct RenameFields {
    fields: IndexMap<String, String>,
    drop_empty: bool,
}

inventory::submit! {
    TransformDescription::new::<RenameFieldsConfig>("rename_fields")
}

impl GenerateConfig for RenameFieldsConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(r#"fields.old_field_name = "new_field_name""#).unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "rename_fields")]
impl TransformConfig for RenameFieldsConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        let mut fields = IndexMap::default();
        for (key, value) in self.fields.clone().all_fields() {
            fields.insert(key.to_string(), value.to_string());
        }
        Ok(Transform::function(RenameFields::new(
            fields,
            self.drop_empty.unwrap_or(false),
        )?))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn outputs(&self, _: &schema::Definition) -> Vec<Output> {
        vec![Output::default(DataType::Log)]
    }

    fn transform_type(&self) -> &'static str {
        "rename_fields"
    }
}

impl RenameFields {
    pub fn new(fields: IndexMap<String, String>, drop_empty: bool) -> crate::Result<Self> {
        Ok(Self { fields, drop_empty })
    }
}

impl FunctionTransform for RenameFields {
    fn transform(&mut self, output: &mut OutputBuffer, mut event: Event) {
        for (old_key, new_key) in &self.fields {
            let log = event.as_mut_log();
            match log.remove_prune(old_key.as_str(), self.drop_empty) {
                Some(v) => {
                    if event.as_mut_log().insert(new_key.as_str(), v).is_some() {
                        emit!(FieldOverwritten { field: old_key });
                    }
                }
                None => {
                    emit!(RenameFieldsFieldDoesNotExist { field: old_key });
                }
            }
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
        crate::test_util::test_generate_config::<RenameFieldsConfig>();
    }

    #[test]
    fn rename_fields() {
        let mut log = LogEvent::from("message");
        let mut expected = log.clone();
        log.insert("to_move", "some value");
        log.insert("do_not_move", "not moved");
        expected.insert("moved", "some value");
        expected.insert("do_not_move", "not moved");

        let mut fields = IndexMap::new();
        fields.insert(String::from("to_move"), String::from("moved"));
        fields.insert(
            String::from("not_present"),
            String::from("should_not_exist"),
        );
        let mut transform = RenameFields::new(fields, false).unwrap();

        let new_event = transform_one(&mut transform, log.into()).unwrap();

        assert_eq!(new_event.into_log(), expected);
    }
}
