use serde::{Deserialize, Serialize};

use crate::{
    config::{
        DataType, GenerateConfig, Input, Output, TransformConfig, TransformContext,
        TransformDescription,
    },
    event::Event,
    schema,
    transforms::{FunctionTransform, OutputBuffer, Transform},
};

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct FieldFilterConfig {
    pub field: String,
    pub value: String,
}

inventory::submit! {
    TransformDescription::new::<FieldFilterConfig>("field_filter")
}

impl GenerateConfig for FieldFilterConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            field: String::new(),
            value: String::new(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "field_filter")]
impl TransformConfig for FieldFilterConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        warn!(
            message =
                r#"The "field_filter" transform is deprecated, use the "filter" transform instead"#
        );
        Ok(Transform::function(FieldFilter::new(
            self.field.clone(),
            self.value.clone(),
        )))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn outputs(&self, _: &schema::Definition) -> Vec<Output> {
        vec![Output::default(DataType::Log)]
    }

    fn transform_type(&self) -> &'static str {
        "field_filter"
    }
}

#[derive(Debug, Clone)]
pub struct FieldFilter {
    field_name: String,
    value: String,
}

impl FieldFilter {
    pub const fn new(field_name: String, value: String) -> Self {
        Self { field_name, value }
    }
}

impl FunctionTransform for FieldFilter {
    fn transform(&mut self, output: &mut OutputBuffer, event: Event) {
        if event
            .as_log()
            .get(self.field_name.as_str())
            .map(|f| f.coerce_to_bytes())
            .map_or(false, |b| b == self.value.as_bytes())
        {
            output.push(event);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{event::Event, transforms::test::transform_one};

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<super::FieldFilterConfig>();
    }

    fn transform_it(msg: &str) -> Option<Event> {
        let mut transform = FieldFilter {
            field_name: "message".into(),
            value: "something".into(),
        };
        let event = Event::from(msg);
        let metadata = event.metadata().clone();
        let result = transform_one(&mut transform, event);
        if let Some(event) = &result {
            assert_eq!(event.metadata(), &metadata);
        }
        result
    }

    #[test]
    fn passes_matching() {
        assert!(transform_it("something").is_some());
    }

    #[test]
    fn drops_not_matching() {
        assert!(transform_it("nothing").is_none());
    }
}
