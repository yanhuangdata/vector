pub mod v1;
pub mod v2;

use serde::{Deserialize, Serialize};

use crate::{
    config::{
        GenerateConfig, Input, Output, TransformConfig, TransformContext, TransformDescription,
    },
    schema,
    transforms::Transform,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
enum V1 {
    #[serde(rename = "1")]
    V1,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct LuaConfigV1 {
    version: Option<V1>,
    #[serde(flatten)]
    config: v1::LuaConfig,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum V2 {
    #[serde(rename = "2")]
    V2,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct LuaConfigV2 {
    version: V2,
    #[serde(flatten)]
    config: v2::LuaConfig,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum LuaConfig {
    V1(LuaConfigV1),
    V2(LuaConfigV2),
}

inventory::submit! {
    TransformDescription::new::<LuaConfig>("lua")
}

impl GenerateConfig for LuaConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"version = "2"
            hooks.process = """#,
        )
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "lua")]
impl TransformConfig for LuaConfig {
    async fn build(&self, _context: &TransformContext) -> crate::Result<Transform> {
        match self {
            LuaConfig::V1(v1) => v1.config.build(),
            LuaConfig::V2(v2) => v2.config.build(),
        }
    }

    fn input(&self) -> Input {
        match self {
            LuaConfig::V1(v1) => v1.config.input(),
            LuaConfig::V2(v2) => v2.config.input(),
        }
    }

    fn outputs(&self, merged_definition: &schema::Definition) -> Vec<Output> {
        match self {
            LuaConfig::V1(v1) => v1.config.outputs(merged_definition),
            LuaConfig::V2(v2) => v2.config.outputs(merged_definition),
        }
    }

    fn transform_type(&self) -> &'static str {
        match self {
            LuaConfig::V1(v1) => v1.config.transform_type(),
            LuaConfig::V2(v2) => v2.config.transform_type(),
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<super::LuaConfig>();
    }
}
