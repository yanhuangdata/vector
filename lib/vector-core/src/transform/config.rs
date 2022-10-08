use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use indexmap::IndexMap;

use crate::{
    config::{ComponentKey, GlobalOptions, Input, Output},
    schema,
};

#[derive(Debug, serde::Serialize)]
pub struct InnerTopologyTransform {
    pub inputs: Vec<String>,
    pub inner: Box<dyn TransformConfig>,
}

#[derive(Debug, Default)]
pub struct InnerTopology {
    pub inner: IndexMap<ComponentKey, InnerTopologyTransform>,
    pub outputs: Vec<(ComponentKey, Vec<Output>)>,
}

impl InnerTopology {
    pub fn outputs(&self) -> Vec<String> {
        self.outputs
            .iter()
            .flat_map(|(name, outputs)| {
                outputs.iter().map(|output| match output.port {
                    Some(ref port) => name.port(port),
                    None => name.id().to_string(),
                })
            })
            .collect()
    }
}

#[derive(Debug)]
pub struct TransformContext {
    // This is optional because currently there are a lot of places we use `TransformContext` that
    // may not have the relevant data available (e.g. tests). In the future it'd be nice to make it
    // required somehow.
    pub key: Option<ComponentKey>,
    pub globals: GlobalOptions,
    #[cfg(feature = "vrl")]
    pub enrichment_tables: enrichment::TableRegistry,

    /// Tracks the schema IDs assigned to schemas exposed by the transform.
    ///
    /// Given a transform can expose multiple [`Output`] channels, the ID is tied to the identifier of
    /// that `Output`.
    pub schema_definitions: HashMap<Option<String>, schema::Definition>,

    /// The schema definition created by merging all inputs of the transform.
    ///
    /// This information can be used by transforms that behave differently based on schema
    /// information, such as the `remap` transform, which passes this information along to the VRL
    /// compiler such that type coercion becomes less of a need for operators writing VRL programs.
    pub merged_schema_definition: schema::Definition,
}

impl Default for TransformContext {
    fn default() -> Self {
        Self {
            key: Default::default(),
            globals: Default::default(),
            #[cfg(feature = "vrl")]
            enrichment_tables: Default::default(),
            schema_definitions: HashMap::from([(None, schema::Definition::empty())]),
            merged_schema_definition: schema::Definition::empty(),
        }
    }
}

impl TransformContext {
    // clippy allow avoids an issue where vrl is flagged off and `globals` is
    // the sole field in the struct
    #[allow(clippy::needless_update)]
    pub fn new_with_globals(globals: GlobalOptions) -> Self {
        Self {
            globals,
            ..Default::default()
        }
    }

    #[cfg(any(test, feature = "test"))]
    pub fn new_test(schema_definitions: HashMap<Option<String>, schema::Definition>) -> Self {
        Self {
            schema_definitions,
            ..Default::default()
        }
    }
}

#[async_trait]
#[typetag::serde(tag = "type")]
pub trait TransformConfig: core::fmt::Debug + Send + Sync + dyn_clone::DynClone {
    async fn build(&self, globals: &TransformContext)
        -> crate::Result<crate::transform::Transform>;

    fn input(&self) -> Input;

    /// Returns a list of outputs to which this transform can deliver events.
    ///
    /// The provided `merged_definition` can be used by transforms to understand the expected shape
    /// of events flowing through the transform.
    fn outputs(&self, merged_definition: &schema::Definition) -> Vec<Output>;

    /// Verifies that the provided outputs and the inner plumbing of the transform are valid.
    fn validate(&self, _merged_definition: &schema::Definition) -> Result<(), Vec<String>> {
        Ok(())
    }

    fn transform_type(&self) -> &'static str;

    /// Return true if the transform is able to be run across multiple tasks simultaneously with no
    /// concerns around statefulness, ordering, etc.
    fn enable_concurrency(&self) -> bool {
        false
    }

    /// Allows to detect if a transform can be embedded in another transform.
    /// It's used by the pipelines transform for now.
    fn nestable(&self, _parents: &HashSet<&'static str>) -> bool {
        true
    }

    /// Allows a transform configuration to expand itself into multiple "child"
    /// transformations to replace it. This allows a transform to act as a macro
    /// for various patterns.
    fn expand(
        &mut self,
        _name: &ComponentKey,
        _inputs: &[String],
    ) -> crate::Result<Option<InnerTopology>> {
        Ok(None)
    }
}

dyn_clone::clone_trait_object!(TransformConfig);
