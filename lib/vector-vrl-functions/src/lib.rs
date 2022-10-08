pub mod get_metadata_field;
pub mod remove_metadata_field;
pub mod set_metadata_field;
pub mod set_semantic_meaning;

use ::value::Value;
use vrl::prelude::*;

pub(crate) fn keys() -> Vec<Value> {
    vec![value!("datadog_api_key"), value!("splunk_hec_token")]
}

pub fn vrl_functions() -> Vec<Box<dyn vrl::Function>> {
    vec![
        Box::new(get_metadata_field::GetMetadataField) as _,
        Box::new(remove_metadata_field::RemoveMetadataField) as _,
        Box::new(set_metadata_field::SetMetadataField) as _,
        Box::new(set_semantic_meaning::SetSemanticMeaning) as _,
    ]
}
