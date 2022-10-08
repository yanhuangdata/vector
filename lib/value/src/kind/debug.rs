use crate::kind::Unknown;
use crate::{Kind, Value};
use std::collections::BTreeMap;

impl Kind {
    /// Returns a tree representation of `Kind`, in a more human readable format.
    /// This is for debugging / development purposes only.
    #[must_use]
    pub fn debug_info(&self) -> BTreeMap<String, Value> {
        let mut output = BTreeMap::new();
        insert_kind(&mut output, self, true);
        output
    }
}

fn insert_kind(tree: &mut BTreeMap<String, Value>, kind: &Kind, show_unknown: bool) {
    if kind.is_any() {
        insert_if_true(tree, "any", true);
    } else {
        insert_if_true(tree, "bytes", kind.contains_bytes());
        insert_if_true(tree, "integer", kind.contains_integer());
        insert_if_true(tree, "float", kind.contains_float());
        insert_if_true(tree, "boolean", kind.contains_boolean());
        insert_if_true(tree, "timestamp", kind.contains_timestamp());
        insert_if_true(tree, "regex", kind.contains_regex());
        insert_if_true(tree, "null", kind.contains_null());

        if let Some(fields) = &kind.object {
            let mut object_tree = BTreeMap::new();
            for (field, field_kind) in fields.known() {
                let mut field_tree = BTreeMap::new();
                insert_kind(&mut field_tree, field_kind, show_unknown);
                object_tree.insert(field.name.clone(), Value::Object(field_tree));
            }
            tree.insert("object".to_owned(), Value::Object(object_tree));
            if show_unknown {
                insert_unknown(tree, fields.unknown(), "object");
            }
        }

        if let Some(indices) = &kind.array {
            let mut array_tree = BTreeMap::new();
            for (index, index_kind) in indices.known() {
                let mut index_tree = BTreeMap::new();
                insert_kind(&mut index_tree, index_kind, show_unknown);
                array_tree.insert(index.to_string(), Value::Object(index_tree));
            }
            tree.insert("array".to_owned(), Value::Object(array_tree));
            if show_unknown {
                insert_unknown(tree, indices.unknown(), "array");
            }
        }
    }
}

fn insert_unknown(tree: &mut BTreeMap<String, Value>, unknown: Option<&Unknown>, prefix: &str) {
    if let Some(unknown) = unknown {
        let mut unknown_tree = BTreeMap::new();
        insert_kind(&mut unknown_tree, unknown.to_kind().as_ref(), false);
        if unknown.is_exact() {
            tree.insert(
                format!("{}_unknown_exact", prefix),
                Value::Object(unknown_tree),
            );
        } else {
            tree.insert(
                format!("{}_unknown_infinite", prefix),
                Value::Object(unknown_tree),
            );
        }
    }
}

fn insert_if_true(tree: &mut BTreeMap<String, Value>, key: &str, value: bool) {
    if value {
        tree.insert(key.to_owned(), Value::Boolean(true));
    }
}
