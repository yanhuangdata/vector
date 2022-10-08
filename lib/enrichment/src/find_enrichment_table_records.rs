use std::collections::BTreeMap;

use ::value::Value;
use vrl::prelude::*;

use crate::{
    vrl_util::{self, add_index, evaluate_condition, index_from_args, EnrichmentTableRecord},
    Case, Condition, IndexHandle, TableRegistry, TableSearch,
};

fn find_enrichment_table_records(
    select: Option<Value>,
    enrichment_tables: &TableSearch,
    table: &str,
    case_sensitive: Case,
    condition: &[Condition],
    index: Option<IndexHandle>,
) -> Resolved {
    let select = select
        .map(|select| match select {
            Value::Array(arr) => arr
                .iter()
                .map(|value| Ok(value.try_bytes_utf8_lossy()?.to_string()))
                .collect::<std::result::Result<Vec<_>, _>>(),
            value => Err(value::Error::Expected {
                got: value.kind(),
                expected: Kind::array(Collection::any()),
            }),
        })
        .transpose()?;

    let data = enrichment_tables
        .find_table_rows(
            table,
            case_sensitive,
            condition,
            select.as_ref().map(|select| select.as_ref()),
            index,
        )?
        .into_iter()
        .map(Value::Object)
        .collect();
    Ok(Value::Array(data))
}

#[derive(Clone, Copy, Debug)]
pub struct FindEnrichmentTableRecords;
impl Function for FindEnrichmentTableRecords {
    fn identifier(&self) -> &'static str {
        "find_enrichment_table_records"
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[
            Parameter {
                keyword: "table",
                kind: kind::BYTES,
                required: true,
            },
            Parameter {
                keyword: "condition",
                kind: kind::OBJECT,
                required: true,
            },
            Parameter {
                keyword: "select",
                kind: kind::ARRAY,
                required: false,
            },
            Parameter {
                keyword: "case_sensitive",
                kind: kind::BOOLEAN,
                required: false,
            },
        ]
    }

    fn examples(&self) -> &'static [Example] {
        &[Example {
            title: "find records",
            source: r#"find_enrichment_table_records!("test", {"surname": "Smith"})"#,
            result: Ok(
                indoc! { r#"[{"id": 1, "firstname": "Bob", "surname": "Smith"},
                             {"id": 2, "firstname": "Fred", "surname": "Smith"}]"#,
                },
            ),
        }]
    }

    fn compile(
        &self,
        _state: (&mut state::LocalEnv, &mut state::ExternalEnv),
        ctx: &mut FunctionCompileContext,
        mut arguments: ArgumentList,
    ) -> Compiled {
        let registry = ctx
            .get_external_context_mut::<TableRegistry>()
            .ok_or(Box::new(vrl_util::Error::TablesNotLoaded) as Box<dyn DiagnosticMessage>)?;

        let tables = registry
            .table_ids()
            .into_iter()
            .map(Value::from)
            .collect::<Vec<_>>();

        let table = arguments
            .required_enum("table", &tables)?
            .try_bytes_utf8_lossy()
            .expect("table is not valid utf8")
            .into_owned();
        let condition = arguments.required_object("condition")?;

        let select = arguments.optional("select");

        let case_sensitive = arguments
            .optional_literal("case_sensitive")?
            .and_then(|literal| literal.as_value())
            .map(|value| value.try_boolean())
            .transpose()
            .expect("case_sensitive should be boolean") // This will have been caught by the type checker.
            .map(|case_sensitive| {
                if case_sensitive {
                    Case::Sensitive
                } else {
                    Case::Insensitive
                }
            })
            .unwrap_or(Case::Sensitive);

        let index = Some(
            add_index(registry, &table, case_sensitive, &condition)
                .map_err(|err| Box::new(err) as Box<_>)?,
        );

        Ok(Box::new(FindEnrichmentTableRecordsFn {
            table,
            condition,
            index,
            select,
            case_sensitive,
            enrichment_tables: registry.as_readonly(),
        }))
    }

    fn compile_argument(
        &self,
        args: &[(&'static str, Option<FunctionArgument>)],
        ctx: &mut FunctionCompileContext,
        name: &str,
        expr: Option<&expression::Expr>,
    ) -> CompiledArgument {
        match (name, expr) {
            ("table", Some(expr)) => {
                let registry =
                    ctx.get_external_context_mut::<TableRegistry>()
                        .ok_or(Box::new(vrl_util::Error::TablesNotLoaded)
                            as Box<dyn DiagnosticMessage>)?;

                let tables = registry
                    .table_ids()
                    .into_iter()
                    .map(Value::from)
                    .collect::<Vec<_>>();

                let table = expr
                    .as_enum("table", tables)?
                    .try_bytes_utf8_lossy()
                    .expect("table is not valid utf8")
                    .into_owned();

                let record = index_from_args(table, registry, args)?;

                Ok(Some(Box::new(record) as _))
            }
            _ => Ok(None),
        }
    }

    fn call_by_vm(&self, _ctx: &mut Context, args: &mut VmArgumentList) -> Resolved {
        let condition = args.required("condition");
        let condition = condition
            .into_object()
            .expect("condition should be an object");
        let condition = condition
            .iter()
            .map(|(key, value)| evaluate_condition(key, value.clone()))
            .collect::<Result<Vec<Condition>>>()?;

        let record = args
            .required_any("table")
            .downcast_ref::<EnrichmentTableRecord>()
            .unwrap();
        let select = args.optional("select");

        find_enrichment_table_records(
            select,
            &record.enrichment_tables,
            &record.table,
            record.case_sensitive,
            &condition,
            record.index,
        )
    }
}

#[derive(Debug, Clone)]
pub struct FindEnrichmentTableRecordsFn {
    table: String,
    condition: BTreeMap<String, expression::Expr>,
    index: Option<IndexHandle>,
    select: Option<Box<dyn Expression>>,
    case_sensitive: Case,
    enrichment_tables: TableSearch,
}

impl Expression for FindEnrichmentTableRecordsFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let condition = self
            .condition
            .iter()
            .map(|(key, value)| {
                let value = value.resolve(ctx)?;
                evaluate_condition(key, value)
            })
            .collect::<Result<Vec<Condition>>>()?;

        let select = self
            .select
            .as_ref()
            .map(|array| array.resolve(ctx))
            .transpose()?;

        let table = &self.table;
        let case_sensitive = self.case_sensitive;
        let index = self.index;
        let enrichment_tables = &self.enrichment_tables;

        find_enrichment_table_records(
            select,
            enrichment_tables,
            table,
            case_sensitive,
            &condition,
            index,
        )
    }

    fn type_def(&self, _: (&state::LocalEnv, &state::ExternalEnv)) -> TypeDef {
        TypeDef::array(Collection::from_unknown(Kind::object(Collection::any()))).fallible()
    }
}

#[cfg(test)]
mod tests {
    use vector_common::{btreemap, TimeZone};

    use super::*;
    use crate::test_util::get_table_registry;

    #[test]
    fn find_table_row() {
        let registry = get_table_registry();
        let func = FindEnrichmentTableRecordsFn {
            table: "dummy1".to_string(),
            condition: btreemap! {
                "field" =>  expression::Literal::from("value"),
            },
            index: Some(IndexHandle(999)),
            select: None,
            case_sensitive: Case::Sensitive,
            enrichment_tables: registry.as_readonly(),
        };

        let tz = TimeZone::default();
        let mut object: Value = BTreeMap::new().into();
        let mut runtime_state = vrl::state::Runtime::default();
        let mut ctx = Context::new(&mut object, &mut runtime_state, &tz);

        registry.finish_load();

        let got = func.resolve(&mut ctx);

        assert_eq!(Ok(value![vec![value!({ "field": "result" })]]), got);
    }
}
