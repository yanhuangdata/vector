use ::value::Value;
use vrl::prelude::*;

use crate::encode_key_value::EncodeKeyValueFn;

#[derive(Clone, Copy, Debug)]
pub struct EncodeLogfmt;

impl Function for EncodeLogfmt {
    fn identifier(&self) -> &'static str {
        "encode_logfmt"
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[
            Parameter {
                keyword: "value",
                kind: kind::OBJECT,
                required: true,
            },
            Parameter {
                keyword: "fields_ordering",
                kind: kind::ARRAY,
                required: false,
            },
        ]
    }

    fn compile(
        &self,
        _state: (&mut state::LocalEnv, &mut state::ExternalEnv),
        _ctx: &mut FunctionCompileContext,
        mut arguments: ArgumentList,
    ) -> Compiled {
        // The encode_logfmt function is just an alias for `encode_key_value` with the following
        // parameters for the delimiters.
        let key_value_delimiter = expr!("=");
        let field_delimiter = expr!(" ");
        let flatten_boolean = expr!(true);

        let value = arguments.required("value");
        let fields = arguments.optional("fields_ordering");

        Ok(Box::new(EncodeKeyValueFn {
            value,
            fields,
            key_value_delimiter,
            field_delimiter,
            flatten_boolean,
        }))
    }

    fn examples(&self) -> &'static [Example] {
        &[
            Example {
                title: "encode object",
                source: r#"encode_logfmt({"lvl": "info", "msg": "This is a message", "log_id": 12345})"#,
                result: Ok(r#"s'log_id=12345 lvl=info msg="This is a message"'"#),
            },
            Example {
                title: "encode object with fields ordering",
                source: r#"encode_logfmt!({"msg": "This is a message", "lvl": "info", "log_id": 12345}, ["lvl", "msg"])"#,
                result: Ok(r#"s'lvl=info msg="This is a message" log_id=12345'"#),
            },
        ]
    }

    fn call_by_vm(&self, _ctx: &mut Context, args: &mut VmArgumentList) -> Resolved {
        let value = args.required("value");
        let fields = args.optional("fields_ordering");

        let key_value_delimiter = Value::from("=");
        let field_delimiter = Value::from(" ");
        let flatten_boolean = Value::from(true);

        super::encode_key_value::encode_key_value(
            fields,
            value,
            key_value_delimiter,
            field_delimiter,
            flatten_boolean,
        )
    }
}
