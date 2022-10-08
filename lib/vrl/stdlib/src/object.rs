use ::value::Value;
use vrl::prelude::*;

fn object(value: Value) -> Resolved {
    match value {
        v @ Value::Object(_) => Ok(v),
        v => Err(format!("expected object, got {}", v.kind()).into()),
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Object;

impl Function for Object {
    fn identifier(&self) -> &'static str {
        "object"
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[Parameter {
            keyword: "value",
            kind: kind::ANY,
            required: true,
        }]
    }

    fn examples(&self) -> &'static [Example] {
        &[
            Example {
                title: "valid",
                source: r#"object({"foo": "bar"})"#,
                result: Ok(r#"{"foo": "bar"}"#),
            },
            Example {
                title: "invalid",
                source: "object!(true)",
                result: Err(
                    r#"function call error for "object" at (0:13): expected object, got boolean"#,
                ),
            },
        ]
    }

    fn compile(
        &self,
        _state: (&mut state::LocalEnv, &mut state::ExternalEnv),
        _ctx: &mut FunctionCompileContext,
        mut arguments: ArgumentList,
    ) -> Compiled {
        let value = arguments.required("value");

        Ok(Box::new(ObjectFn { value }))
    }

    fn call_by_vm(&self, _ctx: &mut Context, args: &mut VmArgumentList) -> Resolved {
        let value = args.required("value");
        object(value)
    }
}

#[derive(Debug, Clone)]
struct ObjectFn {
    value: Box<dyn Expression>,
}

impl Expression for ObjectFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        object(self.value.resolve(ctx)?)
    }

    fn type_def(&self, state: (&state::LocalEnv, &state::ExternalEnv)) -> TypeDef {
        self.value
            .type_def(state)
            .fallible_unless(Kind::object(Collection::any()))
            .restrict_object()
    }
}
