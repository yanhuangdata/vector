use ::value::Value;
use vrl::prelude::*;

fn boolean(value: Value) -> Resolved {
    match value {
        v @ Value::Boolean(_) => Ok(v),
        v => Err(format!("expected boolean, got {}", v.kind()).into()),
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Boolean;

impl Function for Boolean {
    fn identifier(&self) -> &'static str {
        "bool"
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
                source: r#"bool(false)"#,
                result: Ok("false"),
            },
            Example {
                title: "invalid",
                source: "bool!(42)",
                result: Err(
                    r#"function call error for "bool" at (0:9): expected boolean, got integer"#,
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

        Ok(Box::new(BooleanFn { value }))
    }

    fn call_by_vm(&self, _ctx: &mut Context, args: &mut VmArgumentList) -> Resolved {
        let value = args.required("value");
        boolean(value)
    }
}

#[derive(Debug, Clone)]
struct BooleanFn {
    value: Box<dyn Expression>,
}

impl Expression for BooleanFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        boolean(self.value.resolve(ctx)?)
    }

    fn type_def(&self, state: (&state::LocalEnv, &state::ExternalEnv)) -> TypeDef {
        let non_boolean = !self.value.type_def(state).is_boolean();

        TypeDef::boolean().with_fallibility(non_boolean)
    }
}
