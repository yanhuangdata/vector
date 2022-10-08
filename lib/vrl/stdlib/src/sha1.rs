use ::sha1::Digest;
use ::value::Value;
use vrl::prelude::*;

fn sha1(value: Value) -> Resolved {
    let value = value.try_bytes()?;
    Ok(hex::encode(sha1::Sha1::digest(&value)).into())
}

#[derive(Clone, Copy, Debug)]
pub struct Sha1;

impl Function for Sha1 {
    fn identifier(&self) -> &'static str {
        "sha1"
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[Parameter {
            keyword: "value",
            kind: kind::BYTES,
            required: true,
        }]
    }

    fn examples(&self) -> &'static [Example] {
        &[Example {
            title: "sha1",
            source: r#"sha1("foobar")"#,
            result: Ok("8843d7f92416211de9ebb963ff4ce28125932878"),
        }]
    }

    fn compile(
        &self,
        _state: (&mut state::LocalEnv, &mut state::ExternalEnv),
        _ctx: &mut FunctionCompileContext,
        mut arguments: ArgumentList,
    ) -> Compiled {
        let value = arguments.required("value");

        Ok(Box::new(Sha1Fn { value }))
    }

    fn call_by_vm(&self, _ctx: &mut Context, args: &mut VmArgumentList) -> Resolved {
        let value = args.required("value");
        sha1(value)
    }
}

#[derive(Debug, Clone)]
struct Sha1Fn {
    value: Box<dyn Expression>,
}

impl Expression for Sha1Fn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let value = self.value.resolve(ctx)?;
        sha1(value)
    }

    fn type_def(&self, _: (&state::LocalEnv, &state::ExternalEnv)) -> TypeDef {
        TypeDef::bytes().infallible()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    test_function![
        sha1 => Sha1;

        sha {
             args: func_args![value: "foo"],
             want: Ok("0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33"),
             tdef: TypeDef::bytes().infallible(),
         }
    ];
}
