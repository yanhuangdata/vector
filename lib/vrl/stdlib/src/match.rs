use ::value::Value;
use vrl::prelude::*;

fn match_(value: Value, pattern: Value) -> Resolved {
    let string = value.try_bytes_utf8_lossy()?;
    let pattern = pattern.try_regex()?;
    Ok(pattern.is_match(&string).into())
}

#[derive(Clone, Copy, Debug)]
pub struct Match;

impl Function for Match {
    fn identifier(&self) -> &'static str {
        "match"
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[
            Parameter {
                keyword: "value",
                kind: kind::BYTES,
                required: true,
            },
            Parameter {
                keyword: "pattern",
                kind: kind::REGEX,
                required: true,
            },
        ]
    }

    fn examples(&self) -> &'static [Example] {
        &[
            Example {
                title: "match",
                source: r#"match("foobar", r'foo')"#,
                result: Ok("true"),
            },
            Example {
                title: "mismatch",
                source: r#"match("bazqux", r'foo')"#,
                result: Ok("false"),
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
        let pattern = arguments.required("pattern");

        Ok(Box::new(MatchFn { value, pattern }))
    }

    fn call_by_vm(&self, _ctx: &mut Context, args: &mut VmArgumentList) -> Resolved {
        let value = args.required("value");
        let pattern = args.required("pattern");

        match_(value, pattern)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MatchFn {
    value: Box<dyn Expression>,
    pattern: Box<dyn Expression>,
}

impl Expression for MatchFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let value = self.value.resolve(ctx)?;
        let pattern = self.pattern.resolve(ctx)?;

        match_(value, pattern)
    }

    fn type_def(&self, _: (&state::LocalEnv, &state::ExternalEnv)) -> TypeDef {
        TypeDef::boolean().infallible()
    }
}

#[cfg(test)]
#[allow(clippy::trivial_regex)]
mod tests {
    use regex::Regex;

    use super::*;

    test_function![
        r#match => Match;

        yes {
            args: func_args![value: "foobar",
                             pattern: Value::Regex(Regex::new("\\s\\w+").unwrap().into())],
            want: Ok(value!(false)),
            tdef: TypeDef::boolean().infallible(),
        }

        no {
            args: func_args![value: "foo 2 bar",
                             pattern: Value::Regex(Regex::new("foo \\d bar").unwrap().into())],
            want: Ok(value!(true)),
            tdef: TypeDef::boolean().infallible(),
        }
    ];
}
