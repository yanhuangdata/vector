use ::value::{Value, ValueRegex};
use vrl::prelude::*;

fn find(value: Value, pattern: Value, from: Option<Value>) -> Resolved {
    let from = match from {
        Some(value) => value.try_integer()?,
        None => 0,
    } as usize;

    Ok(FindFn::find(value, pattern, from)?
        .map(|value| Value::Integer(value as i64))
        .unwrap_or_else(|| Value::Integer(-1)))
}

#[derive(Clone, Copy, Debug)]
pub struct Find;

impl Function for Find {
    fn identifier(&self) -> &'static str {
        "find"
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
                kind: kind::BYTES | kind::REGEX,
                required: true,
            },
            Parameter {
                keyword: "from",
                kind: kind::INTEGER,
                required: false,
            },
        ]
    }

    fn examples(&self) -> &'static [Example] {
        &[Example {
            title: "string",
            source: r#"find("foobar", "bar")"#,
            result: Ok("3"),
        }]
    }

    fn compile(
        &self,
        _state: (&mut state::LocalEnv, &mut state::ExternalEnv),
        _ctx: &mut FunctionCompileContext,
        mut arguments: ArgumentList,
    ) -> Compiled {
        let value = arguments.required("value");
        let pattern = arguments.required("pattern");
        let from = arguments.optional("from");

        Ok(Box::new(FindFn {
            value,
            pattern,
            from,
        }))
    }

    fn call_by_vm(&self, _ctx: &mut Context, args: &mut VmArgumentList) -> Resolved {
        let value = args.required("value");
        let pattern = args.required("pattern");
        let from = args.optional("from");

        find(value, pattern, from)
    }
}

#[derive(Debug, Clone)]
struct FindFn {
    value: Box<dyn Expression>,
    pattern: Box<dyn Expression>,
    from: Option<Box<dyn Expression>>,
}

impl FindFn {
    fn find_regex_in_str(value: &str, regex: ValueRegex, offset: usize) -> Option<usize> {
        regex.find_at(value, offset).map(|found| found.start())
    }

    fn find_bytes_in_bytes(value: Bytes, pattern: Bytes, offset: usize) -> Option<usize> {
        if pattern.len() > value.len() {
            return None;
        }
        for from in offset..(value.len() - pattern.len() + 1) {
            let to = from + pattern.len();
            if value[from..to] == pattern {
                return Some(from);
            }
        }
        None
    }

    fn find(value: Value, pattern: Value, offset: usize) -> Result<Option<usize>> {
        match pattern {
            Value::Bytes(bytes) => Ok(Self::find_bytes_in_bytes(value.try_bytes()?, bytes, offset)),
            Value::Regex(regex) => Ok(Self::find_regex_in_str(
                &value.try_bytes_utf8_lossy()?,
                regex,
                offset,
            )),
            other => Err(value::Error::Expected {
                got: other.kind(),
                expected: Kind::bytes() | Kind::regex(),
            }
            .into()),
        }
    }
}

impl Expression for FindFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let value = self.value.resolve(ctx)?;
        let pattern = self.pattern.resolve(ctx)?;
        let from = self
            .from
            .as_ref()
            .map(|expr| expr.resolve(ctx))
            .transpose()?;

        find(value, pattern, from)
    }

    fn type_def(&self, _: (&state::LocalEnv, &state::ExternalEnv)) -> TypeDef {
        TypeDef::integer().infallible()
    }
}

#[cfg(test)]
mod tests {
    use regex::Regex;

    use super::*;

    test_function![
        find => Find;

        str_matching_end {
            args: func_args![value: "foobar", pattern: "bar"],
            want: Ok(value!(3)),
            tdef: TypeDef::integer().infallible(),
        }

        str_matching_beginning {
            args: func_args![value: "foobar", pattern: "foo"],
            want: Ok(value!(0)),
            tdef: TypeDef::integer().infallible(),
        }

        str_matching_middle {
            args: func_args![value: "foobar", pattern: "ob"],
            want: Ok(value!(2)),
            tdef: TypeDef::integer().infallible(),
        }

        str_too_long {
            args: func_args![value: "foo", pattern: "foobar"],
            want: Ok(value!(-1)),
            tdef: TypeDef::integer().infallible(),
        }

        regex_matching_end {
            args: func_args![value: "foobar", pattern: Value::Regex(Regex::new("bar").unwrap().into())],
            want: Ok(value!(3)),
            tdef: TypeDef::integer().infallible(),
        }

        regex_matching_start {
            args: func_args![value: "foobar", pattern: Value::Regex(Regex::new("fo+z?").unwrap().into())],
            want: Ok(value!(0)),
            tdef: TypeDef::integer().infallible(),
        }

        wrong_pattern {
            args: func_args![value: "foobar", pattern: Value::Integer(42)],
            want: Err("expected regex or string, got integer"),
            tdef: TypeDef::integer().infallible(),
        }
    ];
}
