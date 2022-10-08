use ::value::Value;
use lookup_lib::{LookupBuf, SegmentBuf};
use vrl::prelude::*;

fn get(value: Value, path: Value) -> Resolved {
    let path = match path {
        Value::Array(path) => {
            let mut get = LookupBuf::root();

            for segment in path {
                let segment = match segment {
                    Value::Bytes(field) => {
                        SegmentBuf::Field(String::from_utf8_lossy(&field).into_owned().into())
                    }
                    Value::Integer(index) => SegmentBuf::Index(index as isize),
                    value => {
                        return Err(format!(
                            r#"path segment must be either string or integer, not {}"#,
                            value.kind()
                        )
                        .into())
                    }
                };

                get.push_back(segment)
            }

            get
        }
        value => {
            return Err(value::Error::Expected {
                got: value.kind(),
                expected: Kind::array(Collection::any()),
            }
            .into())
        }
    };
    Ok(value.target_get(&path)?.cloned().unwrap_or(Value::Null))
}

#[derive(Clone, Copy, Debug)]
pub struct Get;

impl Function for Get {
    fn identifier(&self) -> &'static str {
        "get"
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[
            Parameter {
                keyword: "value",
                kind: kind::OBJECT | kind::ARRAY,
                required: true,
            },
            Parameter {
                keyword: "path",
                kind: kind::ARRAY,
                required: true,
            },
        ]
    }

    fn examples(&self) -> &'static [Example] {
        &[
            Example {
                title: "returns existing field",
                source: r#"get!(value: {"foo": "bar"}, path: ["foo"])"#,
                result: Ok(r#""bar""#),
            },
            Example {
                title: "returns null for unknown field",
                source: r#"get!(value: {"foo": "bar"}, path: ["baz"])"#,
                result: Ok("null"),
            },
            Example {
                title: "nested path",
                source: r#"get!(value: {"foo": { "bar": true }}, path: ["foo", "bar"])"#,
                result: Ok(r#"true"#),
            },
            Example {
                title: "indexing",
                source: r#"get!(value: [92, 42], path: [0])"#,
                result: Ok("92"),
            },
            Example {
                title: "nested indexing",
                source: r#"get!(value: {"foo": { "bar": [92, 42] }}, path: ["foo", "bar", 1])"#,
                result: Ok("42"),
            },
            Example {
                title: "external target",
                source: indoc! {r#"
                    . = { "foo": true }
                    get!(value: ., path: ["foo"])
                "#},
                result: Ok("true"),
            },
            Example {
                title: "variable",
                source: indoc! {r#"
                    var = { "foo": true }
                    get!(value: var, path: ["foo"])
                "#},
                result: Ok("true"),
            },
            Example {
                title: "missing index",
                source: r#"get!(value: {"foo": { "bar": [92, 42] }}, path: ["foo", "bar", 1, -1])"#,
                result: Ok("null"),
            },
            Example {
                title: "invalid indexing",
                source: r#"get!(value: [42], path: ["foo"])"#,
                result: Ok("null"),
            },
            Example {
                title: "invalid segment type",
                source: r#"get!(value: {"foo": { "bar": [92, 42] }}, path: ["foo", true])"#,
                result: Err(
                    r#"function call error for "get" at (0:62): path segment must be either string or integer, not boolean"#,
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
        let path = arguments.required("path");

        Ok(Box::new(GetFn { value, path }))
    }

    fn call_by_vm(&self, _ctx: &mut Context, args: &mut VmArgumentList) -> Resolved {
        let value = args.required("value");
        let path = args.required("path");

        get(value, path)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct GetFn {
    value: Box<dyn Expression>,
    path: Box<dyn Expression>,
}

impl Expression for GetFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let path = self.path.resolve(ctx)?;
        let value = self.value.resolve(ctx)?;

        get(value, path)
    }

    fn type_def(&self, _: (&state::LocalEnv, &state::ExternalEnv)) -> TypeDef {
        TypeDef::any().fallible()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    test_function![
        get => Get;

        any {
            args: func_args![value: value!([42]), path: value!([0])],
            want: Ok(42),
            tdef: TypeDef::any().fallible(),
        }
    ];
}
