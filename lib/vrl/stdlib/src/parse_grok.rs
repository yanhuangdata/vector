use std::{collections::BTreeMap, fmt, sync::Arc};

use ::value::Value;
use vrl::{
    diagnostic::{Label, Span},
    prelude::*,
};

fn parse_grok(value: Value, remove_empty: Value, pattern: Arc<grok::Pattern>) -> Resolved {
    let bytes = value.try_bytes_utf8_lossy()?;
    let remove_empty = remove_empty.try_boolean()?;
    match pattern.match_against(&bytes) {
        Some(matches) => {
            let mut result = BTreeMap::new();

            for (name, value) in matches.iter() {
                if !remove_empty || !value.is_empty() {
                    result.insert(name.to_string(), Value::from(value));
                }
            }

            Ok(Value::from(result))
        }
        None => Err("unable to parse input with grok pattern".into()),
    }
}

#[derive(Debug)]
pub(crate) enum Error {
    InvalidGrokPattern(grok::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::InvalidGrokPattern(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

impl DiagnosticMessage for Error {
    fn code(&self) -> usize {
        109
    }

    fn labels(&self) -> Vec<Label> {
        match self {
            Error::InvalidGrokPattern(err) => {
                vec![Label::primary(
                    format!("grok pattern error: {}", err),
                    Span::default(),
                )]
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ParseGrok;

impl Function for ParseGrok {
    fn identifier(&self) -> &'static str {
        "parse_grok"
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
                kind: kind::BYTES,
                required: true,
            },
            Parameter {
                keyword: "remove_empty",
                kind: kind::BOOLEAN,
                required: false,
            },
        ]
    }

    fn examples(&self) -> &'static [Example] {
        &[Example {
            title: "parse grok pattern",
            source: indoc! {r#"
                value = "2020-10-02T23:22:12.223222Z info Hello world"
                pattern = "%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} %{GREEDYDATA:message}"

                parse_grok!(value, pattern)
            "#},
            result: Ok(indoc! {r#"
                {
                    "timestamp": "2020-10-02T23:22:12.223222Z",
                    "level": "info",
                    "message": "Hello world"
                }
            "#}),
        }]
    }

    fn compile(
        &self,
        _state: (&mut state::LocalEnv, &mut state::ExternalEnv),
        _ctx: &mut FunctionCompileContext,
        mut arguments: ArgumentList,
    ) -> Compiled {
        let value = arguments.required("value");

        let pattern = arguments
            .required_literal("pattern")?
            .to_value()
            .try_bytes_utf8_lossy()
            .expect("grok pattern not bytes")
            .into_owned();

        let mut grok = grok::Grok::with_patterns();
        let pattern =
            Arc::new(grok.compile(&pattern, true).map_err(|e| {
                Box::new(Error::InvalidGrokPattern(e)) as Box<dyn DiagnosticMessage>
            })?);

        let remove_empty = arguments
            .optional("remove_empty")
            .unwrap_or_else(|| expr!(false));

        Ok(Box::new(ParseGrokFn {
            value,
            pattern,
            remove_empty,
        }))
    }

    fn compile_argument(
        &self,
        _args: &[(&'static str, Option<FunctionArgument>)],
        _ctx: &mut FunctionCompileContext,
        name: &str,
        expr: Option<&expression::Expr>,
    ) -> CompiledArgument {
        match (name, expr) {
            ("pattern", Some(expr)) => {
                let pattern = expr
                    .as_literal("pattern")?
                    .try_bytes_utf8_lossy()
                    .expect("grok pattern not bytes")
                    .into_owned();

                let mut grok = grok::Grok::with_patterns();
                let pattern = Arc::new(grok.compile(&pattern, true).map_err(|e| {
                    Box::new(Error::InvalidGrokPattern(e)) as Box<dyn DiagnosticMessage>
                })?);

                Ok(Some(Box::new(pattern) as _))
            }
            _ => Ok(None),
        }
    }

    fn call_by_vm(&self, _ctx: &mut Context, args: &mut VmArgumentList) -> Resolved {
        let value = args.required("value");
        let remove_empty = args
            .optional("remove_empty")
            .unwrap_or(Value::Boolean(false));
        let pattern = args
            .required_any("pattern")
            .downcast_ref::<Arc<grok::Pattern>>()
            .unwrap()
            .clone();

        parse_grok(value, remove_empty, pattern)
    }
}

#[derive(Clone, Debug)]
struct ParseGrokFn {
    value: Box<dyn Expression>,

    // Wrapping pattern in an Arc, as cloning the pattern could otherwise be expensive.
    pattern: Arc<grok::Pattern>,
    remove_empty: Box<dyn Expression>,
}

impl Expression for ParseGrokFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let value = self.value.resolve(ctx)?;
        let remove_empty = self.remove_empty.resolve(ctx)?;
        let pattern = self.pattern.clone();

        parse_grok(value, remove_empty, pattern)
    }

    fn type_def(&self, _: (&state::LocalEnv, &state::ExternalEnv)) -> TypeDef {
        TypeDef::object(Collection::any()).fallible()
    }
}

#[cfg(test)]
mod test {
    use vector_common::btreemap;

    use super::*;

    test_function![
        parse_grok => ParseGrok;

        invalid_grok {
            args: func_args![ value: "foo",
                              pattern: "%{NOG}"],
            want: Err("The given pattern definition name \"NOG\" could not be found in the definition map"),
            tdef: TypeDef::object(Collection::any()).fallible(),
        }

        error {
            args: func_args![ value: "an ungrokkable message",
                              pattern: "%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} %{GREEDYDATA:message}"],
            want: Err("unable to parse input with grok pattern"),
            tdef: TypeDef::object(Collection::any()).fallible(),
        }

        error2 {
            args: func_args![ value: "2020-10-02T23:22:12.223222Z an ungrokkable message",
                              pattern: "%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} %{GREEDYDATA:message}"],
            want: Err("unable to parse input with grok pattern"),
            tdef: TypeDef::object(Collection::any()).fallible(),
        }

        parsed {
            args: func_args![ value: "2020-10-02T23:22:12.223222Z info Hello world",
                              pattern: "%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} %{GREEDYDATA:message}"],
            want: Ok(Value::from(btreemap! {
                "timestamp" => "2020-10-02T23:22:12.223222Z",
                "level" => "info",
                "message" => "Hello world",
            })),
            tdef: TypeDef::object(Collection::any()).fallible(),
        }

        parsed2 {
            args: func_args![ value: "2020-10-02T23:22:12.223222Z",
                              pattern: "(%{TIMESTAMP_ISO8601:timestamp}|%{LOGLEVEL:level})"],
            want: Ok(Value::from(btreemap! {
                "timestamp" => "2020-10-02T23:22:12.223222Z",
                "level" => "",
            })),
            tdef: TypeDef::object(Collection::any()).fallible(),
        }

        remove_empty {
            args: func_args![ value: "2020-10-02T23:22:12.223222Z",
                              pattern: "(%{TIMESTAMP_ISO8601:timestamp}|%{LOGLEVEL:level})",
                              remove_empty: true,
            ],
            want: Ok(Value::from(
                btreemap! { "timestamp" => "2020-10-02T23:22:12.223222Z" },
            )),
            tdef: TypeDef::object(Collection::any()).fallible(),
        }
    ];
}
