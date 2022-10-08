use std::{borrow::Cow, convert::TryFrom, fmt};

use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use diagnostic::{DiagnosticMessage, Label, Note, Urls};
use ordered_float::NotNan;
use regex::Regex;
use value::{Value, ValueRegex};

use crate::{
    expression::Resolved,
    state::{ExternalEnv, LocalEnv},
    vm::OpCode,
    Context, Expression, Span, TypeDef,
};

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    String(Bytes),
    Integer(i64),
    Float(NotNan<f64>),
    Boolean(bool),
    Regex(ValueRegex),
    Timestamp(DateTime<Utc>),
    Null,
}

impl Literal {
    /// Get a `Value` type stored in the literal.
    ///
    /// This differs from `Expression::as_value` insofar as this *always*
    /// returns a `Value`, whereas `as_value` returns `Option<Value>` which, in
    /// the case of `Literal` means it always returns `Some(Value)`, requiring
    /// an extra `unwrap()`.
    pub fn to_value(&self) -> Value {
        use Literal::*;

        match self {
            String(v) => Value::Bytes(v.clone()),
            Integer(v) => Value::Integer(*v),
            Float(v) => Value::Float(v.to_owned()),
            Boolean(v) => Value::Boolean(*v),
            Regex(v) => Value::Regex(v.clone()),
            Timestamp(v) => Value::Timestamp(v.to_owned()),
            Null => Value::Null,
        }
    }
}

impl Expression for Literal {
    fn resolve(&self, _: &mut Context) -> Resolved {
        Ok(self.to_value())
    }

    fn as_value(&self) -> Option<Value> {
        Some(self.to_value())
    }

    fn type_def(&self, _: (&LocalEnv, &ExternalEnv)) -> TypeDef {
        use Literal::*;

        let type_def = match self {
            String(_) => TypeDef::bytes(),
            Integer(_) => TypeDef::integer(),
            Float(_) => TypeDef::float(),
            Boolean(_) => TypeDef::boolean(),
            Regex(_) => TypeDef::regex(),
            Timestamp(_) => TypeDef::timestamp(),
            Null => TypeDef::null(),
        };

        type_def.infallible()
    }

    fn compile_to_vm(
        &self,
        vm: &mut crate::vm::Vm,
        _state: (&mut LocalEnv, &mut ExternalEnv),
    ) -> Result<(), String> {
        // Add the literal as a constant.
        let constant = vm.add_constant(self.to_value());
        vm.write_opcode(OpCode::Constant);
        vm.write_primitive(constant);
        Ok(())
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Literal::*;

        match self {
            String(v) => write!(f, r#""{}""#, std::string::String::from_utf8_lossy(v)),
            Integer(v) => v.fmt(f),
            Float(v) => v.fmt(f),
            Boolean(v) => v.fmt(f),
            Regex(v) => v.fmt(f),
            Timestamp(v) => write!(f, "t'{}'", v.to_rfc3339_opts(SecondsFormat::AutoSi, true)),
            Null => f.write_str("null"),
        }
    }
}

// Literal::String -------------------------------------------------------------

impl From<Bytes> for Literal {
    fn from(v: Bytes) -> Self {
        Literal::String(v)
    }
}

impl From<Cow<'_, str>> for Literal {
    fn from(v: Cow<'_, str>) -> Self {
        v.as_ref().into()
    }
}

impl From<Vec<u8>> for Literal {
    fn from(v: Vec<u8>) -> Self {
        v.as_slice().into()
    }
}

impl From<&[u8]> for Literal {
    fn from(v: &[u8]) -> Self {
        Literal::String(Bytes::copy_from_slice(v))
    }
}

impl From<String> for Literal {
    fn from(v: String) -> Self {
        Literal::String(v.into())
    }
}

impl From<&str> for Literal {
    fn from(v: &str) -> Self {
        Literal::String(Bytes::copy_from_slice(v.as_bytes()))
    }
}

// Literal::Integer ------------------------------------------------------------

impl From<i8> for Literal {
    fn from(v: i8) -> Self {
        Literal::Integer(v as i64)
    }
}

impl From<i16> for Literal {
    fn from(v: i16) -> Self {
        Literal::Integer(v as i64)
    }
}

impl From<i32> for Literal {
    fn from(v: i32) -> Self {
        Literal::Integer(v as i64)
    }
}

impl From<i64> for Literal {
    fn from(v: i64) -> Self {
        Literal::Integer(v)
    }
}

impl From<u16> for Literal {
    fn from(v: u16) -> Self {
        Literal::Integer(v as i64)
    }
}

impl From<u32> for Literal {
    fn from(v: u32) -> Self {
        Literal::Integer(v as i64)
    }
}

impl From<u64> for Literal {
    fn from(v: u64) -> Self {
        Literal::Integer(v as i64)
    }
}

impl From<usize> for Literal {
    fn from(v: usize) -> Self {
        Literal::Integer(v as i64)
    }
}

// Literal::Float --------------------------------------------------------------

impl From<NotNan<f64>> for Literal {
    fn from(v: NotNan<f64>) -> Self {
        Literal::Float(v)
    }
}

impl TryFrom<f64> for Literal {
    type Error = Error;

    fn try_from(v: f64) -> Result<Self, Self::Error> {
        Ok(Literal::Float(NotNan::new(v).map_err(|_| Error {
            span: Span::default(),
            variant: ErrorVariant::NanFloat,
        })?))
    }
}

// Literal::Boolean ------------------------------------------------------------

impl From<bool> for Literal {
    fn from(v: bool) -> Self {
        Literal::Boolean(v)
    }
}

// Literal::Regex --------------------------------------------------------------

impl From<Regex> for Literal {
    fn from(regex: Regex) -> Self {
        Literal::Regex(ValueRegex::new(regex))
    }
}

impl From<ValueRegex> for Literal {
    fn from(regex: ValueRegex) -> Self {
        Literal::Regex(regex)
    }
}

// Literal::Null ---------------------------------------------------------------

impl From<()> for Literal {
    fn from(_: ()) -> Self {
        Literal::Null
    }
}

impl<T: Into<Literal>> From<Option<T>> for Literal {
    fn from(literal: Option<T>) -> Self {
        match literal {
            None => Literal::Null,
            Some(v) => v.into(),
        }
    }
}

// Literal::Regex --------------------------------------------------------------

impl From<DateTime<Utc>> for Literal {
    fn from(dt: DateTime<Utc>) -> Self {
        Literal::Timestamp(dt)
    }
}

// -----------------------------------------------------------------------------

#[derive(Debug)]
pub struct Error {
    pub(crate) variant: ErrorVariant,
    span: Span,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum ErrorVariant {
    #[error("invalid regular expression")]
    InvalidRegex(#[from] regex::Error),

    #[error("invalid timestamp")]
    InvalidTimestamp(#[from] chrono::ParseError),

    #[error("float literal can't be NaN")]
    NanFloat,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#}", self.variant)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.variant)
    }
}

impl DiagnosticMessage for Error {
    fn code(&self) -> usize {
        use ErrorVariant::*;

        match &self.variant {
            InvalidRegex(..) => 101,
            InvalidTimestamp(..) => 601,
            NanFloat => 602,
        }
    }

    fn labels(&self) -> Vec<Label> {
        use ErrorVariant::*;

        match &self.variant {
            InvalidRegex(err) => {
                let error = err
                    .to_string()
                    .lines()
                    .filter_map(|line| {
                        if line.trim() == "^" || line == "regex parse error:" {
                            return None;
                        }

                        Some(line.trim_start_matches("error: ").trim())
                    })
                    .rev()
                    .collect::<Vec<_>>()
                    .join(": ");

                vec![Label::primary(
                    format!("regex parse error: {}", error),
                    self.span,
                )]
            }
            InvalidTimestamp(err) => vec![Label::primary(
                format!("invalid timestamp format: {}", err),
                self.span,
            )],

            NanFloat => vec![],
        }
    }

    fn notes(&self) -> Vec<Note> {
        use ErrorVariant::*;

        match &self.variant {
            InvalidRegex(_) => vec![Note::SeeDocs(
                "regular expressions".to_owned(),
                Urls::expression_docs_url("#regular-expression"),
            )],
            InvalidTimestamp(_) => vec![Note::SeeDocs(
                "timestamps".to_owned(),
                Urls::expression_docs_url("#timestamp"),
            )],
            NanFloat => vec![Note::SeeDocs(
                "floats".to_owned(),
                Urls::expression_docs_url("#float"),
            )],
        }
    }
}

impl From<(Span, regex::Error)> for Error {
    fn from((span, err): (Span, regex::Error)) -> Self {
        Self {
            variant: err.into(),
            span,
        }
    }
}

impl From<(Span, chrono::ParseError)> for Error {
    fn from((span, err): (Span, chrono::ParseError)) -> Self {
        Self {
            variant: err.into(),
            span,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{expr, test_type_def, TypeDef};

    test_type_def![
        bytes {
            expr: |_| expr!("foo"),
            want: TypeDef::bytes(),
        }

        integer {
            expr: |_| expr!(12),
            want: TypeDef::integer(),
        }
    ];
}
