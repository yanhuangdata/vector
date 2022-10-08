use std::fmt;

use crate::{
    expression::{Not, Resolved},
    state::{ExternalEnv, LocalEnv},
    vm::Vm,
    Context, Expression, TypeDef,
};

#[derive(Debug, Clone, PartialEq)]
pub struct Unary {
    variant: Variant,
}

impl Unary {
    pub fn new(variant: Variant) -> Self {
        Self { variant }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Variant {
    Not(Not),
}

impl Expression for Unary {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        use Variant::*;

        match &self.variant {
            Not(v) => v.resolve(ctx),
        }
    }

    fn type_def(&self, state: (&LocalEnv, &ExternalEnv)) -> TypeDef {
        use Variant::*;

        match &self.variant {
            Not(v) => v.type_def(state),
        }
    }

    fn compile_to_vm(
        &self,
        vm: &mut Vm,
        state: (&mut LocalEnv, &mut ExternalEnv),
    ) -> std::result::Result<(), String> {
        match &self.variant {
            Variant::Not(v) => {
                v.compile_to_vm(vm, state)?;
            }
        }

        Ok(())
    }
}

impl fmt::Display for Unary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Variant::*;

        match &self.variant {
            Not(v) => v.fmt(f),
        }
    }
}

impl From<Not> for Variant {
    fn from(not: Not) -> Self {
        Variant::Not(not)
    }
}
