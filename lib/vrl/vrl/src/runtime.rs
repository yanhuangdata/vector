use std::{error::Error, fmt, sync::Arc};

use compiler::{
    state::{ExternalEnv, LocalEnv},
    vm::{OpCode, Vm},
    ExpressionError, Function,
};
use lookup::LookupBuf;
use value::Value;
use vector_common::TimeZone;

use crate::{state, Context, Program, Target};

pub type RuntimeResult = Result<Value, Terminate>;

#[derive(Debug, Default)]
pub struct Runtime {
    state: state::Runtime,
    root_lookup: LookupBuf,
}

/// The error raised if the runtime is terminated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Terminate {
    /// A manual `abort` call.
    ///
    /// This is an intentional termination that does not result in an
    /// `Ok(Value)` result, but should neither be interpreted as an unexpected
    /// outcome.
    Abort(ExpressionError),

    /// An unexpected program termination.
    Error(ExpressionError),
}

impl fmt::Display for Terminate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Terminate::Abort(error) => error.fmt(f),
            Terminate::Error(error) => error.fmt(f),
        }
    }
}

impl Error for Terminate {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

impl Runtime {
    pub fn new(state: state::Runtime) -> Self {
        Self {
            state,

            // `LookupBuf` uses a `VecDeque` internally, which always allocates, even
            // when it's empty (for `LookupBuf::root()`), so we do the
            // allocation on initialization of the runtime, instead of on every
            // `resolve` run.
            root_lookup: LookupBuf::root(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.state.is_empty()
    }

    pub fn clear(&mut self) {
        self.state.clear();
    }

    /// Given the provided [`Target`], resolve the provided [`Program`] to
    /// completion.
    pub fn resolve(
        &mut self,
        target: &mut dyn Target,
        program: &Program,
        timezone: &TimeZone,
    ) -> RuntimeResult {
        // Validate that the path is an object.
        //
        // VRL technically supports any `Value` object as the root, but the
        // assumption is people are expected to use it to query objects.
        match target.target_get(&self.root_lookup) {
            Ok(Some(&Value::Object(_))) => {}
            Ok(Some(value)) => {
                return Err(Terminate::Error(
                    format!(
                        "target must be a valid object, got {}: {}",
                        value.kind(),
                        value
                    )
                    .into(),
                ))
            }
            Ok(None) => {
                return Err(Terminate::Error(
                    "expected target object, got nothing".to_owned().into(),
                ))
            }
            Err(err) => {
                return Err(Terminate::Error(
                    format!("error querying target object: {}", err).into(),
                ))
            }
        };

        let mut ctx = Context::new(target, &mut self.state, timezone);

        program.resolve(&mut ctx).map_err(|err| match err {
            #[cfg(feature = "expr-abort")]
            ExpressionError::Abort { .. } => Terminate::Abort(err),
            err @ ExpressionError::Error { .. } => Terminate::Error(err),
        })
    }

    pub fn compile(
        &self,
        fns: Vec<Box<dyn Function>>,
        program: &Program,
        external: &mut ExternalEnv,
    ) -> Result<Vm, String> {
        let mut local = LocalEnv::default();
        let mut vm = Vm::new(Arc::new(fns));

        program.compile_to_vm(&mut vm, (&mut local, external))?;

        vm.write_opcode(OpCode::Return);

        Ok(vm)
    }

    /// Given the provided [`Target`], runs the [`Vm`] to completion.
    pub fn run_vm(
        &mut self,
        vm: &Vm,
        target: &mut dyn Target,
        timezone: &TimeZone,
    ) -> Result<Value, Terminate> {
        let mut context = Context::new(target, &mut self.state, timezone);
        vm.interpret(&mut context).map_err(|err| match err {
            #[cfg(feature = "expr-abort")]
            ExpressionError::Abort { .. } => Terminate::Abort(err),
            err @ ExpressionError::Error { .. } => Terminate::Error(err),
        })
    }
}
