use std::sync::Arc;

use serde::{Deserialize, Serialize};
use value::Value;
use vector_common::TimeZone;
use vrl::{diagnostic::Formatter, Program, Runtime, Vm, VrlRuntime};

use crate::{
    conditions::{Condition, ConditionConfig, ConditionDescription, Conditional},
    emit,
    event::{Event, VrlTarget},
    internal_events::VrlConditionExecutionError,
};

#[derive(Deserialize, Serialize, Debug, Default, Clone, PartialEq)]
pub struct VrlConfig {
    pub(crate) source: String,

    #[serde(default)]
    pub(crate) runtime: VrlRuntime,
}

inventory::submit! {
    ConditionDescription::new::<VrlConfig>("vrl")
}

impl_generate_config_from_default!(VrlConfig);

#[typetag::serde(name = "vrl")]
impl ConditionConfig for VrlConfig {
    fn build(&self, enrichment_tables: &enrichment::TableRegistry) -> crate::Result<Condition> {
        // TODO(jean): re-add this to VRL
        // let constraint = TypeConstraint {
        //     allow_any: false,
        //     type_def: TypeDef {
        //         fallible: true,
        //         kind: value::Kind::Boolean,
        //         ..Default::default()
        //     },
        // };

        // Filter out functions that directly mutate the event.
        //
        // TODO(jean): expose this as a method on the `Function` trait, so we
        // don't need to do this manually.
        let functions = vrl_stdlib::all()
            .into_iter()
            .filter(|f| f.identifier() != "del")
            .filter(|f| f.identifier() != "only_fields")
            .chain(enrichment::vrl_functions().into_iter())
            .chain(vector_vrl_functions::vrl_functions())
            .collect::<Vec<_>>();

        let mut state = vrl::state::ExternalEnv::default();
        state.set_external_context(enrichment_tables.clone());

        let (program, warnings) = vrl::compile_with_state(&self.source, &functions, &mut state)
            .map_err(|diagnostics| {
                Formatter::new(&self.source, diagnostics)
                    .colored()
                    .to_string()
            })?;

        if !warnings.is_empty() {
            let warnings = Formatter::new(&self.source, warnings).colored().to_string();
            warn!(message = "VRL compilation warning.", %warnings);
        }

        match self.runtime {
            VrlRuntime::Vm => {
                let vm = Arc::new(Runtime::default().compile(functions, &program, &mut state)?);
                Ok(Condition::VrlVm(VrlVm {
                    program,
                    source: self.source.clone(),
                    vm,
                }))
            }
            VrlRuntime::Ast => Ok(Condition::Vrl(Vrl {
                program,
                source: self.source.clone(),
            })),
        }
    }
}

//------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Vrl {
    pub(super) program: Program,
    pub(super) source: String,
}

impl Vrl {
    fn run(&self, event: &Event) -> vrl::RuntimeResult {
        // TODO(jean): This clone exists until vrl-lang has an "immutable"
        // mode.
        //
        // For now, mutability in reduce "vrl ends-when conditions" is
        // allowed, but it won't mutate the original event, since we cloned it
        // here.
        //
        // Having first-class immutability support in the language allows for
        // more performance (one less clone), and boot-time errors when a
        // program wants to mutate its events.
        //
        // see: https://github.com/vectordotdev/vector/issues/4744
        let mut target = VrlTarget::new(event.clone(), self.program.info());
        // TODO: use timezone from remap config
        let timezone = TimeZone::default();

        Runtime::default().resolve(&mut target, &self.program, &timezone)
    }
}

impl Conditional for Vrl {
    fn check(&self, event: &Event) -> bool {
        self.run(event)
            .map(|value| match value {
                Value::Boolean(boolean) => boolean,
                _ => false,
            })
            .unwrap_or_else(|err| {
                emit!(VrlConditionExecutionError {
                    error: err.to_string().as_ref()
                });
                false
            })
    }

    fn check_with_context(&self, event: &Event) -> Result<(), String> {
        let value = self.run(event).map_err(|err| match err {
            vrl::Terminate::Abort(err) => {
                let err = Formatter::new(
                    &self.source,
                    vrl::diagnostic::Diagnostic::from(
                        Box::new(err) as Box<dyn vrl::diagnostic::DiagnosticMessage>
                    ),
                )
                .colored()
                .to_string();
                format!("source execution aborted: {}", err)
            }
            vrl::Terminate::Error(err) => {
                let err = Formatter::new(
                    &self.source,
                    vrl::diagnostic::Diagnostic::from(
                        Box::new(err) as Box<dyn vrl::diagnostic::DiagnosticMessage>
                    ),
                )
                .colored()
                .to_string();
                format!("source execution failed: {}", err)
            }
        })?;

        match value {
            Value::Boolean(v) if v => Ok(()),
            Value::Boolean(v) if !v => Err("source execution resolved to false".into()),
            _ => Err("source execution resolved to a non-boolean value".into()),
        }
    }
}

//------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct VrlVm {
    pub(super) program: Program,
    pub(super) source: String,
    pub(super) vm: Arc<Vm>,
}

impl VrlVm {
    fn run(&self, event: &Event) -> vrl::RuntimeResult {
        // TODO(jean): This clone exists until vrl-lang has an "immutable"
        // mode.
        //
        // For now, mutability in reduce "vrl ends-when conditions" is
        // allowed, but it won't mutate the original event, since we cloned it
        // here.
        //
        // Having first-class immutability support in the language allows for
        // more performance (one less clone), and boot-time errors when a
        // program wants to mutate its events.
        //
        // see: https://github.com/vectordotdev/vector/issues/4744
        let mut target = VrlTarget::new(event.clone(), self.program.info());
        // TODO: use timezone from remap config
        let timezone = TimeZone::default();

        Runtime::default().run_vm(&self.vm, &mut target, &timezone)
    }
}

impl Conditional for VrlVm {
    fn check(&self, event: &Event) -> bool {
        self.run(event)
            .map(|value| match value {
                Value::Boolean(boolean) => boolean,
                _ => false,
            })
            .unwrap_or_else(|err| {
                emit!(VrlConditionExecutionError {
                    error: err.to_string().as_ref()
                });
                false
            })
    }

    fn check_with_context(&self, event: &Event) -> Result<(), String> {
        let value = self.run(event).map_err(|err| match err {
            vrl::Terminate::Abort(err) => {
                let err = Formatter::new(
                    &self.source,
                    vrl::diagnostic::Diagnostic::from(
                        Box::new(err) as Box<dyn vrl::diagnostic::DiagnosticMessage>
                    ),
                )
                .colored()
                .to_string();
                format!("source execution aborted: {}", err)
            }
            vrl::Terminate::Error(err) => {
                let err = Formatter::new(
                    &self.source,
                    vrl::diagnostic::Diagnostic::from(
                        Box::new(err) as Box<dyn vrl::diagnostic::DiagnosticMessage>
                    ),
                )
                .colored()
                .to_string();
                format!("source execution failed: {}", err)
            }
        })?;

        match value {
            Value::Boolean(v) if v => Ok(()),
            Value::Boolean(v) if !v => Err("source execution resolved to false".into()),
            _ => Err("source execution resolved to a non-boolean value".into()),
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{
        event::{Metric, MetricKind, MetricValue},
        log_event,
    };

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<VrlConfig>();
    }

    #[test]
    fn check_vrl() {
        let checks = vec![
            (
                log_event![],   // event
                "true == true", // source
                Ok(()),         // build result
                Ok(()),         // check result
            ),
            (
                log_event!["foo" => true, "bar" => false],
                "to_bool(.bar || .foo) ?? false",
                Ok(()),
                Ok(()),
            ),
            (
                log_event![],
                "true == false",
                Ok(()),
                Err("source execution resolved to false"),
            ),
            // TODO: enable once we don't emit large diagnostics with colors when no tty is present.
            // (
            //     log_event![],
            //     "null",
            //     Err("\n\u{1b}[0m\u{1b}[1m\u{1b}[38;5;9merror\u{1b}[0m\u{1b}[1m: unexpected return value\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m┌─\u{1b}[0m :1:1\n  \u{1b}[0m\u{1b}[34m│\u{1b}[0m\n\u{1b}[0m\u{1b}[34m1\u{1b}[0m \u{1b}[0m\u{1b}[34m│\u{1b}[0m \u{1b}[0m\u{1b}[31mnull\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m│\u{1b}[0m \u{1b}[0m\u{1b}[31m^^^^\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m│\u{1b}[0m \u{1b}[0m\u{1b}[31m│\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m│\u{1b}[0m \u{1b}[0m\u{1b}[31mgot: null\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m│\u{1b}[0m \u{1b}[0m\u{1b}[34mexpected: boolean\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m│\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m=\u{1b}[0m see language documentation at: https://vector.dev/docs/reference/vrl/\n\n"),
            //     Ok(()),
            // ),
            // (
            //     log_event!["foo" => "string"],
            //     ".foo",
            //     Err("\n\u{1b}[0m\u{1b}[1m\u{1b}[38;5;9merror\u{1b}[0m\u{1b}[1m: unexpected return value\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m┌─\u{1b}[0m :1:1\n  \u{1b}[0m\u{1b}[34m│\u{1b}[0m\n\u{1b}[0m\u{1b}[34m1\u{1b}[0m \u{1b}[0m\u{1b}[34m│\u{1b}[0m \u{1b}[0m\u{1b}[31m.foo\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m│\u{1b}[0m \u{1b}[0m\u{1b}[31m^^^^\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m│\u{1b}[0m \u{1b}[0m\u{1b}[31m│\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m│\u{1b}[0m \u{1b}[0m\u{1b}[31mgot: any\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m│\u{1b}[0m \u{1b}[0m\u{1b}[34mexpected: boolean\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m│\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m=\u{1b}[0m see language documentation at: https://vector.dev/docs/reference/vrl/\n\n"),
            //     Ok(()),
            // ),
            // (
            //     log_event![],
            //     ".",
            //     Err("n\u{1b}[0m\u{1b}[1m\u{1b}[38;5;9merror\u{1b}[0m\u{1b}[1m: unexpected return value\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m┌─\u{1b}[0m :1:1\n  \u{1b}[0m\u{1b}[34m│\u{1b}[0m\n\u{1b}[0m\u{1b}[34m1\u{1b}[0m \u{1b}[0m\u{1b}[34m│\u{1b}[0m \u{1b}[0m\u{1b}[31m.\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m│\u{1b}[0m \u{1b}[0m\u{1b}[31m^\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m│\u{1b}[0m \u{1b}[0m\u{1b}[31m│\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m│\u{1b}[0m \u{1b}[0m\u{1b}[31mgot: any\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m│\u{1b}[0m \u{1b}[0m\u{1b}[34mexpected: boolean\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m│\u{1b}[0m\n  \u{1b}[0m\u{1b}[34m=\u{1b}[0m see language documentation at: https://vector.dev/docs/reference/vrl/\n\n"),
            //     Ok(()),
            // ),
            (
                Event::Metric(
                    Metric::new(
                        "zork",
                        MetricKind::Incremental,
                        MetricValue::Counter { value: 1.0 },
                    )
                    .with_namespace(Some("zerk"))
                    .with_tags(Some({
                        let mut tags = BTreeMap::new();
                        tags.insert("host".into(), "zoobub".into());
                        tags
                    })),
                ),
                r#".name == "zork" && .tags.host == "zoobub" && .kind == "incremental""#,
                Ok(()),
                Ok(()),
            ),
        ];

        for (event, source, build, check) in checks {
            let source = source.to_owned();
            let config = VrlConfig {
                source,
                runtime: Default::default(),
            };

            assert_eq!(
                config
                    .build(&Default::default())
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
                build
            );

            if let Ok(cond) = config.build(&Default::default()) {
                assert_eq!(
                    cond.check_with_context(&event),
                    check.map_err(|e| e.to_string())
                );
            }
        }
    }
}
