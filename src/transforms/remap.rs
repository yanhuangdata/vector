use std::sync::Arc;
use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, Read},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use value::Kind;
use vector_common::TimeZone;
use vrl::{
    diagnostic::{Formatter, Note},
    prelude::{DiagnosticMessage, ExpressionError},
    Program, Runtime, Terminate, Vm, VrlRuntime,
};

use crate::{
    config::{
        log_schema, ComponentKey, DataType, Input, Output, TransformConfig, TransformContext,
        TransformDescription,
    },
    event::{Event, TargetEvents, VrlTarget},
    internal_events::{RemapMappingAbort, RemapMappingError},
    schema,
    transforms::{SyncTransform, Transform, TransformOutputsBuf},
    Result,
};

const DROPPED: &str = "dropped";

#[derive(Deserialize, Serialize, Debug, Clone, Derivative)]
#[serde(deny_unknown_fields, default)]
#[derivative(Default)]
pub struct RemapConfig {
    pub source: Option<String>,
    pub file: Option<PathBuf>,
    #[serde(default)]
    pub timezone: TimeZone,
    pub drop_on_error: bool,
    #[serde(default = "crate::serde::default_true")]
    pub drop_on_abort: bool,
    pub reroute_dropped: bool,
    #[serde(default)]
    pub runtime: VrlRuntime,
}

impl RemapConfig {
    fn compile_vrl_program(
        &self,
        enrichment_tables: enrichment::TableRegistry,
        merged_schema_definition: schema::Definition,
    ) -> Result<(
        vrl::Program,
        String,
        Vec<Box<dyn vrl::Function>>,
        vrl::state::ExternalEnv,
    )> {
        let source = match (&self.source, &self.file) {
            (Some(source), None) => source.to_owned(),
            (None, Some(path)) => {
                let mut buffer = String::new();

                File::open(path)
                    .with_context(|_| FileOpenFailedSnafu { path })?
                    .read_to_string(&mut buffer)
                    .with_context(|_| FileReadFailedSnafu { path })?;

                buffer
            }
            _ => return Err(Box::new(BuildError::SourceAndOrFile)),
        };

        let mut functions = vrl_stdlib::all();
        functions.append(&mut enrichment::vrl_functions());
        functions.append(&mut vector_vrl_functions::vrl_functions());

        let mut state = vrl::state::ExternalEnv::new_with_kind(merged_schema_definition.into());
        state.set_external_context(enrichment_tables);

        vrl::compile_with_state(&source, &functions, &mut state)
            .map_err(|diagnostics| {
                Formatter::new(&source, diagnostics)
                    .colored()
                    .to_string()
                    .into()
            })
            .map(|(program, diagnostics)| {
                (
                    program,
                    Formatter::new(&source, diagnostics).to_string(),
                    functions,
                    state,
                )
            })
    }
}

inventory::submit! {
    TransformDescription::new::<RemapConfig>("remap")
}

impl_generate_config_from_default!(RemapConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "remap")]
impl TransformConfig for RemapConfig {
    async fn build(&self, context: &TransformContext) -> Result<Transform> {
        let (transform, warnings) = match self.runtime {
            VrlRuntime::Ast => {
                let (remap, warnings) = Remap::new_ast(self.clone(), context)?;
                (Transform::synchronous(remap), warnings)
            }
            VrlRuntime::Vm => {
                let (remap, warnings) = Remap::new_vm(self.clone(), context)?;
                (Transform::synchronous(remap), warnings)
            }
        };

        // TODO: We could improve on this by adding support for non-fatal error
        // messages in the topology. This would make the topology responsible
        // for printing warnings (including potentially emiting metrics),
        // instead of individual transforms.
        if !warnings.is_empty() {
            warn!(message = "VRL compilation warning.", %warnings);
        }

        Ok(transform)
    }

    fn input(&self) -> Input {
        Input::all()
    }

    fn outputs(&self, merged_definition: &schema::Definition) -> Vec<Output> {
        // We need to compile the VRL program in order to know the schema definition output of this
        // transform. We ignore any compilation errors, as those are caught by the transform build
        // step.
        //
        // TODO: Keep track of semantic meaning for fields.
        let default_definition = self
            .compile_vrl_program(
                enrichment::TableRegistry::default(),
                merged_definition.clone(),
            )
            .ok()
            .and_then(|(_, _, _, state)| state.target_kind().cloned())
            .and_then(Kind::into_object)
            .map(Into::into)
            .unwrap_or_else(schema::Definition::empty);

        // When a message is dropped and re-routed, we keep the original event, but also annotate
        // it with additional metadata.
        let dropped_definition = merged_definition.clone().required_field(
            log_schema().metadata_key(),
            Kind::object(BTreeMap::from([
                ("reason".into(), Kind::bytes()),
                ("message".into(), Kind::bytes()),
                ("component_id".into(), Kind::bytes()),
                ("component_type".into(), Kind::bytes()),
                ("component_kind".into(), Kind::bytes()),
            ])),
            Some("metadata"),
        );

        let default_output =
            Output::default(DataType::all()).with_schema_definition(default_definition);

        if self.reroute_dropped {
            vec![
                default_output,
                Output::default(DataType::all())
                    .with_schema_definition(dropped_definition)
                    .with_port(DROPPED),
            ]
        } else {
            vec![default_output]
        }
    }

    fn transform_type(&self) -> &'static str {
        "remap"
    }

    fn enable_concurrency(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
pub struct Remap<Runner>
where
    Runner: VrlRunner,
{
    component_key: Option<ComponentKey>,
    program: Program,
    timezone: TimeZone,
    drop_on_error: bool,
    drop_on_abort: bool,
    reroute_dropped: bool,
    default_schema_definition: Arc<schema::Definition>,
    dropped_schema_definition: Arc<schema::Definition>,
    runner: Runner,
}

pub trait VrlRunner {
    fn run(
        &mut self,
        target: &mut VrlTarget,
        program: &Program,
        timezone: &TimeZone,
    ) -> std::result::Result<value::Value, Terminate>;
}

#[derive(Debug)]
pub struct VmRunner {
    runtime: Runtime,
    vm: Arc<Vm>,
}

impl Clone for VmRunner {
    fn clone(&self) -> Self {
        Self {
            runtime: Runtime::default(),
            vm: Arc::clone(&self.vm),
        }
    }
}

impl VrlRunner for VmRunner {
    fn run(
        &mut self,
        target: &mut VrlTarget,
        _: &Program,
        timezone: &TimeZone,
    ) -> std::result::Result<value::Value, Terminate> {
        self.runtime.run_vm(&self.vm, target, timezone)
    }
}

#[derive(Debug)]
pub struct AstRunner {
    pub runtime: Runtime,
}

impl Clone for AstRunner {
    fn clone(&self) -> Self {
        Self {
            runtime: Runtime::default(),
        }
    }
}

impl VrlRunner for AstRunner {
    fn run(
        &mut self,
        target: &mut VrlTarget,
        program: &Program,
        timezone: &TimeZone,
    ) -> std::result::Result<value::Value, Terminate> {
        let result = self.runtime.resolve(target, program, timezone);
        self.runtime.clear();
        result
    }
}

impl Remap<VmRunner> {
    pub fn new_vm(
        config: RemapConfig,
        context: &TransformContext,
    ) -> crate::Result<(Self, String)> {
        let (program, warnings, functions, mut state) = config.compile_vrl_program(
            context.enrichment_tables.clone(),
            context.merged_schema_definition.clone(),
        )?;

        let runtime = Runtime::default();
        let vm = runtime.compile(functions, &program, &mut state)?;
        let runner = VmRunner {
            runtime,
            vm: Arc::new(vm),
        };

        Self::new(config, context, program, runner).map(|remap| (remap, warnings))
    }
}

impl Remap<AstRunner> {
    pub fn new_ast(
        config: RemapConfig,
        context: &TransformContext,
    ) -> crate::Result<(Self, String)> {
        let (program, warnings, _, _) = config.compile_vrl_program(
            context.enrichment_tables.clone(),
            context.merged_schema_definition.clone(),
        )?;

        let runtime = Runtime::default();
        let runner = AstRunner { runtime };

        Self::new(config, context, program, runner).map(|remap| (remap, warnings))
    }
}

impl<Runner> Remap<Runner>
where
    Runner: VrlRunner,
{
    fn new(
        config: RemapConfig,
        context: &TransformContext,
        program: Program,
        runner: Runner,
    ) -> crate::Result<Self> {
        let default_schema_definition = context
            .schema_definitions
            .get(&None)
            .expect("default schema required")
            .clone();

        let dropped_schema_definition = context
            .schema_definitions
            .get(&Some(DROPPED.to_owned()))
            .or_else(|| context.schema_definitions.get(&None))
            .expect("dropped schema required")
            .clone();

        Ok(Remap {
            component_key: context.key.clone(),
            program,
            timezone: config.timezone,
            drop_on_error: config.drop_on_error,
            drop_on_abort: config.drop_on_abort,
            reroute_dropped: config.reroute_dropped,
            default_schema_definition: Arc::new(default_schema_definition),
            dropped_schema_definition: Arc::new(dropped_schema_definition),
            runner,
        })
    }

    #[cfg(test)]
    fn runner(&self) -> &Runner {
        &self.runner
    }

    fn anotate_data(&self, reason: &str, error: ExpressionError) -> serde_json::Value {
        let message = error
            .notes()
            .iter()
            .filter(|note| matches!(note, Note::UserErrorMessage(_)))
            .last()
            .map(|note| note.to_string())
            .unwrap_or_else(|| error.to_string());
        serde_json::json!({
            "dropped": {
                "reason": reason,
                "message": message,
                "component_id": self.component_key,
                "component_type": "remap",
                "component_kind": "transform",
            }
        })
    }

    fn annotate_dropped(&self, event: &mut Event, reason: &str, error: ExpressionError) {
        match event {
            Event::Log(ref mut log) => {
                log.insert(
                    log_schema().metadata_key(),
                    self.anotate_data(reason, error),
                );
            }
            Event::Metric(ref mut metric) => {
                let m = log_schema().metadata_key();
                metric.insert_tag(format!("{}.dropped.reason", m), reason.into());
                metric.insert_tag(
                    format!("{}.dropped.component_id", m),
                    self.component_key
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(String::new),
                );
                metric.insert_tag(format!("{}.dropped.component_type", m), "remap".into());
                metric.insert_tag(format!("{}.dropped.component_kind", m), "transform".into());
            }
            Event::Trace(ref mut trace) => {
                trace.insert(
                    log_schema().metadata_key(),
                    self.anotate_data(reason, error),
                );
            }
        }
    }

    fn run_vrl(&mut self, target: &mut VrlTarget) -> std::result::Result<value::Value, Terminate> {
        self.runner.run(target, &self.program, &self.timezone)
    }
}

impl<Runner> SyncTransform for Remap<Runner>
where
    Runner: VrlRunner + Clone + Send + Sync,
{
    fn transform(&mut self, event: Event, output: &mut TransformOutputsBuf) {
        // If a program can fail or abort at runtime and we know that we will still need to forward
        // the event in that case (either to the main output or `dropped`, depending on the
        // config), we need to clone the original event and keep it around, to allow us to discard
        // any mutations made to the event while the VRL program runs, before it failed or aborted.
        //
        // The `drop_on_{error, abort}` transform config allows operators to remove events from the
        // main output if they're failed or aborted, in which case we can skip the cloning, since
        // any mutations made by VRL will be ignored regardless. If they hav configured
        // `reroute_dropped`, however, we still need to do the clone to ensure that we can forward
        // the event to the `dropped` output.
        let forward_on_error = !self.drop_on_error || self.reroute_dropped;
        let forward_on_abort = !self.drop_on_abort || self.reroute_dropped;
        let original_event = if (self.program.info().fallible && forward_on_error)
            || (self.program.info().abortable && forward_on_abort)
        {
            Some(event.clone())
        } else {
            None
        };

        let mut target = VrlTarget::new(event, self.program.info());
        let result = self.run_vrl(&mut target);

        match result {
            Ok(_) => match target.into_events() {
                TargetEvents::One(event) => {
                    push_default(event, output, &self.default_schema_definition)
                }
                TargetEvents::Logs(events) => events
                    .for_each(|event| push_default(event, output, &self.default_schema_definition)),
                TargetEvents::Traces(events) => events
                    .for_each(|event| push_default(event, output, &self.default_schema_definition)),
            },
            Err(reason) => {
                let (reason, error, drop) = match reason {
                    Terminate::Abort(error) => {
                        emit!(RemapMappingAbort {
                            event_dropped: self.drop_on_abort,
                        });

                        ("abort", error, self.drop_on_abort)
                    }
                    Terminate::Error(error) => {
                        emit!(RemapMappingError {
                            error: error.to_string(),
                            event_dropped: self.drop_on_error,
                        });

                        ("error", error, self.drop_on_error)
                    }
                };

                if !drop {
                    let event = original_event.expect("event will be set");

                    push_default(event, output, &self.default_schema_definition);
                } else if self.reroute_dropped {
                    let mut event = original_event.expect("event will be set");

                    self.annotate_dropped(&mut event, reason, error);
                    push_dropped(event, output, &self.dropped_schema_definition);
                }
            }
        }
    }
}

#[inline]
fn push_default(
    mut event: Event,
    output: &mut TransformOutputsBuf,
    schema_definition: &Arc<schema::Definition>,
) {
    event
        .metadata_mut()
        .set_schema_definition(schema_definition);

    output.push(event)
}

#[inline]
fn push_dropped(
    mut event: Event,
    output: &mut TransformOutputsBuf,
    schema_definition: &Arc<schema::Definition>,
) {
    event
        .metadata_mut()
        .set_schema_definition(schema_definition);

    output.push_named(DROPPED, event)
}

#[derive(Debug, Snafu)]
pub enum BuildError {
    #[snafu(display("must provide exactly one of `source` or `file` configuration"))]
    SourceAndOrFile,

    #[snafu(display("Could not open vrl program {:?}: {}", path, source))]
    FileOpenFailed { path: PathBuf, source: io::Error },
    #[snafu(display("Could not read vrl program {:?}: {}", path, source))]
    FileReadFailed { path: PathBuf, source: io::Error },
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use indoc::{formatdoc, indoc};
    use vector_common::btreemap;
    use vector_core::event::EventMetadata;

    use super::*;
    use crate::{
        config::{build_unit_tests, ConfigBuilder},
        event::{
            metric::{MetricKind, MetricValue},
            LogEvent, Metric, Value,
        },
        schema,
        test_util::components::{init_test, COMPONENT_MULTIPLE_OUTPUTS_TESTS},
        transforms::OutputBuffer,
    };

    fn test_default_schema_definition() -> schema::Definition {
        schema::Definition::empty().required_field(
            "a default field",
            Kind::integer().or_bytes(),
            Some("default"),
        )
    }

    fn test_dropped_schema_definition() -> schema::Definition {
        schema::Definition::empty().required_field(
            "a dropped field",
            Kind::boolean().or_null(),
            Some("dropped"),
        )
    }

    fn remap(config: RemapConfig) -> Result<Remap<AstRunner>> {
        let schema_definitions = HashMap::from([
            (None, test_default_schema_definition()),
            (Some(DROPPED.to_owned()), test_dropped_schema_definition()),
        ]);

        Remap::new_ast(config, &TransformContext::new_test(schema_definitions))
            .map(|(remap, _)| remap)
    }

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<RemapConfig>();
    }

    #[test]
    fn config_missing_source_and_file() {
        let config = RemapConfig {
            source: None,
            file: None,
            ..Default::default()
        };

        let err = remap(config).unwrap_err().to_string();
        assert_eq!(
            &err,
            "must provide exactly one of `source` or `file` configuration"
        )
    }

    #[test]
    fn config_both_source_and_file() {
        let config = RemapConfig {
            source: Some("".to_owned()),
            file: Some("".into()),
            ..Default::default()
        };

        let err = remap(config).unwrap_err().to_string();
        assert_eq!(
            &err,
            "must provide exactly one of `source` or `file` configuration"
        )
    }

    fn get_field_string(event: &Event, field: &str) -> String {
        event.as_log().get(field).unwrap().to_string_lossy()
    }

    #[test]
    fn check_remap_doesnt_share_state_between_events() {
        let conf = RemapConfig {
            source: Some(".foo = .sentinel".to_string()),
            file: None,
            timezone: TimeZone::default(),
            drop_on_error: true,
            drop_on_abort: false,
            ..Default::default()
        };
        let mut tform = remap(conf).unwrap();
        assert!(tform.runner().runtime.is_empty());

        let event1 = {
            let mut event1 = LogEvent::from("event1");
            event1.insert("sentinel", "bar");
            Event::from(event1)
        };
        let result1 = transform_one(&mut tform, event1).unwrap();
        assert_eq!(get_field_string(&result1, "message"), "event1");
        assert_eq!(get_field_string(&result1, "foo"), "bar");
        assert_eq!(
            result1.metadata().schema_definition(),
            &test_default_schema_definition()
        );
        assert!(tform.runner().runtime.is_empty());

        let event2 = {
            let event2 = LogEvent::from("event2");
            Event::from(event2)
        };
        let result2 = transform_one(&mut tform, event2).unwrap();
        assert_eq!(get_field_string(&result2, "message"), "event2");
        assert_eq!(result2.as_log().get("foo"), Some(&Value::Null));
        assert_eq!(
            result2.metadata().schema_definition(),
            &test_default_schema_definition()
        );
        assert!(tform.runner().runtime.is_empty());
    }

    #[test]
    fn check_remap_adds() {
        let event = {
            let mut event = LogEvent::from("augment me");
            event.insert("copy_from", "buz");
            Event::from(event)
        };

        let conf = RemapConfig {
            source: Some(
                r#"  .foo = "bar"
  .bar = "baz"
  .copy = .copy_from
"#
                .to_string(),
            ),
            file: None,
            timezone: TimeZone::default(),
            drop_on_error: true,
            drop_on_abort: false,
            ..Default::default()
        };
        let mut tform = remap(conf).unwrap();
        let result = transform_one(&mut tform, event).unwrap();
        assert_eq!(get_field_string(&result, "message"), "augment me");
        assert_eq!(get_field_string(&result, "copy_from"), "buz");
        assert_eq!(get_field_string(&result, "foo"), "bar");
        assert_eq!(get_field_string(&result, "bar"), "baz");
        assert_eq!(get_field_string(&result, "copy"), "buz");

        assert_eq!(
            result.metadata().schema_definition(),
            &test_default_schema_definition()
        );
    }

    #[test]
    fn check_remap_emits_multiple() {
        let event = {
            let mut event = LogEvent::from("augment me");
            event.insert(
                "events",
                vec![btreemap!("message" => "foo"), btreemap!("message" => "bar")],
            );
            Event::from(event)
        };

        let conf = RemapConfig {
            source: Some(
                indoc! {r#"
                . = .events
            "#}
                .to_owned(),
            ),
            file: None,
            timezone: TimeZone::default(),
            drop_on_error: true,
            drop_on_abort: false,
            ..Default::default()
        };
        let mut tform = remap(conf).unwrap();

        let out = collect_outputs(&mut tform, event);
        assert_eq!(2, out.primary.len());
        let mut result = out.primary.into_events();

        let r = result.next().unwrap();
        assert_eq!(get_field_string(&r, "message"), "foo");
        assert_eq!(
            r.metadata().schema_definition(),
            &test_default_schema_definition()
        );
        let r = result.next().unwrap();
        assert_eq!(get_field_string(&r, "message"), "bar");

        assert_eq!(
            r.metadata().schema_definition(),
            &test_default_schema_definition()
        );
    }

    #[test]
    fn check_remap_error() {
        let event = {
            let mut event = Event::from("augment me");
            event.as_mut_log().insert("bar", "is a string");
            event
        };

        let conf = RemapConfig {
            source: Some(formatdoc! {r#"
                .foo = "foo"
                .not_an_int = int!(.bar)
                .baz = 12
            "#}),
            file: None,
            timezone: TimeZone::default(),
            drop_on_error: false,
            drop_on_abort: false,
            ..Default::default()
        };
        let mut tform = remap(conf).unwrap();

        let event = transform_one(&mut tform, event).unwrap();

        assert_eq!(event.as_log().get("bar"), Some(&Value::from("is a string")));
        assert!(event.as_log().get("foo").is_none());
        assert!(event.as_log().get("baz").is_none());
    }

    #[test]
    fn check_remap_error_drop() {
        let event = {
            let mut event = Event::from("augment me");
            event.as_mut_log().insert("bar", "is a string");
            event
        };

        let conf = RemapConfig {
            source: Some(formatdoc! {r#"
                .foo = "foo"
                .not_an_int = int!(.bar)
                .baz = 12
            "#}),
            file: None,
            timezone: TimeZone::default(),
            drop_on_error: true,
            drop_on_abort: false,
            ..Default::default()
        };
        let mut tform = remap(conf).unwrap();

        assert!(transform_one(&mut tform, event).is_none())
    }

    #[test]
    fn check_remap_error_infallible() {
        let event = {
            let mut event = Event::from("augment me");
            event.as_mut_log().insert("bar", "is a string");
            event
        };

        let conf = RemapConfig {
            source: Some(formatdoc! {r#"
                .foo = "foo"
                .baz = 12
            "#}),
            file: None,
            timezone: TimeZone::default(),
            drop_on_error: false,
            drop_on_abort: false,
            ..Default::default()
        };
        let mut tform = remap(conf).unwrap();

        let event = transform_one(&mut tform, event).unwrap();

        assert_eq!(event.as_log().get("foo"), Some(&Value::from("foo")));
        assert_eq!(event.as_log().get("bar"), Some(&Value::from("is a string")));
        assert_eq!(event.as_log().get("baz"), Some(&Value::from(12)));
    }

    #[test]
    fn check_remap_abort() {
        let event = {
            let mut event = Event::from("augment me");
            event.as_mut_log().insert("bar", "is a string");
            event
        };

        let conf = RemapConfig {
            source: Some(formatdoc! {r#"
                .foo = "foo"
                abort
                .baz = 12
            "#}),
            file: None,
            timezone: TimeZone::default(),
            drop_on_error: false,
            drop_on_abort: false,
            ..Default::default()
        };
        let mut tform = remap(conf).unwrap();

        let event = transform_one(&mut tform, event).unwrap();

        assert_eq!(event.as_log().get("bar"), Some(&Value::from("is a string")));
        assert!(event.as_log().get("foo").is_none());
        assert!(event.as_log().get("baz").is_none());
    }

    #[test]
    fn check_remap_abort_drop() {
        let event = {
            let mut event = Event::from("augment me");
            event.as_mut_log().insert("bar", "is a string");
            event
        };

        let conf = RemapConfig {
            source: Some(formatdoc! {r#"
                .foo = "foo"
                abort
                .baz = 12
            "#}),
            file: None,
            timezone: TimeZone::default(),
            drop_on_error: false,
            drop_on_abort: true,
            ..Default::default()
        };
        let mut tform = remap(conf).unwrap();

        assert!(transform_one(&mut tform, event).is_none())
    }

    #[test]
    fn check_remap_metric() {
        let metric = Event::Metric(Metric::new(
            "counter",
            MetricKind::Absolute,
            MetricValue::Counter { value: 1.0 },
        ));
        let metadata = metric.metadata().clone();

        let conf = RemapConfig {
            source: Some(
                r#".tags.host = "zoobub"
                       .name = "zork"
                       .namespace = "zerk"
                       .kind = "incremental""#
                    .to_string(),
            ),
            file: None,
            timezone: TimeZone::default(),
            drop_on_error: true,
            drop_on_abort: false,
            ..Default::default()
        };
        let mut tform = remap(conf).unwrap();

        let result = transform_one(&mut tform, metric).unwrap();
        assert_eq!(
            result,
            Event::Metric(
                Metric::new_with_metadata(
                    "zork",
                    MetricKind::Incremental,
                    MetricValue::Counter { value: 1.0 },
                    metadata.with_schema_definition(&Arc::new(test_default_schema_definition())),
                )
                .with_namespace(Some("zerk"))
                .with_tags(Some({
                    let mut tags = BTreeMap::new();
                    tags.insert("host".into(), "zoobub".into());
                    tags
                }))
            )
        );
    }

    #[test]
    fn check_remap_branching() {
        let happy = Event::try_from(serde_json::json!({"hello": "world"})).unwrap();
        let abort = Event::try_from(serde_json::json!({"hello": "goodbye"})).unwrap();
        let error = Event::try_from(serde_json::json!({"hello": 42})).unwrap();

        let happy_metric = {
            let mut metric = Metric::new(
                "counter",
                MetricKind::Absolute,
                MetricValue::Counter { value: 1.0 },
            );
            metric.insert_tag("hello".into(), "world".into());
            Event::Metric(metric)
        };

        let abort_metric = {
            let mut metric = Metric::new(
                "counter",
                MetricKind::Absolute,
                MetricValue::Counter { value: 1.0 },
            );
            metric.insert_tag("hello".into(), "goodbye".into());
            Event::Metric(metric)
        };

        let error_metric = {
            let mut metric = Metric::new(
                "counter",
                MetricKind::Absolute,
                MetricValue::Counter { value: 1.0 },
            );
            metric.insert_tag("not_hello".into(), "oops".into());
            Event::Metric(metric)
        };

        let conf = RemapConfig {
            source: Some(formatdoc! {r#"
                if exists(.tags) {{
                    # metrics
                    .tags.foo = "bar"
                    if string!(.tags.hello) == "goodbye" {{
                      abort
                    }}
                }} else {{
                    # logs
                    .foo = "bar"
                    if string(.hello) == "goodbye" {{
                      abort
                    }}
                }}
            "#}),
            drop_on_error: true,
            drop_on_abort: true,
            reroute_dropped: true,
            ..Default::default()
        };
        let schema_definitions = HashMap::from([
            (None, test_default_schema_definition()),
            (Some(DROPPED.to_owned()), test_dropped_schema_definition()),
        ]);
        let context = TransformContext {
            key: Some(ComponentKey::from("remapper")),
            schema_definitions,
            merged_schema_definition: schema::Definition::empty().required_field(
                "hello",
                Kind::bytes(),
                None,
            ),
            ..Default::default()
        };
        let mut tform = Remap::new_ast(conf, &context).unwrap().0;

        let output = transform_one_fallible(&mut tform, happy).unwrap();
        let log = output.as_log();
        assert_eq!(log["hello"], "world".into());
        assert_eq!(log["foo"], "bar".into());
        assert!(!log.contains("metadata"));

        let output = transform_one_fallible(&mut tform, abort).unwrap_err();
        let log = output.as_log();
        assert_eq!(log["hello"], "goodbye".into());
        assert!(!log.contains("foo"));
        assert_eq!(
            log["metadata"],
            serde_json::json!({
                "dropped": {
                    "reason": "abort",
                    "message": "aborted",
                    "component_id": "remapper",
                    "component_type": "remap",
                    "component_kind": "transform",
                }
            })
            .try_into()
            .unwrap()
        );

        let output = transform_one_fallible(&mut tform, error).unwrap_err();
        let log = output.as_log();
        assert_eq!(log["hello"], 42.into());
        assert!(!log.contains("foo"));
        assert_eq!(
            log["metadata"],
            serde_json::json!({
                "dropped": {
                    "reason": "error",
                    "message": "function call error for \"string\" at (160:174): expected string, got integer",
                    "component_id": "remapper",
                    "component_type": "remap",
                    "component_kind": "transform",
                }
            })
            .try_into()
            .unwrap()
        );

        let output = transform_one_fallible(&mut tform, happy_metric).unwrap();
        pretty_assertions::assert_eq!(
            output,
            Event::Metric(
                Metric::new_with_metadata(
                    "counter",
                    MetricKind::Absolute,
                    MetricValue::Counter { value: 1.0 },
                    EventMetadata::default()
                        .with_schema_definition(&Arc::new(test_default_schema_definition())),
                )
                .with_tags(Some({
                    let mut tags = BTreeMap::new();
                    tags.insert("hello".into(), "world".into());
                    tags.insert("foo".into(), "bar".into());
                    tags
                }))
            )
        );

        let output = transform_one_fallible(&mut tform, abort_metric).unwrap_err();
        pretty_assertions::assert_eq!(
            output,
            Event::Metric(
                Metric::new_with_metadata(
                    "counter",
                    MetricKind::Absolute,
                    MetricValue::Counter { value: 1.0 },
                    EventMetadata::default()
                        .with_schema_definition(&Arc::new(test_dropped_schema_definition())),
                )
                .with_tags(Some({
                    let mut tags = BTreeMap::new();
                    tags.insert("hello".into(), "goodbye".into());
                    tags.insert("metadata.dropped.reason".into(), "abort".into());
                    tags.insert("metadata.dropped.component_id".into(), "remapper".into());
                    tags.insert("metadata.dropped.component_type".into(), "remap".into());
                    tags.insert("metadata.dropped.component_kind".into(), "transform".into());
                    tags
                }))
            )
        );

        let output = transform_one_fallible(&mut tform, error_metric).unwrap_err();
        pretty_assertions::assert_eq!(
            output,
            Event::Metric(
                Metric::new_with_metadata(
                    "counter",
                    MetricKind::Absolute,
                    MetricValue::Counter { value: 1.0 },
                    EventMetadata::default()
                        .with_schema_definition(&Arc::new(test_dropped_schema_definition())),
                )
                .with_tags(Some({
                    let mut tags = BTreeMap::new();
                    tags.insert("not_hello".into(), "oops".into());
                    tags.insert("metadata.dropped.reason".into(), "error".into());
                    tags.insert("metadata.dropped.component_id".into(), "remapper".into());
                    tags.insert("metadata.dropped.component_type".into(), "remap".into());
                    tags.insert("metadata.dropped.component_kind".into(), "transform".into());
                    tags
                }))
            )
        );
    }

    #[test]
    fn check_remap_branching_assert_with_message() {
        let error_trigger_assert_custom_message =
            Event::try_from(serde_json::json!({"hello": 42})).unwrap();
        let error_trigger_default_assert_message =
            Event::try_from(serde_json::json!({"hello": 0})).unwrap();
        let conf = RemapConfig {
            source: Some(formatdoc! {r#"
                assert_eq!(.hello, 0, "custom message here")
                assert_eq!(.hello, 1)
            "#}),
            drop_on_error: true,
            drop_on_abort: true,
            reroute_dropped: true,
            ..Default::default()
        };
        let context = TransformContext {
            key: Some(ComponentKey::from("remapper")),
            ..Default::default()
        };
        let mut tform = Remap::new_ast(conf, &context).unwrap().0;

        let output =
            transform_one_fallible(&mut tform, error_trigger_assert_custom_message).unwrap_err();
        let log = output.as_log();
        assert_eq!(log["hello"], 42.into());
        assert!(!log.contains("foo"));
        assert_eq!(
            log["metadata"],
            serde_json::json!({
                "dropped": {
                    "reason": "error",
                    "message": "custom message here",
                    "component_id": "remapper",
                    "component_type": "remap",
                    "component_kind": "transform",
                }
            })
            .try_into()
            .unwrap()
        );

        let output =
            transform_one_fallible(&mut tform, error_trigger_default_assert_message).unwrap_err();
        let log = output.as_log();
        assert_eq!(log["hello"], 0.into());
        assert!(!log.contains("foo"));
        assert_eq!(
            log["metadata"],
            serde_json::json!({
                "dropped": {
                    "reason": "error",
                    "message": "function call error for \"assert_eq\" at (45:66): assertion failed: 0 == 1",
                    "component_id": "remapper",
                    "component_type": "remap",
                    "component_kind": "transform",
                }
            })
            .try_into()
            .unwrap()
        );
    }

    #[test]
    fn check_remap_branching_abort_with_message() {
        let error = Event::try_from(serde_json::json!({"hello": 42})).unwrap();
        let conf = RemapConfig {
            source: Some(formatdoc! {r#"
                abort "custom message here"
            "#}),
            drop_on_error: true,
            drop_on_abort: true,
            reroute_dropped: true,
            ..Default::default()
        };
        let context = TransformContext {
            key: Some(ComponentKey::from("remapper")),
            ..Default::default()
        };
        let mut tform = Remap::new_ast(conf, &context).unwrap().0;

        let output = transform_one_fallible(&mut tform, error).unwrap_err();
        let log = output.as_log();
        assert_eq!(log["hello"], 42.into());
        assert!(!log.contains("foo"));
        assert_eq!(
            log["metadata"],
            serde_json::json!({
                "dropped": {
                    "reason": "abort",
                    "message": "custom message here",
                    "component_id": "remapper",
                    "component_type": "remap",
                    "component_kind": "transform",
                }
            })
            .try_into()
            .unwrap()
        );
    }

    #[test]
    fn check_remap_branching_disabled() {
        let happy = Event::try_from(serde_json::json!({"hello": "world"})).unwrap();
        let abort = Event::try_from(serde_json::json!({"hello": "goodbye"})).unwrap();
        let error = Event::try_from(serde_json::json!({"hello": 42})).unwrap();

        let conf = RemapConfig {
            source: Some(formatdoc! {r#"
                if exists(.tags) {{
                    # metrics
                    .tags.foo = "bar"
                    if string!(.tags.hello) == "goodbye" {{
                      abort
                    }}
                }} else {{
                    # logs
                    .foo = "bar"
                    if string!(.hello) == "goodbye" {{
                      abort
                    }}
                }}
            "#}),
            drop_on_error: true,
            drop_on_abort: true,
            reroute_dropped: false,
            ..Default::default()
        };

        let schema_definition = schema::Definition::empty()
            .required_field("foo", Kind::bytes(), None)
            .required_field(
                "tags",
                Kind::object(BTreeMap::from([("foo".into(), Kind::bytes())])),
                None,
            );

        assert_eq!(
            vec![Output::default(DataType::all()).with_schema_definition(schema_definition)],
            conf.outputs(&schema::Definition::empty()),
        );

        let context = TransformContext {
            key: Some(ComponentKey::from("remapper")),
            ..Default::default()
        };
        let mut tform = Remap::new_ast(conf, &context).unwrap().0;

        let output = transform_one_fallible(&mut tform, happy).unwrap();
        let log = output.as_log();
        assert_eq!(log["hello"], "world".into());
        assert_eq!(log["foo"], "bar".into());
        assert!(!log.contains("metadata"));

        let out = collect_outputs(&mut tform, abort);
        assert!(out.primary.is_empty());
        assert!(out.named[DROPPED].is_empty());

        let out = collect_outputs(&mut tform, error);
        assert!(out.primary.is_empty());
        assert!(out.named[DROPPED].is_empty());
    }

    #[tokio::test]
    async fn check_remap_branching_metrics_with_output() {
        init_test();

        let config: ConfigBuilder = toml::from_str(indoc! {r#"
            [transforms.foo]
            inputs = []
            type = "remap"
            drop_on_abort = true
            reroute_dropped = true
            source = "abort"

            [[tests]]
            name = "metric output"

            [tests.input]
                insert_at = "foo"
                value = "none"

            [[tests.outputs]]
                extract_from = "foo.dropped"
                [[tests.outputs.conditions]]
                type = "vrl"
                source = "true"
        "#})
        .unwrap();

        let mut tests = build_unit_tests(config).await.unwrap();
        assert!(tests.remove(0).run().await.errors.is_empty());
        // Check that metrics were emitted with output tag
        COMPONENT_MULTIPLE_OUTPUTS_TESTS.assert(&["output"]);
    }

    struct CollectedOuput {
        primary: OutputBuffer,
        named: HashMap<String, OutputBuffer>,
    }

    fn collect_outputs(ft: &mut dyn SyncTransform, event: Event) -> CollectedOuput {
        let mut outputs = TransformOutputsBuf::new_with_capacity(
            vec![
                Output::default(DataType::all()),
                Output::default(DataType::all()).with_port(DROPPED),
            ],
            1,
        );

        ft.transform(event, &mut outputs);

        CollectedOuput {
            primary: outputs.take_primary(),
            named: outputs.take_all_named(),
        }
    }

    fn transform_one(ft: &mut dyn SyncTransform, event: Event) -> Option<Event> {
        let out = collect_outputs(ft, event);
        assert_eq!(0, out.named.iter().map(|(_, v)| v.len()).sum::<usize>());
        assert!(out.primary.len() <= 1);
        out.primary.into_events().next()
    }

    fn transform_one_fallible(
        ft: &mut dyn SyncTransform,
        event: Event,
    ) -> std::result::Result<Event, Event> {
        let mut outputs = TransformOutputsBuf::new_with_capacity(
            vec![
                Output::default(DataType::all()),
                Output::default(DataType::all()).with_port(DROPPED),
            ],
            1,
        );

        ft.transform(event, &mut outputs);

        let mut buf = outputs.drain().collect::<Vec<_>>();
        let mut err_buf = outputs.drain_named(DROPPED).collect::<Vec<_>>();

        assert!(buf.len() < 2);
        assert!(err_buf.len() < 2);
        match (buf.pop(), err_buf.pop()) {
            (Some(good), None) => Ok(good),
            (None, Some(bad)) => Err(bad),
            (a, b) => panic!("expected output xor error output, got {:?} and {:?}", a, b),
        }
    }
}
