use std::{net::IpAddr, str::FromStr};

use cidr_utils::cidr::IpCidr;
use indexmap::IndexMap;
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::{
    conditions::{Condition, ConditionConfig, ConditionDescription, Conditional},
    event::{Event, Value},
};

#[derive(Deserialize, Serialize, Clone, Derivative)]
#[serde(untagged)]
#[derivative(Debug)]
pub(crate) enum CheckFieldsPredicateArg {
    #[derivative(Debug = "transparent")]
    String(String),
    #[derivative(Debug = "transparent")]
    VecString(Vec<String>),
    #[derivative(Debug = "transparent")]
    Integer(i64),
    #[derivative(Debug = "transparent")]
    Float(f64),
    #[derivative(Debug = "transparent")]
    Boolean(bool),
}

pub(crate) trait CheckFieldsPredicate:
    std::fmt::Debug + Send + Sync + dyn_clone::DynClone
{
    fn check(&self, e: &Event) -> bool;
}

dyn_clone::clone_trait_object!(CheckFieldsPredicate);

//------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct EqualsPredicate {
    target: String,
    arg: CheckFieldsPredicateArg,
}

impl EqualsPredicate {
    pub(crate) fn new(
        target: String,
        arg: &CheckFieldsPredicateArg,
    ) -> Result<Box<dyn CheckFieldsPredicate>, String> {
        Ok(Box::new(Self {
            target,
            arg: arg.clone(),
        }))
    }

    fn check_field(&self, field: Option<&Value>) -> bool {
        field.map_or(false, |v| match &self.arg {
            CheckFieldsPredicateArg::String(s) => s.as_bytes() == v.coerce_to_bytes(),
            CheckFieldsPredicateArg::VecString(ss) => {
                ss.iter().any(|s| s.as_bytes() == v.coerce_to_bytes())
            }
            CheckFieldsPredicateArg::Integer(i) => match v {
                Value::Integer(vi) => *i == *vi,
                Value::Float(vf) => *i == vf.into_inner() as i64,
                _ => false,
            },
            CheckFieldsPredicateArg::Float(f) => match v {
                Value::Float(vf) => *f == vf.into_inner(),
                Value::Integer(vi) => *f == *vi as f64,
                _ => false,
            },
            CheckFieldsPredicateArg::Boolean(b) => match v {
                Value::Boolean(vb) => *b == *vb,
                _ => false,
            },
        })
    }
}

impl CheckFieldsPredicate for EqualsPredicate {
    fn check(&self, event: &Event) -> bool {
        match event {
            Event::Log(l) => self.check_field(l.get(self.target.as_str())),
            Event::Metric(m) => m
                .tags()
                .and_then(|t| t.get(&self.target))
                .map_or(false, |v| match &self.arg {
                    CheckFieldsPredicateArg::String(s) => s.as_bytes() == v.as_bytes(),
                    _ => false,
                }),
            Event::Trace(t) => self.check_field(t.get(&self.target)),
        }
    }
}

//------------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ContainsPredicate {
    target: String,
    arg: Vec<String>,
}

impl ContainsPredicate {
    pub(crate) fn new(
        target: String,
        arg: &CheckFieldsPredicateArg,
    ) -> Result<Box<dyn CheckFieldsPredicate>, String> {
        match arg {
            CheckFieldsPredicateArg::String(s) => Ok(Box::new(Self {
                target,
                arg: vec![s.clone()],
            })),
            CheckFieldsPredicateArg::VecString(ss) => Ok(Box::new(Self {
                target,
                arg: ss.clone(),
            })),
            _ => Err("contains predicate requires a string or list of string argument".to_owned()),
        }
    }
}

impl CheckFieldsPredicate for ContainsPredicate {
    fn check(&self, event: &Event) -> bool {
        match event {
            Event::Log(l) => l.get(self.target.as_str()).map_or(false, |v| {
                let v = v.to_string_lossy();
                self.arg.iter().any(|s| v.contains(s))
            }),
            _ => false,
        }
    }
}

//------------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct StartsWithPredicate {
    target: String,
    arg: Vec<String>,
}

impl StartsWithPredicate {
    pub(crate) fn new(
        target: String,
        arg: &CheckFieldsPredicateArg,
    ) -> Result<Box<dyn CheckFieldsPredicate>, String> {
        match arg {
            CheckFieldsPredicateArg::String(s) => Ok(Box::new(Self {
                target,
                arg: vec![s.clone()],
            })),
            CheckFieldsPredicateArg::VecString(ss) => Ok(Box::new(Self {
                target,
                arg: ss.clone(),
            })),
            _ => {
                Err("starts_with predicate requires a string or list of string argument".to_owned())
            }
        }
    }
}

impl CheckFieldsPredicate for StartsWithPredicate {
    fn check(&self, event: &Event) -> bool {
        match event {
            Event::Log(l) => l.get(self.target.as_str()).map_or(false, |v| {
                let v = v.to_string_lossy();
                self.arg.iter().any(|s| v.starts_with(s))
            }),
            _ => false,
        }
    }
}

//------------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct EndsWithPredicate {
    target: String,
    arg: Vec<String>,
}

impl EndsWithPredicate {
    pub(crate) fn new(
        target: String,
        arg: &CheckFieldsPredicateArg,
    ) -> Result<Box<dyn CheckFieldsPredicate>, String> {
        match arg {
            CheckFieldsPredicateArg::String(s) => Ok(Box::new(Self {
                target,
                arg: vec![s.clone()],
            })),
            CheckFieldsPredicateArg::VecString(ss) => Ok(Box::new(Self {
                target,
                arg: ss.clone(),
            })),
            _ => Err("ends_with predicate requires a string argument".to_owned()),
        }
    }
}

impl CheckFieldsPredicate for EndsWithPredicate {
    fn check(&self, event: &Event) -> bool {
        match event {
            Event::Log(l) => l.get(self.target.as_str()).map_or(false, |v| {
                let v = v.to_string_lossy();
                self.arg.iter().any(|s| v.ends_with(s))
            }),
            _ => false,
        }
    }
}

//------------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct NotEqualsPredicate {
    target: String,
    arg: Vec<String>,
}

impl NotEqualsPredicate {
    pub(crate) fn new(
        target: String,
        arg: &CheckFieldsPredicateArg,
    ) -> Result<Box<dyn CheckFieldsPredicate>, String> {
        Ok(Box::new(Self {
            target,
            arg: match arg {
                CheckFieldsPredicateArg::String(s) => vec![s.clone()],
                CheckFieldsPredicateArg::VecString(ss) => ss.clone(),
                CheckFieldsPredicateArg::Integer(a) => vec![a.to_string()],
                CheckFieldsPredicateArg::Float(a) => vec![a.to_string()],
                CheckFieldsPredicateArg::Boolean(a) => vec![a.to_string()],
            },
        }))
    }
}

impl CheckFieldsPredicate for NotEqualsPredicate {
    fn check(&self, event: &Event) -> bool {
        match event {
            Event::Log(l) => l
                .get(self.target.as_str())
                .map(|f| f.coerce_to_bytes())
                .map_or(false, |b| {
                    //false if any match, else true
                    !self.arg.iter().any(|s| b == s.as_bytes())
                }),
            Event::Metric(m) => m
                .tags()
                .and_then(|t| t.get(&self.target))
                .map_or(false, |v| {
                    !self.arg.iter().any(|s| v.as_bytes() == s.as_bytes())
                }),
            Event::Trace(t) => {
                t.get(&self.target)
                    .map(|f| f.coerce_to_bytes())
                    .map_or(false, |b| {
                        //false if any match, else true
                        !self.arg.iter().any(|s| b == s.as_bytes())
                    })
            }
        }
    }
}

//------------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct RegexPredicate {
    target: String,
    regex: Regex,
}

impl RegexPredicate {
    pub(crate) fn new(
        target: String,
        arg: &CheckFieldsPredicateArg,
    ) -> Result<Box<dyn CheckFieldsPredicate>, String> {
        let pattern = match arg {
            CheckFieldsPredicateArg::String(s) => s.clone(),
            _ => return Err("regex predicate requires a string argument".to_owned()),
        };
        let regex = Regex::new(&pattern)
            .map_err(|error| format!("Invalid regex \"{}\": {}", pattern, error))?;
        Ok(Box::new(Self { target, regex }))
    }
}

impl CheckFieldsPredicate for RegexPredicate {
    fn check(&self, event: &Event) -> bool {
        match event {
            Event::Log(log) => log
                .get(self.target.as_str())
                .map(|field| field.to_string_lossy())
                .map_or(false, |field| self.regex.is_match(&field)),
            Event::Metric(metric) => metric
                .tags()
                .and_then(|tags| tags.get(&self.target))
                .map_or(false, |field| self.regex.is_match(field)),
            Event::Trace(trace) => trace
                .get(&self.target)
                .map(|field| field.to_string_lossy())
                .map_or(false, |field| self.regex.is_match(&field)),
        }
    }
}

//------------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ExistsPredicate {
    target: String,
    arg: bool,
}

impl ExistsPredicate {
    pub(crate) fn new(
        target: String,
        arg: &CheckFieldsPredicateArg,
    ) -> Result<Box<dyn CheckFieldsPredicate>, String> {
        match arg {
            CheckFieldsPredicateArg::Boolean(b) => Ok(Box::new(Self { target, arg: *b })),
            _ => Err("exists predicate requires a boolean argument".to_owned()),
        }
    }
}

impl CheckFieldsPredicate for ExistsPredicate {
    fn check(&self, event: &Event) -> bool {
        (match event {
            Event::Log(l) => l.get(self.target.as_str()).is_some(),
            Event::Metric(m) => m.tags().map_or(false, |t| t.contains_key(&self.target)),
            Event::Trace(t) => t.get(&self.target).is_some(),
        }) == self.arg
    }
}

//------------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct IpCidrPredicate {
    target: String,
    cidrs: Vec<IpCidr>,
}

impl IpCidrPredicate {
    pub(crate) fn new(
        target: String,
        arg: &CheckFieldsPredicateArg,
    ) -> Result<Box<dyn CheckFieldsPredicate>, String> {
        let cidr_strings = match arg {
            CheckFieldsPredicateArg::String(s) => vec![s.clone()],
            CheckFieldsPredicateArg::VecString(ss) => ss.clone(),
            _ => {
                return Err(
                    "ip_cidr_contains predicate requires a string or list of string argument"
                        .to_owned(),
                )
            }
        };
        let cidrs = match cidr_strings.iter().map(IpCidr::from_str).collect() {
            Ok(v) => v,
            Err(error) => return Err(format!("Invalid IP CIDR: {}", error)),
        };
        Ok(Box::new(Self { target, cidrs }))
    }
}

impl CheckFieldsPredicate for IpCidrPredicate {
    fn check(&self, event: &Event) -> bool {
        match event {
            Event::Log(l) => l.get(self.target.as_str()).map_or(false, |v| {
                let v = v.to_string_lossy();
                IpAddr::from_str(&v).map_or(false, |ip_addr| {
                    self.cidrs.iter().any(|cidr| cidr.contains(ip_addr))
                })
            }),
            _ => false,
        }
    }
}

//------------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct NegatePredicate {
    subpred: Box<dyn CheckFieldsPredicate>,
}

impl NegatePredicate {
    pub(crate) fn new(
        predicate: &str,
        target: String,
        arg: &CheckFieldsPredicateArg,
    ) -> Result<Box<dyn CheckFieldsPredicate>, String> {
        let subpred = build_predicate(predicate, target, arg)?;
        Ok(Box::new(Self { subpred }))
    }
}

impl CheckFieldsPredicate for NegatePredicate {
    fn check(&self, event: &Event) -> bool {
        !self.subpred.check(event)
    }
}

//------------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct LengthEqualsPredicate {
    target: String,
    arg: i64,
}

impl LengthEqualsPredicate {
    pub(crate) fn new(
        target: String,
        arg: &CheckFieldsPredicateArg,
    ) -> Result<Box<dyn CheckFieldsPredicate>, String> {
        match arg {
            CheckFieldsPredicateArg::Integer(i) => {
                if *i < 0 {
                    return Err("length_eq predicate integer cannot be negative".to_owned());
                }

                Ok(Box::new(Self { target, arg: *i }))
            }
            _ => Err("length_eq predicate requires an integer argument".to_owned()),
        }
    }
}

impl CheckFieldsPredicate for LengthEqualsPredicate {
    fn check(&self, event: &Event) -> bool {
        match event {
            Event::Log(l) => l.get(self.target.as_str()).map_or(false, |v| {
                let len = match v {
                    Value::Bytes(value) => value.len(),
                    Value::Array(value) => value.len(),
                    Value::Object(value) => value.len(),
                    Value::Null => 0,
                    value => value.to_string_lossy().len(),
                };

                len as i64 == self.arg
            }),
            _ => false,
        }
    }
}

//------------------------------------------------------------------------------

fn build_predicate(
    predicate: &str,
    target: String,
    arg: &CheckFieldsPredicateArg,
) -> Result<Box<dyn CheckFieldsPredicate>, String> {
    match predicate {
        "eq" | "equals" => EqualsPredicate::new(target, arg),
        "neq" | "not_equals" => NotEqualsPredicate::new(target, arg),
        "contains" => ContainsPredicate::new(target, arg),
        "prefix" => {
            warn!(
                message = "The `prefix` comparison predicate is deprecated, use `starts_with` instead.",
                %target,
            );
            StartsWithPredicate::new(target, arg)
        }
        "starts_with" => StartsWithPredicate::new(target, arg),
        "ends_with" => EndsWithPredicate::new(target, arg),
        "exists" => ExistsPredicate::new(target, arg),
        "regex" => RegexPredicate::new(target, arg),
        "ip_cidr_contains" => IpCidrPredicate::new(target, arg),
        "length_eq" => LengthEqualsPredicate::new(target, arg),
        _ if predicate.starts_with("not_") => NegatePredicate::new(&predicate[4..], target, arg),
        _ => Err(format!("predicate type '{}' not recognized", predicate)),
    }
}

fn build_predicates(
    map: &IndexMap<String, CheckFieldsPredicateArg>,
) -> Result<IndexMap<String, Box<dyn CheckFieldsPredicate>>, Vec<String>> {
    let mut predicates: IndexMap<String, Box<dyn CheckFieldsPredicate>> = IndexMap::new();
    let mut errors = Vec::new();

    for (target_pred, arg) in map {
        match target_pred.rfind('.')
        {
            Some(i) if i > 0 && i < target_pred.len() - 1 => {
                let mut target = target_pred.clone();
                let pred = target.split_off(i + 1);
                target.truncate(target.len() - 1);
                match build_predicate(&pred, target, arg) {
                    Ok(pred) => {
                        predicates.insert(format!("{}: {:?}", target_pred, arg), pred);
                    }
                    Err(err) => errors.push(err),
                }
            }
            _ => errors.push(format!(
                "predicate not found in check_fields value '{target_pred}', format must be <target>.<predicate>"
            )),
        }
    }

    if errors.is_empty() {
        Ok(predicates)
    } else {
        Err(errors)
    }
}

//------------------------------------------------------------------------------

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub(crate) struct CheckFieldsConfig {
    #[serde(flatten, default)]
    predicates: IndexMap<String, CheckFieldsPredicateArg>,
}

inventory::submit! {
    ConditionDescription::new::<CheckFieldsConfig>("check_fields")
}

impl_generate_config_from_default!(CheckFieldsConfig);

#[typetag::serde(name = "check_fields")]
impl ConditionConfig for CheckFieldsConfig {
    fn build(&self, _enrichment_tables: &enrichment::TableRegistry) -> crate::Result<Condition> {
        warn!(message = "The `check_fields` condition is deprecated, use `vrl` instead.",);
        build_predicates(&self.predicates)
            .map(|preds| -> Condition { Condition::CheckFields(CheckFields { predicates: preds }) })
            .map_err(|errs| {
                if errs.len() > 1 {
                    let mut err_fmt = errs.join("\n");
                    err_fmt.insert_str(0, "failed to parse predicates:\n");
                    err_fmt
                } else {
                    errs[0].clone()
                }
                .into()
            })
    }
}

//------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CheckFields {
    predicates: IndexMap<String, Box<dyn CheckFieldsPredicate>>,
}

impl Conditional for CheckFields {
    fn check(&self, e: &Event) -> bool {
        self.predicates.iter().all(|(_, p)| p.check(e))
    }

    fn check_with_context(&self, e: &Event) -> Result<(), String> {
        let failed_preds = self
            .predicates
            .iter()
            .filter(|(_, p)| !p.check(e))
            .map(|(n, _)| n.to_owned())
            .collect::<Vec<_>>();
        if failed_preds.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "predicates failed: [ {} ]",
                failed_preds.join(", ")
            ))
        }
    }
}

//------------------------------------------------------------------------------

#[cfg(test)]
mod test {
    use super::*;
    use crate::event::Event;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<CheckFieldsConfig>();
    }

    #[test]
    fn check_predicate_errors() {
        let cases = vec![
            ("foo", "predicate not found in check_fields value 'foo', format must be <target>.<predicate>"),
            (".nah", "predicate not found in check_fields value '.nah', format must be <target>.<predicate>"),
            ("", "predicate not found in check_fields value '', format must be <target>.<predicate>"),
            ("what.", "predicate not found in check_fields value 'what.', format must be <target>.<predicate>"),
            ("foo.nix_real", "predicate type 'nix_real' not recognized"),
        ];

        let mut aggregated_preds: IndexMap<String, CheckFieldsPredicateArg> = IndexMap::new();
        let mut exp_errs = Vec::new();
        for (pred, exp) in cases {
            aggregated_preds.insert(pred.into(), CheckFieldsPredicateArg::String("foo".into()));
            exp_errs.push(exp);

            let mut preds: IndexMap<String, CheckFieldsPredicateArg> = IndexMap::new();
            preds.insert(pred.into(), CheckFieldsPredicateArg::String("foo".into()));

            assert_eq!(
                CheckFieldsConfig { predicates: preds }
                    .build(&Default::default())
                    .err()
                    .unwrap()
                    .to_string(),
                exp.to_owned()
            );
        }

        let mut exp_err = exp_errs.join("\n");
        exp_err.insert_str(0, "failed to parse predicates:\n");

        assert_eq!(
            CheckFieldsConfig {
                predicates: aggregated_preds
            }
            .build(&Default::default())
            .err()
            .unwrap()
            .to_string(),
            exp_err
        );
    }

    #[test]
    fn check_field_equals() {
        let mut preds: IndexMap<String, CheckFieldsPredicateArg> = IndexMap::new();
        preds.insert(
            "message.equals".into(),
            CheckFieldsPredicateArg::String("foo".into()),
        );
        preds.insert(
            "other_thing.eq".into(),
            CheckFieldsPredicateArg::String("bar".into()),
        );
        preds.insert(
            "third_thing.eq".into(),
            CheckFieldsPredicateArg::VecString(vec!["hello".into(), "world".into()]),
        );

        let cond = CheckFieldsConfig { predicates: preds }
            .build(&Default::default())
            .unwrap();

        let mut event = Event::from("neither");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err(
                "predicates failed: [ message.equals: \"foo\", other_thing.eq: \"bar\", third_thing.eq: [\"hello\", \"world\"] ]"
                    .to_owned()
            )
        );

        event.as_mut_log().insert("message", "foo");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ other_thing.eq: \"bar\", third_thing.eq: [\"hello\", \"world\"] ]".to_owned())
        );

        event.as_mut_log().insert("other_thing", "bar");
        event.as_mut_log().insert("third_thing", "hello");
        assert!(cond.check(&event));
        assert_eq!(cond.check_with_context(&event), Ok(()));

        event.as_mut_log().insert("third_thing", "world");
        assert!(cond.check(&event));
        assert_eq!(cond.check_with_context(&event), Ok(()));

        event.as_mut_log().insert("message", "not foo");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ message.equals: \"foo\" ]".to_owned())
        );
    }

    #[test]
    fn check_field_contains() {
        let mut preds: IndexMap<String, CheckFieldsPredicateArg> = IndexMap::new();
        preds.insert(
            "message.contains".into(),
            CheckFieldsPredicateArg::String("foo".into()),
        );
        preds.insert(
            "other_thing.contains".into(),
            CheckFieldsPredicateArg::String("bar".into()),
        );
        preds.insert(
            "third_thing.contains".into(),
            CheckFieldsPredicateArg::VecString(vec!["hello".into(), "world".into()]),
        );

        let cond = CheckFieldsConfig { predicates: preds }
            .build(&Default::default())
            .unwrap();

        let mut event = Event::from("neither");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err(
                "predicates failed: [ message.contains: \"foo\", other_thing.contains: \"bar\", third_thing.contains: [\"hello\", \"world\"] ]"
                    .to_owned()
            )
        );

        event.as_mut_log().insert("message", "hello foo world");
        event.as_mut_log().insert("third_thing", "hello world");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ other_thing.contains: \"bar\" ]".to_owned())
        );

        event.as_mut_log().insert("other_thing", "hello bar world");
        assert!(cond.check(&event));
        assert_eq!(cond.check_with_context(&event), Ok(()));

        event
            .as_mut_log()
            .insert("third_thing", "not hell0 or w0rld");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ third_thing.contains: [\"hello\", \"world\"] ]".to_owned()),
        );

        event.as_mut_log().insert("third_thing", "world");
        assert!(cond.check(&event));
        assert_eq!(cond.check_with_context(&event), Ok(()));

        event.as_mut_log().insert("message", "not fo0");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ message.contains: \"foo\" ]".to_owned())
        );
    }

    #[test]
    fn check_field_prefix() {
        let mut preds: IndexMap<String, CheckFieldsPredicateArg> = IndexMap::new();
        preds.insert(
            "message.prefix".into(),
            CheckFieldsPredicateArg::String("foo".into()),
        );
        preds.insert(
            "other_thing.prefix".into(),
            CheckFieldsPredicateArg::String("bar".into()),
        );

        let cond = CheckFieldsConfig { predicates: preds }
            .build(&Default::default())
            .unwrap();

        let mut event = Event::from("neither");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err(
                "predicates failed: [ message.prefix: \"foo\", other_thing.prefix: \"bar\" ]"
                    .to_owned()
            )
        );

        event.as_mut_log().insert("message", "foo hello world");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ other_thing.prefix: \"bar\" ]".to_owned())
        );

        event.as_mut_log().insert("other_thing", "bar hello world");
        assert!(cond.check(&event));
        assert_eq!(cond.check_with_context(&event), Ok(()));

        event.as_mut_log().insert("message", "not prefixed");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ message.prefix: \"foo\" ]".to_owned())
        );
    }

    #[test]
    fn check_field_starts_with() {
        let mut preds: IndexMap<String, CheckFieldsPredicateArg> = IndexMap::new();
        preds.insert(
            "message.starts_with".into(),
            CheckFieldsPredicateArg::String("foo".into()),
        );
        preds.insert(
            "other_thing.starts_with".into(),
            CheckFieldsPredicateArg::String("bar".into()),
        );
        preds.insert(
            "third_thing.starts_with".into(),
            CheckFieldsPredicateArg::VecString(vec!["hello".into(), "world".into()]),
        );

        let cond = CheckFieldsConfig { predicates: preds }
            .build(&Default::default())
            .unwrap();

        let mut event = Event::from("neither");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err(
                "predicates failed: [ message.starts_with: \"foo\", other_thing.starts_with: \"bar\", third_thing.starts_with: [\"hello\", \"world\"] ]"
                    .to_owned()
            )
        );

        event.as_mut_log().insert("third_thing", "hello world");
        event.as_mut_log().insert("message", "foo hello world");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ other_thing.starts_with: \"bar\" ]".to_owned())
        );

        event.as_mut_log().insert("other_thing", "bar hello world");
        assert!(cond.check(&event));
        assert_eq!(cond.check_with_context(&event), Ok(()));

        event
            .as_mut_log()
            .insert("third_thing", "wrong hello world");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err(
                "predicates failed: [ third_thing.starts_with: [\"hello\", \"world\"] ]".to_owned()
            ),
        );

        event.as_mut_log().insert("third_thing", "world");
        assert!(cond.check(&event));
        assert_eq!(cond.check_with_context(&event), Ok(()));

        event.as_mut_log().insert("message", "not prefixed");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ message.starts_with: \"foo\" ]".to_owned())
        );
    }

    #[test]
    fn check_field_ends_with() {
        let mut preds: IndexMap<String, CheckFieldsPredicateArg> = IndexMap::new();
        preds.insert(
            "message.ends_with".into(),
            CheckFieldsPredicateArg::String("foo".into()),
        );
        preds.insert(
            "other_thing.ends_with".into(),
            CheckFieldsPredicateArg::String("bar".into()),
        );
        preds.insert(
            "third_thing.ends_with".into(),
            CheckFieldsPredicateArg::VecString(vec!["hello".into(), "world".into()]),
        );

        let cond = CheckFieldsConfig { predicates: preds }
            .build(&Default::default())
            .unwrap();

        let mut event = Event::from("neither");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err(
                "predicates failed: [ message.ends_with: \"foo\", other_thing.ends_with: \"bar\", third_thing.ends_with: [\"hello\", \"world\"] ]"
                    .to_owned()
            )
        );

        event.as_mut_log().insert("message", "hello world foo");
        event.as_mut_log().insert("third_thing", "hello world");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ other_thing.ends_with: \"bar\" ]".to_owned())
        );

        event.as_mut_log().insert("other_thing", "hello world bar");
        assert!(cond.check(&event));
        assert_eq!(cond.check_with_context(&event), Ok(()));

        event.as_mut_log().insert("third_thing", "hello world bad");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ third_thing.ends_with: [\"hello\", \"world\"] ]".to_owned()),
        );

        event.as_mut_log().insert("third_thing", "world hello");
        assert!(cond.check(&event));
        assert_eq!(cond.check_with_context(&event), Ok(()));

        event.as_mut_log().insert("message", "not suffixed");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ message.ends_with: \"foo\" ]".to_owned())
        );
    }

    #[test]
    fn check_field_not_equals() {
        let mut preds: IndexMap<String, CheckFieldsPredicateArg> = IndexMap::new();
        preds.insert(
            "message.not_equals".into(),
            CheckFieldsPredicateArg::String("foo".into()),
        );
        preds.insert(
            "other_thing.neq".into(),
            CheckFieldsPredicateArg::String("bar".into()),
        );
        preds.insert(
            "third_thing.neq".into(),
            CheckFieldsPredicateArg::VecString(vec!["hello".into(), "world".into()]),
        );

        let cond = CheckFieldsConfig { predicates: preds }
            .build(&Default::default())
            .unwrap();

        let mut event = Event::from("not foo");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ other_thing.neq: \"bar\", third_thing.neq: [\"hello\", \"world\"] ]".to_owned())
        );

        event.as_mut_log().insert("other_thing", "not bar");
        event
            .as_mut_log()
            .insert("third_thing", "not hello or world");
        assert!(cond.check(&event));
        assert_eq!(cond.check_with_context(&event), Ok(()));

        event.as_mut_log().insert("third_thing", "world");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ third_thing.neq: [\"hello\", \"world\"] ]".to_owned()),
        );

        event.as_mut_log().insert("third_thing", "hello");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ third_thing.neq: [\"hello\", \"world\"] ]".to_owned()),
        );

        event.as_mut_log().insert("third_thing", "safe");
        event.as_mut_log().insert("other_thing", "bar");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ other_thing.neq: \"bar\" ]".to_owned())
        );

        event.as_mut_log().insert("message", "foo");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err(
                "predicates failed: [ message.not_equals: \"foo\", other_thing.neq: \"bar\" ]"
                    .to_owned()
            )
        );
    }

    #[test]
    fn check_field_regex() {
        let mut preds: IndexMap<String, CheckFieldsPredicateArg> = IndexMap::new();
        preds.insert(
            "message.regex".into(),
            CheckFieldsPredicateArg::String("^start".into()),
        );
        preds.insert(
            "other_thing.regex".into(),
            CheckFieldsPredicateArg::String("end$".into()),
        );

        let cond = CheckFieldsConfig { predicates: preds }
            .build(&Default::default())
            .unwrap();

        let mut event = Event::from("starts with a bang");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err(r#"predicates failed: [ other_thing.regex: "end$" ]"#.to_owned())
        );

        event.as_mut_log().insert("other_thing", "at the end");
        assert!(cond.check(&event));
        assert_eq!(cond.check_with_context(&event), Ok(()));

        event.as_mut_log().insert("other_thing", "end up here");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err(r#"predicates failed: [ other_thing.regex: "end$" ]"#.to_owned())
        );

        event.as_mut_log().insert("message", "foo");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err(
                r#"predicates failed: [ message.regex: "^start", other_thing.regex: "end$" ]"#
                    .to_owned()
            )
        );
    }

    #[test]
    fn check_ip_cidr() {
        let mut preds: IndexMap<String, CheckFieldsPredicateArg> = IndexMap::new();
        preds.insert(
            "foo.ip_cidr_contains".into(),
            CheckFieldsPredicateArg::String("10.0.0.0/8".into()),
        );
        preds.insert(
            "bar.ip_cidr_contains".into(),
            CheckFieldsPredicateArg::VecString(vec!["2000::/3".into(), "192.168.0.0/16".into()]),
        );

        let cond = CheckFieldsConfig { predicates: preds }
            .build(&Default::default())
            .unwrap();

        let mut event = Event::from("ignored message");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ foo.ip_cidr_contains: \"10.0.0.0/8\", bar.ip_cidr_contains: [\"2000::/3\", \"192.168.0.0/16\"] ]".to_owned()),
        );

        event.as_mut_log().insert("foo", "10.1.2.3");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err(
                "predicates failed: [ bar.ip_cidr_contains: [\"2000::/3\", \"192.168.0.0/16\"] ]"
                    .to_owned()
            ),
        );

        event.as_mut_log().insert("bar", "2000::");
        assert!(cond.check(&event));
        assert_eq!(cond.check_with_context(&event), Ok(()));

        event.as_mut_log().insert("bar", "192.168.255.255");
        assert!(cond.check(&event));
        assert_eq!(cond.check_with_context(&event), Ok(()));

        event.as_mut_log().insert("foo", "192.200.200.200");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ foo.ip_cidr_contains: \"10.0.0.0/8\" ]".to_owned()),
        );

        event.as_mut_log().insert("foo", "not an ip");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ foo.ip_cidr_contains: \"10.0.0.0/8\" ]".to_owned()),
        );
    }

    #[test]
    fn check_field_exists() {
        let mut preds: IndexMap<String, CheckFieldsPredicateArg> = IndexMap::new();
        preds.insert("foo.exists".into(), CheckFieldsPredicateArg::Boolean(true));
        preds.insert("bar.exists".into(), CheckFieldsPredicateArg::Boolean(false));

        let cond = CheckFieldsConfig { predicates: preds }
            .build(&Default::default())
            .unwrap();

        let mut event = Event::from("ignored field");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ foo.exists: true ]".to_owned())
        );

        event.as_mut_log().insert("foo", "not ignored");
        assert!(cond.check(&event));
        assert_eq!(cond.check_with_context(&event), Ok(()));

        event.as_mut_log().insert("bar", "also not ignored");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ bar.exists: false ]".to_owned())
        );
    }

    #[test]
    fn check_field_length_eq() {
        let mut preds: IndexMap<String, CheckFieldsPredicateArg> = IndexMap::new();
        preds.insert("foo.length_eq".into(), CheckFieldsPredicateArg::Integer(10));
        preds.insert("bar.length_eq".into(), CheckFieldsPredicateArg::Integer(4));

        let cond = CheckFieldsConfig { predicates: preds }
            .build(&Default::default())
            .unwrap();

        let mut event = Event::from("");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ foo.length_eq: 10, bar.length_eq: 4 ]".to_owned())
        );

        event.as_mut_log().insert("foo", "helloworld");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ bar.length_eq: 4 ]".to_owned())
        );

        event.as_mut_log().insert("bar", vec![0, 1, 2, 3]);
        assert!(cond.check(&event));
        assert_eq!(cond.check_with_context(&event), Ok(()));
    }

    #[test]
    fn negate_predicate() {
        let mut preds: IndexMap<String, CheckFieldsPredicateArg> = IndexMap::new();
        preds.insert(
            "foo.not_exists".into(),
            CheckFieldsPredicateArg::Boolean(true),
        );
        let cond = CheckFieldsConfig { predicates: preds }
            .build(&Default::default())
            .unwrap();

        let mut event = Event::from("ignored field");
        assert!(cond.check(&event));
        assert_eq!(cond.check_with_context(&event), Ok(()));

        event.as_mut_log().insert("foo", "not ignored");
        assert!(!cond.check(&event));
        assert_eq!(
            cond.check_with_context(&event),
            Err("predicates failed: [ foo.not_exists: true ]".into())
        );
    }
}
