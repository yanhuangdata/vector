/// Create a boxed [`Expression`][crate::Expression] trait object from a given `Value`.
///
/// Supports the same format as the [`value`] macro.
#[macro_export]
macro_rules! expr {
    ($($v:tt)*) => {{
        let value = $crate::value!($($v)*);
        $crate::value::VrlValueConvert::into_expression(value)
    }};
}

#[macro_export]
macro_rules! test_type_def {
    ($($name:ident { expr: $expr:expr, want: $def:expr, })+) => {
        mod type_def {
            use super::*;

            $(
                #[test]
                fn $name() {
                    let mut local = $crate::state::LocalEnv::default();
                    let mut external = $crate::state::ExternalEnv::default();
                    let expression = Box::new($expr((&mut local, &mut external)));

                    assert_eq!(expression.type_def((&local, &external)), $def);
                }
            )+
        }
    };
}

#[macro_export]
macro_rules! func_args {
    () => (
        ::std::collections::HashMap::<&'static str, ::value::Value>::default()
    );
    ($($k:tt: $v:expr),+ $(,)?) => {
        vec![$((stringify!($k), $v.into())),+]
            .into_iter()
            .collect::<::std::collections::HashMap<&'static str, ::value::Value>>()
    };
}

#[macro_export]
macro_rules! bench_function {
    ($name:tt => $func:path; $($case:ident { args: $args:expr, want: $(Ok($ok:expr))? $(Err($err:expr))? $(,)* })+) => {
        fn $name(c: &mut criterion::Criterion) {
            let mut group = c.benchmark_group(&format!("vrl_stdlib/functions/{}", stringify!($name)));
            group.throughput(criterion::Throughput::Elements(1));
            $(
                group.bench_function(&stringify!($case).to_string(), |b| {
                    let mut local = $crate::state::LocalEnv::default();
                    let mut external = $crate::state::ExternalEnv::default();

                    let (expression, want) = $crate::__prep_bench_or_test!($func, (&mut local, &mut external), $args, $(Ok(::value::Value::from($ok)))? $(Err($err.to_owned()))?);
                    let expression = expression.unwrap();
                    let mut runtime_state = $crate::state::Runtime::default();
                    let mut target: ::value::Value = ::std::collections::BTreeMap::default().into();
                    let tz = vector_common::TimeZone::Named(chrono_tz::Tz::UTC);
                    let mut ctx = $crate::Context::new(&mut target, &mut runtime_state, &tz);

                    b.iter(|| {
                        let got = expression.resolve(&mut ctx).map_err(|e| e.to_string());
                        debug_assert_eq!(got, want);
                        got
                    })
                });
            )+
        }
    };
}

#[macro_export]
macro_rules! test_function {

    ($name:tt => $func:path; $($case:ident { args: $args:expr, want: $(Ok($ok:expr))? $(Err($err:expr))?, tdef: $tdef:expr,  $(,)* })+) => {
        test_function!($name => $func; before_each => {} $($case { args: $args, want: $(Ok($ok))? $(Err($err))?, tdef: $tdef, tz: vector_common::TimeZone::Named(chrono_tz::Tz::UTC), })+);
    };

    ($name:tt => $func:path; $($case:ident { args: $args:expr, want: $(Ok($ok:expr))? $(Err($err:expr))?, tdef: $tdef:expr, tz: $tz:expr,  $(,)* })+) => {
        test_function!($name => $func; before_each => {} $($case { args: $args, want: $(Ok($ok))? $(Err($err))?, tdef: $tdef, tz: $tz, })+);
    };

    ($name:tt => $func:path; before_each => $before:block $($case:ident { args: $args:expr, want: $(Ok($ok:expr))? $(Err($err:expr))?, tdef: $tdef:expr,  $(,)* })+) => {
        test_function!($name => $func; before_each => $before $($case { args: $args, want: $(Ok($ok))? $(Err($err))?, tdef: $tdef, tz: vector_common::TimeZone::Named(chrono_tz::Tz::UTC), })+);
    };

    ($name:tt => $func:path; before_each => $before:block $($case:ident { args: $args:expr, want: $(Ok($ok:expr))? $(Err($err:expr))?, tdef: $tdef:expr, tz: $tz:expr,  $(,)* })+) => {
        $crate::paste!{$(
            #[test]
            fn [<$name _ $case:snake:lower>]() {
                $before
                let mut local = $crate::state::LocalEnv::default();
                let mut external = $crate::state::ExternalEnv::default();

                let (expression, want) = $crate::__prep_bench_or_test!($func, (&mut local, &mut external), $args, $(Ok(::value::Value::from($ok)))? $(Err($err.to_owned()))?);
                match expression {
                    Ok(expression) => {
                        let mut runtime_state = $crate::state::Runtime::default();
                        let mut target: ::value::Value = ::std::collections::BTreeMap::default().into();
                        let tz = $tz;
                        let mut ctx = $crate::Context::new(&mut target, &mut runtime_state, &tz);

                        let got_value = expression.resolve(&mut ctx)
                            .map_err(|e| format!("{:#}", anyhow::anyhow!(e)));

                        assert_eq!(got_value, want);
                        let got_tdef = expression.type_def((&local, &external));

                        assert_eq!(got_tdef, $tdef);
                    }
                    err@Err(_) => {
                        // Allow tests against compiler errors.
                        assert_eq!(err
                                   // We have to map to a value just to make sure the types match even though
                                   // it will never be used.
                                   .map(|_| ::value::Value::Null)
                                   .map_err(|e| format!("{:#}", e.message())), want);
                    }
                }

                // Test the VM response.
                let mut args: $crate::function::ArgumentList = $args.into();
                let anys = $crate::vm::function_compile_arguments(&$func, &args);
                match anys {
                    Ok(mut anys) => {
                        let mut args = $crate::vm::compile_arguments(&$func, &mut args, &anys);
                        let mut runtime_state = $crate::state::Runtime::default();
                        let mut target: ::value::Value = ::std::collections::BTreeMap::default().into();
                        let tz = $tz;
                        let mut ctx = $crate::Context::new(&mut target, &mut runtime_state, &tz);
                        let got_value_vm = $func.call_by_vm(&mut ctx, &mut args)
                            .map_err(|e| format!("{:#}", anyhow::anyhow!(e)));

                        // Don't assert results against unimplemented functions.
                        // This needs to be removed when we implement all the functions.
                        if got_value_vm != Err("unimplemented".to_string()) {
                            assert_eq!(got_value_vm, want);
                        }
                    }
                    err@Err(_) => {
                        // Allow tests against compiler errors.
                        assert_eq!(err
                                   // We have to map to a value just to make sure the types match even though
                                   // it will never be used.
                                   .map(|_| ::value::Value::Null)
                                   .map_err(|e| format!("{:#}", e.message())), want);
                    }
                }
            }
        )+}
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __prep_bench_or_test {
    ($func:path, $state:expr, $args:expr, $want:expr) => {{
        (
            $func.compile(
                $state,
                &mut $crate::function::FunctionCompileContext::new(vrl::diagnostic::Span::new(
                    0, 0,
                )),
                $args.into(),
            ),
            $want,
        )
    }};
}

#[macro_export]
macro_rules! type_def {
    (unknown) => {
        TypeDef::any()
    };

    (bytes) => {
        TypeDef::bytes()
    };

    (object {$(unknown => $unknown:expr,)? $($key:literal => $value:expr,)+ }) => {{
        let mut v = value::kind::Collection::from(::std::collections::BTreeMap::from([$(($key.into(), $value.into()),)+]));
        $(v.set_unknown(value::Kind::from($unknown)))?;

        TypeDef::object(v)
    }};

    (array [ $($value:expr,)+ ]) => {{
        $(let v = value::kind::Collection::from_unknown(value::Kind::from($value));)+

        TypeDef::array(v)
    }};

    (array { $(unknown => $unknown:expr,)? $($idx:literal => $value:expr,)+ }) => {{
        let mut v = value::kind::Collection::from(::std::collections::BTreeMap::from([$(($idx.into(), $value.into()),)+]));
        $(v.set_unknown(value::Kind::from($unknown)))?;

        TypeDef::array(v)
    }};

    (array) => {
        TypeDef::array(value::kind::Collection::any())
    };
}
