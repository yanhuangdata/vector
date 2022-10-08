use std::net::Ipv4Addr;

use ::value::Value;
use vrl::prelude::*;

fn ip_aton(value: Value) -> Resolved {
    let ip: Ipv4Addr = value
        .try_bytes_utf8_lossy()?
        .parse()
        .map_err(|err| format!("unable to parse IPv4 address: {}", err))?;
    Ok(u32::from(ip).into())
}

#[derive(Clone, Copy, Debug)]
pub struct IpAton;

impl Function for IpAton {
    fn identifier(&self) -> &'static str {
        "ip_aton"
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[Parameter {
            keyword: "value",
            kind: kind::BYTES,
            required: true,
        }]
    }

    fn examples(&self) -> &'static [Example] {
        &[Example {
            title: "Example",
            source: r#"ip_aton!("1.2.3.4")"#,
            result: Ok("16909060"),
        }]
    }

    fn compile(
        &self,
        _state: (&mut state::LocalEnv, &mut state::ExternalEnv),
        _ctx: &mut FunctionCompileContext,
        mut arguments: ArgumentList,
    ) -> Compiled {
        let value = arguments.required("value");

        Ok(Box::new(IpAtonFn { value }))
    }

    fn call_by_vm(&self, _ctx: &mut Context, args: &mut VmArgumentList) -> Resolved {
        let value = args.required("value");
        ip_aton(value)
    }
}

#[derive(Debug, Clone)]
struct IpAtonFn {
    value: Box<dyn Expression>,
}

impl Expression for IpAtonFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let value = self.value.resolve(ctx)?;
        ip_aton(value)
    }

    fn type_def(&self, _: (&state::LocalEnv, &state::ExternalEnv)) -> TypeDef {
        TypeDef::integer().fallible()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    test_function![
        ip_aton => IpAton;

        invalid {
            args: func_args![value: "i am not an ipaddress"],
            want: Err("unable to parse IPv4 address: invalid IP address syntax"),
            tdef: TypeDef::integer().fallible(),
        }

        valid {
            args: func_args![value: "1.2.3.4"],
            want: Ok(value!(16909060)),
            tdef: TypeDef::integer().fallible(),
        }
    ];
}
