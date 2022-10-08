use bytes::Bytes;
use futures::{FutureExt, SinkExt};
use http::{Request, StatusCode, Uri};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    config::{
        log_schema, AcknowledgementsConfig, GenerateConfig, Input, SinkConfig, SinkContext,
        SinkDescription,
    },
    event::{Event, Value},
    http::HttpClient,
    sinks::util::{
        http::{BatchedHttpSink, HttpEventEncoder, HttpSink},
        BatchConfig, BoxedRawValue, JsonArrayBuffer, SinkBatchSettings, TowerRequestConfig,
    },
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct HoneycombConfig {
    #[serde(skip, default = "default_endpoint")]
    endpoint: String,

    api_key: String,

    // TODO: we probably want to make this a template
    // but this limits us in how we can do our healthcheck.
    dataset: String,

    #[serde(default)]
    batch: BatchConfig<HoneycombDefaultBatchSettings>,

    #[serde(default)]
    request: TowerRequestConfig,

    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    acknowledgements: AcknowledgementsConfig,
}

fn default_endpoint() -> String {
    "https://api.honeycomb.io/1/batch".to_string()
}

#[derive(Clone, Copy, Debug, Default)]
struct HoneycombDefaultBatchSettings;

impl SinkBatchSettings for HoneycombDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = None;
    const MAX_BYTES: Option<usize> = Some(100_000);
    const TIMEOUT_SECS: f64 = 1.0;
}

inventory::submit! {
    SinkDescription::new::<HoneycombConfig>("honeycomb")
}

impl GenerateConfig for HoneycombConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"api_key = "${HONEYCOMB_API_KEY}"
            dataset = "my-honeycomb-dataset""#,
        )
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "honeycomb")]
impl SinkConfig for HoneycombConfig {
    async fn build(
        &self,
        cx: SinkContext,
    ) -> crate::Result<(super::VectorSink, super::Healthcheck)> {
        let request_settings = self.request.unwrap_with(&TowerRequestConfig::default());
        let batch_settings = self.batch.into_batch_settings()?;

        let buffer = JsonArrayBuffer::new(batch_settings.size);

        let client = HttpClient::new(None, cx.proxy())?;

        let sink = BatchedHttpSink::new(
            self.clone(),
            buffer,
            request_settings,
            batch_settings.timeout,
            client.clone(),
            cx.acker(),
        )
        .sink_map_err(|error| error!(message = "Fatal honeycomb sink error.", %error));

        let healthcheck = healthcheck(self.clone(), client).boxed();

        Ok((super::VectorSink::from_event_sink(sink), healthcheck))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn sink_type(&self) -> &'static str {
        "honeycomb"
    }

    fn acknowledgements(&self) -> Option<&AcknowledgementsConfig> {
        Some(&self.acknowledgements)
    }
}

pub struct HoneycombEventEncoder;

impl HttpEventEncoder<serde_json::Value> for HoneycombEventEncoder {
    fn encode_event(&mut self, event: Event) -> Option<serde_json::Value> {
        let mut log = event.into_log();

        let timestamp = if let Some(Value::Timestamp(ts)) = log.remove(log_schema().timestamp_key())
        {
            ts
        } else {
            chrono::Utc::now()
        };

        let data = json!({
            "timestamp": timestamp.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            "data": log.all_fields(),
        });

        Some(data)
    }
}

#[async_trait::async_trait]
impl HttpSink for HoneycombConfig {
    type Input = serde_json::Value;
    type Output = Vec<BoxedRawValue>;
    type Encoder = HoneycombEventEncoder;

    fn build_encoder(&self) -> Self::Encoder {
        HoneycombEventEncoder
    }

    async fn build_request(&self, events: Self::Output) -> crate::Result<http::Request<Bytes>> {
        let uri = self.build_uri();
        let request = Request::post(uri).header("X-Honeycomb-Team", self.api_key.clone());
        let body = crate::serde::json::to_bytes(&events).unwrap().freeze();

        request.body(body).map_err(Into::into)
    }
}

impl HoneycombConfig {
    fn build_uri(&self) -> Uri {
        let uri = format!("{}/{}", self.endpoint, self.dataset);

        uri.parse::<http::Uri>()
            .expect("This should be a valid uri")
    }
}

async fn healthcheck(config: HoneycombConfig, client: HttpClient) -> crate::Result<()> {
    let req = config
        .build_request(Vec::new())
        .await?
        .map(hyper::Body::from);

    let res = client.send(req).await?;

    let status = res.status();
    let body = hyper::body::to_bytes(res.into_body()).await?;

    if status == StatusCode::BAD_REQUEST {
        Ok(())
    } else if status == StatusCode::UNAUTHORIZED {
        let json: serde_json::Value = serde_json::from_slice(&body[..])?;

        let message = if let Some(s) = json
            .as_object()
            .and_then(|o| o.get("error"))
            .and_then(|s| s.as_str())
        {
            s.to_string()
        } else {
            "Token is not valid, 401 returned.".to_string()
        };

        Err(message.into())
    } else {
        let body = String::from_utf8_lossy(&body[..]);

        Err(format!(
            "Server returned unexpected error status: {} body: {}",
            status, body
        )
        .into())
    }
}
#[cfg(test)]
mod test {
    use futures::{future::ready, stream};
    use vector_core::event::Event;

    use crate::{
        config::{GenerateConfig, SinkConfig, SinkContext},
        test_util::{
            components::{run_and_assert_sink_compliance, SINK_TAGS},
            http::{always_200_response, spawn_blackhole_http_server},
        },
    };

    use super::HoneycombConfig;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<super::HoneycombConfig>();
    }

    #[tokio::test]
    async fn component_spec_compliance() {
        let mock_endpoint = spawn_blackhole_http_server(always_200_response).await;

        let config = HoneycombConfig::generate_config().to_string();
        let mut config =
            toml::from_str::<HoneycombConfig>(&config).expect("config should be valid");
        config.endpoint = mock_endpoint.to_string();

        let context = SinkContext::new_test();
        let (sink, _healthcheck) = config.build(context).await.unwrap();

        let event = Event::from("simple message");
        run_and_assert_sink_compliance(sink, stream::once(ready(event)), &SINK_TAGS).await;
    }
}
