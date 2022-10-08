use std::sync::Arc;

use futures::FutureExt;
use http::Uri;
use indoc::indoc;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tower::ServiceBuilder;
use vector_core::config::{proxy::ProxyConfig, AcknowledgementsConfig};

use super::service::TraceApiRetry;
use crate::{
    common::datadog::get_base_domain,
    config::{GenerateConfig, Input, SinkConfig, SinkContext},
    http::HttpClient,
    sinks::{
        datadog::{
            get_api_validate_endpoint, healthcheck,
            traces::{
                request_builder::DatadogTracesRequestBuilder, service::TraceApiService,
                sink::TracesSink,
            },
        },
        util::{
            service::ServiceBuilderExt, BatchConfig, Compression, Concurrency, SinkBatchSettings,
            TowerRequestConfig,
        },
        Healthcheck, UriParseSnafu, VectorSink,
    },
    tls::{MaybeTlsSettings, TlsEnableableConfig},
};

// The Datadog API has a hard limit of 3.2MB for uncompressed payloads.
// Beyond this limit the payload will be ignored, enforcing a slight lower
// limit as a safety margin.
pub const BATCH_GOAL_BYTES: usize = 3_000_000;
pub const BATCH_MAX_EVENTS: usize = 1_000;
pub const BATCH_DEFAULT_TIMEOUT_SECS: f64 = 10.0;

pub const PAYLOAD_LIMIT: usize = 3_200_000;

const DEFAULT_REQUEST_LIMITS: TowerRequestConfig = TowerRequestConfig::new(Concurrency::None)
    .retry_attempts(5)
    .retry_max_duration_secs(300);

#[derive(Clone, Copy, Debug, Default)]
pub struct DatadogTracesDefaultBatchSettings;

impl SinkBatchSettings for DatadogTracesDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = Some(BATCH_MAX_EVENTS);
    const MAX_BYTES: Option<usize> = Some(BATCH_GOAL_BYTES);
    const TIMEOUT_SECS: f64 = BATCH_DEFAULT_TIMEOUT_SECS;
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct DatadogTracesConfig {
    pub(crate) endpoint: Option<String>,
    site: Option<String>,
    default_api_key: String,

    tls: Option<TlsEnableableConfig>,

    #[serde(default)]
    compression: Option<Compression>,

    #[serde(default)]
    batch: BatchConfig<DatadogTracesDefaultBatchSettings>,

    #[serde(default)]
    request: TowerRequestConfig,

    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    acknowledgements: AcknowledgementsConfig,
}

impl GenerateConfig for DatadogTracesConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(indoc! {r#"
            default_api_key = "${DATADOG_API_KEY_ENV_VAR}"
        "#})
        .unwrap()
    }
}

/// Datadog traces API has two routes: one for traces and another one for stats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DatadogTracesEndpoint {
    Traces,
    APMStats,
}

/// Store traces & APM stats endpoints actual URIs.
pub struct DatadogTracesEndpointConfiguration {
    traces_endpoint: Uri,
    // Unused so far
    stats_endpoint: Uri,
}

impl DatadogTracesEndpointConfiguration {
    pub fn get_uri_for_endpoint(&self, endpoint: DatadogTracesEndpoint) -> Uri {
        match endpoint {
            DatadogTracesEndpoint::Traces => self.traces_endpoint.clone(),
            DatadogTracesEndpoint::APMStats => self.stats_endpoint.clone(),
        }
    }
}

impl DatadogTracesConfig {
    fn generate_traces_endpoint_configuration(
        &self,
    ) -> crate::Result<DatadogTracesEndpointConfiguration> {
        let base_uri = self.endpoint.clone().unwrap_or_else(|| {
            format!(
                "https://trace.agent.{}",
                get_base_domain(self.site.as_ref(), None)
            )
        });

        let traces_endpoint = build_uri(&base_uri, "/api/v0.2/traces")?;
        let stats_endpoint = build_uri(&base_uri, "/api/v0.2/stats")?;

        Ok(DatadogTracesEndpointConfiguration {
            traces_endpoint,
            stats_endpoint,
        })
    }
}

impl DatadogTracesConfig {
    pub fn build_sink(&self, client: HttpClient, cx: SinkContext) -> crate::Result<VectorSink> {
        let default_api_key: Arc<str> = Arc::from(self.default_api_key.clone().as_str());
        let request_limits = self.request.unwrap_with(&DEFAULT_REQUEST_LIMITS);
        let endpoints = self.generate_traces_endpoint_configuration()?;
        let batcher_settings = self
            .batch
            .validate()?
            .limit_max_bytes(BATCH_GOAL_BYTES)?
            .limit_max_events(BATCH_MAX_EVENTS)?
            .into_batcher_settings()?;
        let service = ServiceBuilder::new()
            .settings(request_limits, TraceApiRetry)
            .service(TraceApiService::new(client));
        let request_builder = DatadogTracesRequestBuilder::new(
            Arc::clone(&default_api_key),
            endpoints,
            self.compression.unwrap_or_else(Compression::gzip_default),
            PAYLOAD_LIMIT,
        )?;
        let sink = TracesSink::new(cx, service, request_builder, batcher_settings);
        Ok(VectorSink::from_event_streamsink(sink))
    }

    pub fn build_healthcheck(&self, client: HttpClient) -> crate::Result<Healthcheck> {
        let validate_endpoint =
            get_api_validate_endpoint(self.endpoint.as_ref(), self.site.as_ref(), None)?;
        Ok(healthcheck(client, validate_endpoint, self.default_api_key.clone()).boxed())
    }

    pub fn build_client(&self, proxy: &ProxyConfig) -> crate::Result<HttpClient> {
        let tls_settings = MaybeTlsSettings::from_config(
            &Some(
                self.tls
                    .clone()
                    .unwrap_or_else(TlsEnableableConfig::enabled),
            ),
            false,
        )?;
        Ok(HttpClient::new(tls_settings, proxy)?)
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "datadog_traces")]
impl SinkConfig for DatadogTracesConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let client = self.build_client(&cx.proxy)?;
        let healthcheck = self.build_healthcheck(client.clone())?;
        let sink = self.build_sink(client, cx)?;
        Ok((sink, healthcheck))
    }

    fn input(&self) -> Input {
        // Metric will be accepted as soon as APM stats will be handled
        Input::trace()
    }

    fn sink_type(&self) -> &'static str {
        "datadog_traces"
    }

    fn acknowledgements(&self) -> Option<&AcknowledgementsConfig> {
        Some(&self.acknowledgements)
    }
}

fn build_uri(host: &str, endpoint: &str) -> crate::Result<Uri> {
    let result = format!("{}{}", host, endpoint)
        .parse::<Uri>()
        .context(UriParseSnafu)?;
    Ok(result)
}

#[cfg(test)]
mod test {
    use crate::sinks::datadog::traces::DatadogTracesConfig;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<DatadogTracesConfig>();
    }
}
