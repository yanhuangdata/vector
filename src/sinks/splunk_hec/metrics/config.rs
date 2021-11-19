use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use tower::ServiceBuilder;
use vector_core::transform::DataType;

use crate::config::{GenerateConfig, SinkConfig, SinkContext};
use crate::http::HttpClient;
use crate::sinks::splunk_hec::common::acknowledgements::HecClientAcknowledgementsConfig;
use crate::sinks::splunk_hec::common::retry::HecRetryLogic;
use crate::sinks::splunk_hec::common::service::{HecService, HttpRequestBuilder};
use crate::sinks::splunk_hec::common::{
    build_healthcheck, create_client, host_key, SplunkHecDefaultBatchSettings,
};
use crate::sinks::splunk_hec::metrics::request_builder::HecMetricsRequestBuilder;
use crate::sinks::splunk_hec::metrics::sink::HecMetricsSink;
use crate::sinks::util::{BatchConfig, Compression, ServiceBuilderExt, TowerRequestConfig};
use crate::sinks::Healthcheck;
use crate::template::Template;
use crate::tls::TlsOptions;
use vector_core::sink::VectorSink;

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct HecMetricsSinkConfig {
    pub default_namespace: Option<String>,
    pub token: String,
    pub endpoint: String,
    #[serde(default = "crate::sinks::splunk_hec::common::host_key")]
    pub host_key: String,
    pub index: Option<Template>,
    pub sourcetype: Option<Template>,
    pub source: Option<Template>,
    #[serde(default)]
    pub compression: Compression,
    #[serde(default)]
    pub batch: BatchConfig<SplunkHecDefaultBatchSettings>,
    #[serde(default)]
    pub request: TowerRequestConfig,
    pub tls: Option<TlsOptions>,
    pub indexer_acknowledgements: HecClientAcknowledgementsConfig,
}

impl GenerateConfig for HecMetricsSinkConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            default_namespace: None,
            token: "${VECTOR_SPLUNK_HEC_TOKEN}".to_owned(),
            endpoint: "http://localhost:8088".to_owned(),
            host_key: host_key(),
            index: None,
            sourcetype: None,
            source: None,
            compression: Compression::default(),
            batch: BatchConfig::default(),
            request: TowerRequestConfig::default(),
            tls: None,
            indexer_acknowledgements: HecClientAcknowledgementsConfig::default(),
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "splunk_hec_metrics")]
impl SinkConfig for HecMetricsSinkConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let client = create_client(&self.tls, cx.proxy())?;
        let healthcheck =
            build_healthcheck(self.endpoint.clone(), self.token.clone(), client.clone()).boxed();
        let sink = self.build_processor(client, cx)?;
        Ok((sink, healthcheck))
    }

    fn input_type(&self) -> DataType {
        DataType::Metric
    }

    fn sink_type(&self) -> &'static str {
        "splunk_hec_metrics"
    }
}

impl HecMetricsSinkConfig {
    pub fn build_processor(
        &self,
        client: HttpClient,
        cx: SinkContext,
    ) -> crate::Result<VectorSink> {
        let request_builder = HecMetricsRequestBuilder {
            compression: self.compression,
        };

        let request_settings = self.request.unwrap_with(&TowerRequestConfig::default());
        let http_request_builder = HttpRequestBuilder {
            endpoint: self.endpoint.clone(),
            token: self.token.clone(),
            compression: self.compression,
        };
        let service = ServiceBuilder::new()
            .settings(request_settings, HecRetryLogic)
            .service(HecService::new(
                client,
                http_request_builder,
                self.indexer_acknowledgements.clone(),
            ));

        let batch_settings = self.batch.into_batcher_settings()?;
        let sink = HecMetricsSink {
            context: cx,
            service,
            batch_settings,
            request_builder,
            sourcetype: self.sourcetype.clone(),
            source: self.source.clone(),
            index: self.index.clone(),
            host: self.host_key.clone(),
            default_namespace: self.default_namespace.clone(),
        };

        Ok(VectorSink::Stream(Box::new(sink)))
    }
}
