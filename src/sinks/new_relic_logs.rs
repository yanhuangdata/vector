use http::Uri;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use snafu::Snafu;

use super::util::SinkBatchSettings;
use crate::{
    config::{
        AcknowledgementsConfig, GenerateConfig, Input, SinkConfig, SinkContext, SinkDescription,
    },
    sinks::{
        http::{HttpMethod, HttpSinkConfig},
        util::{
            encoding::{EncodingConfig, EncodingConfigWithDefault},
            http::RequestConfig,
            BatchConfig, Compression, TowerRequestConfig,
        },
    },
};

// New Relic Logs API accepts payloads up to 1MB (10^6 bytes)
const MAX_PAYLOAD_SIZE: usize = 1_000_000_usize;

#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display(
        "Missing authentication key, must provide either 'license_key' or 'insert_key'"
    ))]
    MissingAuthParam,
}

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone, Derivative)]
#[serde(rename_all = "snake_case")]
#[derivative(Default)]
pub enum NewRelicLogsRegion {
    #[derivative(Default)]
    Us,
    Eu,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NewRelicLogsDefaultBatchSettings;

impl SinkBatchSettings for NewRelicLogsDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = None;
    const MAX_BYTES: Option<usize> = Some(1_000_000);
    const TIMEOUT_SECS: f64 = 1.0;
}

#[derive(Deserialize, Serialize, Debug, Derivative, Clone)]
#[derivative(Default)]
pub struct NewRelicLogsConfig {
    pub license_key: Option<String>,
    pub insert_key: Option<String>,
    pub region: Option<NewRelicLogsRegion>,
    #[serde(
        skip_serializing_if = "crate::serde::skip_serializing_if_default",
        default
    )]
    pub encoding: EncodingConfigWithDefault<Encoding>,
    #[serde(default)]
    pub compression: Compression,
    #[serde(default)]
    pub batch: BatchConfig<NewRelicLogsDefaultBatchSettings>,

    #[serde(default)]
    pub request: TowerRequestConfig,
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    acknowledgements: AcknowledgementsConfig,
}

inventory::submit! {
    SinkDescription::new::<NewRelicLogsConfig>("new_relic_logs")
}

impl GenerateConfig for NewRelicLogsConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self::with_encoding(Encoding::Json)).unwrap()
    }
}

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone, Derivative)]
#[serde(rename_all = "snake_case")]
#[derivative(Default)]
pub enum Encoding {
    #[derivative(Default)]
    Json,
}

impl From<Encoding> for crate::sinks::http::Encoding {
    fn from(v: Encoding) -> crate::sinks::http::Encoding {
        match v {
            Encoding::Json => crate::sinks::http::Encoding::Json,
        }
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "new_relic_logs")]
impl SinkConfig for NewRelicLogsConfig {
    async fn build(
        &self,
        cx: SinkContext,
    ) -> crate::Result<(super::VectorSink, super::Healthcheck)> {
        let http_conf = self.create_config()?;
        http_conf.build(cx).await
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn sink_type(&self) -> &'static str {
        "new_relic_logs"
    }

    fn acknowledgements(&self) -> Option<&AcknowledgementsConfig> {
        Some(&self.acknowledgements)
    }
}

impl NewRelicLogsConfig {
    fn with_encoding(encoding: Encoding) -> Self {
        Self {
            license_key: None,
            insert_key: None,
            region: None,
            encoding: encoding.into(),
            compression: Compression::default(),
            batch: BatchConfig::default(),
            request: TowerRequestConfig::default(),
            acknowledgements: Default::default(),
        }
    }

    fn create_config(&self) -> crate::Result<HttpSinkConfig> {
        let mut headers: IndexMap<String, String> = IndexMap::new();
        if let Some(license_key) = &self.license_key {
            headers.insert("X-License-Key".to_owned(), license_key.clone());
        } else if let Some(insert_key) = &self.insert_key {
            headers.insert("X-Insert-Key".to_owned(), insert_key.clone());
        } else {
            return Err(Box::new(BuildError::MissingAuthParam));
        }

        let uri = match self.region.as_ref().unwrap_or(&NewRelicLogsRegion::Us) {
            NewRelicLogsRegion::Us => Uri::from_static("https://log-api.newrelic.com/log/v1"),
            NewRelicLogsRegion::Eu => Uri::from_static("https://log-api.eu.newrelic.com/log/v1"),
        };

        let batch_settings = self.batch.validate()?.limit_max_bytes(MAX_PAYLOAD_SIZE)?;

        let tower = TowerRequestConfig { ..self.request };

        let request = RequestConfig { tower, headers };

        Ok(HttpSinkConfig {
            uri: uri.into(),
            method: Some(HttpMethod::Post),
            auth: None,
            headers: None,
            compression: self.compression,
            encoding: EncodingConfig::<Encoding>::from(self.encoding.clone())
                .into_encoding()
                .into(),
            batch: batch_settings.into(),
            request,
            tls: None,
            acknowledgements: self.acknowledgements,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::BufRead;

    use bytes::Buf;
    use codecs::encoding::Serializer;
    use futures::{stream, StreamExt};
    use hyper::Method;
    use serde_json::Value;

    use super::*;
    use crate::{
        config::SinkConfig,
        event::Event,
        sinks::util::{service::RATE_LIMIT_NUM_DEFAULT, test::build_test_server, Concurrency},
        test_util::{
            components::{run_and_assert_sink_compliance, HTTP_SINK_TAGS},
            next_addr,
        },
    };

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<NewRelicLogsConfig>();
    }

    #[test]
    fn new_relic_logs_check_config_no_auth() {
        assert_eq!(
            format!(
                "{}",
                NewRelicLogsConfig::with_encoding(Encoding::Json)
                    .create_config()
                    .unwrap_err()
            ),
            "Missing authentication key, must provide either 'license_key' or 'insert_key'"
                .to_owned(),
        );
    }

    #[test]
    fn new_relic_logs_check_config_defaults() {
        let mut nr_config = NewRelicLogsConfig::with_encoding(Encoding::Json);
        nr_config.license_key = Some("foo".to_owned());
        let http_config = nr_config.create_config().unwrap();

        assert_eq!(
            http_config.uri.uri.to_string(),
            "https://log-api.newrelic.com/log/v1".to_string()
        );
        assert_eq!(http_config.method, Some(HttpMethod::Post));
        assert!(matches!(
            http_config.encoding.encoding().1,
            Serializer::Json(_)
        ));
        assert_eq!(http_config.batch.max_bytes, Some(MAX_PAYLOAD_SIZE));
        assert_eq!(http_config.request.tower.concurrency, Concurrency::None);
        assert_eq!(
            http_config.request.tower.rate_limit_num,
            Some(RATE_LIMIT_NUM_DEFAULT)
        );
        assert_eq!(
            http_config.request.headers["X-License-Key"],
            "foo".to_owned()
        );
        assert!(http_config.tls.is_none());
        assert!(http_config.auth.is_none());
    }

    #[test]
    fn new_relic_logs_check_config_custom() {
        let mut nr_config = NewRelicLogsConfig::with_encoding(Encoding::Json);
        nr_config.insert_key = Some("foo".to_owned());
        nr_config.region = Some(NewRelicLogsRegion::Eu);
        nr_config.batch.max_bytes = Some(MAX_PAYLOAD_SIZE);
        nr_config.request.concurrency = Concurrency::Fixed(12);
        nr_config.request.rate_limit_num = Some(24);

        let http_config = nr_config.create_config().unwrap();

        assert_eq!(
            http_config.uri.uri.to_string(),
            "https://log-api.eu.newrelic.com/log/v1".to_string()
        );
        assert_eq!(http_config.method, Some(HttpMethod::Post));
        assert!(matches!(
            http_config.encoding.encoding().1,
            Serializer::Json(_)
        ));
        assert_eq!(http_config.batch.max_bytes, Some(MAX_PAYLOAD_SIZE));
        assert_eq!(
            http_config.request.tower.concurrency,
            Concurrency::Fixed(12)
        );
        assert_eq!(http_config.request.tower.rate_limit_num, Some(24));
        assert_eq!(
            http_config.request.headers["X-Insert-Key"],
            "foo".to_owned()
        );
        assert!(http_config.tls.is_none());
        assert!(http_config.auth.is_none());
    }

    #[test]
    fn new_relic_logs_check_config_custom_from_toml() {
        let config = r#"
        insert_key = "foo"
        region = "eu"
        encoding = "json"

        [batch]
        max_bytes = 838860

        [request]
        concurrency = 12
        rate_limit_num = 24
    "#;
        let nr_config: NewRelicLogsConfig = toml::from_str(config).unwrap();

        let http_config = nr_config.create_config().unwrap();

        assert_eq!(
            http_config.uri.uri.to_string(),
            "https://log-api.eu.newrelic.com/log/v1".to_string()
        );
        assert_eq!(http_config.method, Some(HttpMethod::Post));
        assert!(matches!(
            http_config.encoding.encoding().1,
            Serializer::Json(_)
        ));
        assert_eq!(http_config.batch.max_bytes, Some(838860));
        assert_eq!(
            http_config.request.tower.concurrency,
            Concurrency::Fixed(12)
        );
        assert_eq!(http_config.request.tower.rate_limit_num, Some(24));
        assert_eq!(
            http_config.request.headers["X-Insert-Key"],
            "foo".to_owned()
        );
        assert!(http_config.tls.is_none());
        assert!(http_config.auth.is_none());
    }

    #[test]
    #[should_panic]
    fn new_relic_logs_check_config_custom_from_toml_batch_max_size_too_high() {
        let config = r#"
        insert_key = "foo"
        region = "eu"
        encoding = "json"

        [batch]
        max_bytes = 8388600

        [request]
        concurrency = 12
        rate_limit_num = 24
    "#;
        let nr_config: NewRelicLogsConfig = toml::from_str(config).unwrap();

        nr_config.create_config().unwrap();
    }

    #[tokio::test]
    async fn new_relic_logs_happy_path() {
        let in_addr = next_addr();

        let mut nr_config = NewRelicLogsConfig::with_encoding(Encoding::Json);
        nr_config.license_key = Some("foo".to_owned());
        let mut http_config = nr_config.create_config().unwrap();
        http_config.uri = format!("http://{}/fake_nr", in_addr)
            .parse::<http::Uri>()
            .unwrap()
            .into();

        let (sink, _healthcheck) = http_config.build(SinkContext::new_test()).await.unwrap();
        let (rx, trigger, server) = build_test_server(in_addr);
        tokio::spawn(server);

        let input_lines = (0..100).map(|i| format!("msg {}", i)).collect::<Vec<_>>();
        let events = stream::iter(input_lines.clone()).map(Event::from);

        run_and_assert_sink_compliance(sink, events, &HTTP_SINK_TAGS).await;
        drop(trigger);

        let output_lines = rx
            .flat_map(|(parts, body)| {
                assert_eq!(Method::POST, parts.method);
                assert_eq!("/fake_nr", parts.uri.path());
                assert_eq!(
                    parts
                        .headers
                        .get("X-License-Key")
                        .and_then(|v| v.to_str().ok()),
                    Some("foo")
                );
                stream::iter(body.reader().lines())
            })
            .map(Result::unwrap)
            .flat_map(|line| {
                let vals: Vec<Value> = serde_json::from_str(&line).unwrap();
                stream::iter(
                    vals.into_iter()
                        .map(|v| v.get("message").unwrap().as_str().unwrap().to_owned()),
                )
            })
            .collect::<Vec<_>>()
            .await;

        assert_eq!(input_lines, output_lines);
    }
}
