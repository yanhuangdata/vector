use bytes::{BufMut, Bytes, BytesMut};
use futures::{FutureExt, SinkExt};
use http::{Request, StatusCode, Uri};
use hyper::Body;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::{
    config::{AcknowledgementsConfig, Input, SinkConfig, SinkContext, SinkDescription},
    event::Event,
    http::{Auth, HttpClient, HttpError, MaybeAuth},
    sinks::util::{
        encoding::{EncodingConfigWithDefault, EncodingConfiguration},
        http::{BatchedHttpSink, HttpEventEncoder, HttpRetryLogic, HttpSink},
        retries::{RetryAction, RetryLogic},
        BatchConfig, Buffer, Compression, RealtimeSizeBasedDefaultBatchSettings,
        TowerRequestConfig, UriSerde,
    },
    tls::{TlsConfig, TlsSettings},
};

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct ClickhouseConfig {
    // Deprecated name
    #[serde(alias = "host")]
    pub endpoint: UriSerde,
    pub table: String,
    pub database: Option<String>,
    #[serde(default)]
    pub skip_unknown_fields: bool,
    #[serde(default = "Compression::gzip_default")]
    pub compression: Compression,
    #[serde(
        skip_serializing_if = "crate::serde::skip_serializing_if_default",
        default
    )]
    pub encoding: EncodingConfigWithDefault<Encoding>,
    #[serde(default)]
    pub batch: BatchConfig<RealtimeSizeBasedDefaultBatchSettings>,
    pub auth: Option<Auth>,
    #[serde(default)]
    pub request: TowerRequestConfig,
    pub tls: Option<TlsConfig>,
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::skip_serializing_if_default"
    )]
    acknowledgements: AcknowledgementsConfig,
}

inventory::submit! {
    SinkDescription::new::<ClickhouseConfig>("clickhouse")
}

impl_generate_config_from_default!(ClickhouseConfig);

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone, Derivative)]
#[serde(rename_all = "snake_case")]
#[derivative(Default)]
pub enum Encoding {
    #[derivative(Default)]
    Default,
}

#[async_trait::async_trait]
#[typetag::serde(name = "clickhouse")]
impl SinkConfig for ClickhouseConfig {
    async fn build(
        &self,
        cx: SinkContext,
    ) -> crate::Result<(super::VectorSink, super::Healthcheck)> {
        let batch = self.batch.into_batch_settings()?;
        let request = self.request.unwrap_with(&TowerRequestConfig::default());
        let tls_settings = TlsSettings::from_options(&self.tls)?;
        let client = HttpClient::new(tls_settings, &cx.proxy)?;

        let config = ClickhouseConfig {
            auth: self.auth.choose_one(&self.endpoint.auth)?,
            ..self.clone()
        };

        let sink = BatchedHttpSink::with_logic(
            config.clone(),
            Buffer::new(batch.size, self.compression),
            ClickhouseRetryLogic::default(),
            request,
            batch.timeout,
            client.clone(),
            cx.acker(),
        )
        .sink_map_err(|error| error!(message = "Fatal clickhouse sink error.", %error));

        let healthcheck = healthcheck(client, config).boxed();

        Ok((super::VectorSink::from_event_sink(sink), healthcheck))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn sink_type(&self) -> &'static str {
        "clickhouse"
    }

    fn acknowledgements(&self) -> Option<&AcknowledgementsConfig> {
        Some(&self.acknowledgements)
    }
}

pub struct ClickhouseEventEncoder {
    encoding: EncodingConfigWithDefault<Encoding>,
}

impl HttpEventEncoder<BytesMut> for ClickhouseEventEncoder {
    fn encode_event(&mut self, mut event: Event) -> Option<BytesMut> {
        self.encoding.apply_rules(&mut event);
        let log = event.into_log();

        let mut body = crate::serde::json::to_bytes(&log).expect("Events should be valid json!");
        body.put_u8(b'\n');

        Some(body)
    }
}

#[async_trait::async_trait]
impl HttpSink for ClickhouseConfig {
    type Input = BytesMut;
    type Output = BytesMut;
    type Encoder = ClickhouseEventEncoder;

    fn build_encoder(&self) -> Self::Encoder {
        ClickhouseEventEncoder {
            encoding: self.encoding.clone(),
        }
    }

    async fn build_request(&self, events: Self::Output) -> crate::Result<http::Request<Bytes>> {
        let database = if let Some(database) = &self.database {
            database.as_str()
        } else {
            "default"
        };

        let uri = set_uri_query(
            &self.endpoint.uri,
            database,
            &self.table,
            self.skip_unknown_fields,
        )
        .expect("Unable to encode uri");

        let mut builder = Request::post(&uri).header("Content-Type", "application/x-ndjson");

        if let Some(ce) = self.compression.content_encoding() {
            builder = builder.header("Content-Encoding", ce);
        }

        let mut request = builder.body(events.freeze()).unwrap();

        if let Some(auth) = &self.auth {
            auth.apply(&mut request);
        }

        Ok(request)
    }
}

async fn healthcheck(client: HttpClient, config: ClickhouseConfig) -> crate::Result<()> {
    // TODO: check if table exists?
    let uri = format!("{}/?query=SELECT%201", config.endpoint);
    let mut request = Request::get(uri).body(Body::empty()).unwrap();

    if let Some(auth) = &config.auth {
        auth.apply(&mut request);
    }

    let response = client.send(request).await?;

    match response.status() {
        StatusCode::OK => Ok(()),
        status => Err(super::HealthcheckError::UnexpectedStatus { status }.into()),
    }
}

fn set_uri_query(uri: &Uri, database: &str, table: &str, skip_unknown: bool) -> crate::Result<Uri> {
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair(
            "query",
            format!(
                "INSERT INTO \"{}\".\"{}\" FORMAT JSONEachRow",
                database,
                table.replace('\"', "\\\"")
            )
            .as_str(),
        )
        .finish();

    let mut uri = uri.to_string();
    if !uri.ends_with('/') {
        uri.push('/');
    }
    uri.push_str("?input_format_import_nested_json=1&");
    if skip_unknown {
        uri.push_str("input_format_skip_unknown_fields=1&");
    }
    uri.push_str(query.as_str());

    uri.parse::<Uri>()
        .context(super::UriParseSnafu)
        .map_err(Into::into)
}

#[derive(Debug, Default, Clone)]
struct ClickhouseRetryLogic {
    inner: HttpRetryLogic,
}

impl RetryLogic for ClickhouseRetryLogic {
    type Error = HttpError;
    type Response = http::Response<Bytes>;

    fn is_retriable_error(&self, error: &Self::Error) -> bool {
        self.inner.is_retriable_error(error)
    }

    fn should_retry_response(&self, response: &Self::Response) -> RetryAction {
        match response.status() {
            StatusCode::INTERNAL_SERVER_ERROR => {
                let body = response.body();

                // Currently, clickhouse returns 500's incorrect data and type mismatch errors.
                // This attempts to check if the body starts with `Code: {code_num}` and to not
                // retry those errors.
                //
                // Reference: https://github.com/vectordotdev/vector/pull/693#issuecomment-517332654
                // Error code definitions: https://github.com/ClickHouse/ClickHouse/blob/master/dbms/src/Common/ErrorCodes.cpp
                //
                // Fix already merged: https://github.com/ClickHouse/ClickHouse/pull/6271
                if body.starts_with(b"Code: 117") {
                    RetryAction::DontRetry("incorrect data".into())
                } else if body.starts_with(b"Code: 53") {
                    RetryAction::DontRetry("type mismatch".into())
                } else {
                    RetryAction::Retry(String::from_utf8_lossy(body).to_string().into())
                }
            }
            _ => self.inner.should_retry_response(response),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<ClickhouseConfig>();
    }

    #[test]
    fn encode_valid() {
        let uri = set_uri_query(
            &"http://localhost:80".parse().unwrap(),
            "my_database",
            "my_table",
            false,
        )
        .unwrap();
        assert_eq!(uri.to_string(), "http://localhost:80/?input_format_import_nested_json=1&query=INSERT+INTO+%22my_database%22.%22my_table%22+FORMAT+JSONEachRow");

        let uri = set_uri_query(
            &"http://localhost:80".parse().unwrap(),
            "my_database",
            "my_\"table\"",
            false,
        )
        .unwrap();
        assert_eq!(uri.to_string(), "http://localhost:80/?input_format_import_nested_json=1&query=INSERT+INTO+%22my_database%22.%22my_%5C%22table%5C%22%22+FORMAT+JSONEachRow");
    }

    #[test]
    fn encode_invalid() {
        set_uri_query(
            &"localhost:80".parse().unwrap(),
            "my_database",
            "my_table",
            false,
        )
        .unwrap_err();
    }
}

#[cfg(test)]
#[cfg(feature = "clickhouse-integration-tests")]
mod integration_tests {
    use std::{
        convert::Infallible,
        net::SocketAddr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
    };

    use futures::{
        future::{ok, ready},
        stream,
    };
    use serde_json::Value;
    use tokio::time::{timeout, Duration};
    use vector_core::event::{BatchNotifier, BatchStatus, BatchStatusReceiver, Event, LogEvent};
    use warp::Filter;

    use super::*;
    use crate::{
        config::{log_schema, SinkConfig, SinkContext},
        sinks::util::encoding::TimestampFormat,
        test_util::{
            components::{run_and_assert_sink_compliance, HTTP_SINK_TAGS},
            random_string, trace_init,
        },
    };

    fn clickhouse_address() -> String {
        std::env::var("CLICKHOUSE_ADDRESS").unwrap_or_else(|_| "http://localhost:8123".into())
    }

    #[tokio::test]
    async fn insert_events() {
        trace_init();

        let table = gen_table();
        let host = clickhouse_address();

        let mut batch = BatchConfig::default();
        batch.max_events = Some(1);

        let config = ClickhouseConfig {
            endpoint: host.parse().unwrap(),
            table: table.clone(),
            compression: Compression::None,
            batch,
            request: TowerRequestConfig {
                retry_attempts: Some(1),
                ..Default::default()
            },
            ..Default::default()
        };

        let client = ClickhouseClient::new(host);
        client
            .create_table(
                &table,
                "host String, timestamp String, message String, items Array(String)",
            )
            .await;

        let (sink, _hc) = config.build(SinkContext::new_test()).await.unwrap();

        let (mut input_event, mut receiver) = make_event();
        input_event
            .as_mut_log()
            .insert("items", vec!["item1", "item2"]);

        run_and_assert_sink_compliance(
            sink,
            stream::once(ready(input_event.clone())),
            &HTTP_SINK_TAGS,
        )
        .await;

        let output = client.select_all(&table).await;
        assert_eq!(1, output.rows);

        let expected = serde_json::to_value(input_event.into_log()).unwrap();
        assert_eq!(expected, output.data[0]);

        assert_eq!(receiver.try_recv(), Ok(BatchStatus::Delivered));
    }

    #[tokio::test]
    async fn skip_unknown_fields() {
        trace_init();

        let table = gen_table();
        let host = clickhouse_address();

        let mut batch = BatchConfig::default();
        batch.max_events = Some(1);

        let config = ClickhouseConfig {
            endpoint: host.parse().unwrap(),
            table: table.clone(),
            skip_unknown_fields: true,
            compression: Compression::None,
            batch,
            request: TowerRequestConfig {
                retry_attempts: Some(1),
                ..Default::default()
            },
            ..Default::default()
        };

        let client = ClickhouseClient::new(host);
        client
            .create_table(&table, "host String, timestamp String, message String")
            .await;

        let (sink, _hc) = config.build(SinkContext::new_test()).await.unwrap();

        let (mut input_event, mut receiver) = make_event();
        input_event.as_mut_log().insert("unknown", "mysteries");

        run_and_assert_sink_compliance(
            sink,
            stream::once(ready(input_event.clone())),
            &HTTP_SINK_TAGS,
        )
        .await;

        let output = client.select_all(&table).await;
        assert_eq!(1, output.rows);

        input_event.as_mut_log().remove("unknown");
        let expected = serde_json::to_value(input_event.into_log()).unwrap();
        assert_eq!(expected, output.data[0]);

        assert_eq!(receiver.try_recv(), Ok(BatchStatus::Delivered));
    }

    #[tokio::test]
    async fn insert_events_unix_timestamps() {
        trace_init();

        let table = gen_table();
        let host = clickhouse_address();
        let encoding = EncodingConfigWithDefault {
            timestamp_format: Some(TimestampFormat::Unix),
            ..Default::default()
        };

        let mut batch = BatchConfig::default();
        batch.max_events = Some(1);

        let config = ClickhouseConfig {
            endpoint: host.parse().unwrap(),
            table: table.clone(),
            compression: Compression::None,
            encoding,
            batch,
            request: TowerRequestConfig {
                retry_attempts: Some(1),
                ..Default::default()
            },
            ..Default::default()
        };

        let client = ClickhouseClient::new(host);
        client
            .create_table(
                &table,
                "host String, timestamp DateTime('UTC'), message String",
            )
            .await;

        let (sink, _hc) = config.build(SinkContext::new_test()).await.unwrap();

        let (mut input_event, _receiver) = make_event();

        run_and_assert_sink_compliance(
            sink,
            stream::once(ready(input_event.clone())),
            &HTTP_SINK_TAGS,
        )
        .await;

        let output = client.select_all(&table).await;
        assert_eq!(1, output.rows);

        let exp_event = input_event.as_mut_log();
        exp_event.insert(
            log_schema().timestamp_key(),
            format!(
                "{}",
                exp_event
                    .get(log_schema().timestamp_key())
                    .unwrap()
                    .as_timestamp()
                    .unwrap()
                    .format("%Y-%m-%d %H:%M:%S")
            ),
        );

        let expected = serde_json::to_value(exp_event).unwrap();
        assert_eq!(expected, output.data[0]);
    }

    #[tokio::test]
    async fn insert_events_unix_timestamps_toml_config() {
        trace_init();

        let table = gen_table();
        let host = clickhouse_address();

        let config: ClickhouseConfig = toml::from_str(&format!(
            r#"
host = "{}"
table = "{}"
compression = "none"
[request]
retry_attempts = 1
[batch]
max_events = 1
[encoding]
timestamp_format = "unix""#,
            host, table
        ))
        .unwrap();

        let client = ClickhouseClient::new(host);
        client
            .create_table(
                &table,
                "host String, timestamp DateTime('UTC'), message String",
            )
            .await;

        let (sink, _hc) = config.build(SinkContext::new_test()).await.unwrap();

        let (mut input_event, _receiver) = make_event();

        run_and_assert_sink_compliance(
            sink,
            stream::once(ready(input_event.clone())),
            &HTTP_SINK_TAGS,
        )
        .await;

        let output = client.select_all(&table).await;
        assert_eq!(1, output.rows);

        let exp_event = input_event.as_mut_log();
        exp_event.insert(
            log_schema().timestamp_key(),
            format!(
                "{}",
                exp_event
                    .get(log_schema().timestamp_key())
                    .unwrap()
                    .as_timestamp()
                    .unwrap()
                    .format("%Y-%m-%d %H:%M:%S")
            ),
        );

        let expected = serde_json::to_value(exp_event).unwrap();
        assert_eq!(expected, output.data[0]);
    }

    #[tokio::test]
    async fn no_retry_on_incorrect_data() {
        trace_init();

        let table = gen_table();
        let host = clickhouse_address();

        let mut batch = BatchConfig::default();
        batch.max_events = Some(1);

        let config = ClickhouseConfig {
            endpoint: host.parse().unwrap(),
            table: table.clone(),
            compression: Compression::None,
            batch,
            ..Default::default()
        };

        let client = ClickhouseClient::new(host);
        // the event contains a message field, but its being omitted to
        // fail the request.
        client
            .create_table(&table, "host String, timestamp String")
            .await;

        let (sink, _hc) = config.build(SinkContext::new_test()).await.unwrap();

        let (input_event, mut receiver) = make_event();

        // Retries should go on forever, so if we are retrying incorrectly
        // this timeout should trigger.
        timeout(Duration::from_secs(5), sink.run_events(vec![input_event]))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(receiver.try_recv(), Ok(BatchStatus::Rejected));
    }

    #[tokio::test]
    async fn no_retry_on_incorrect_data_warp() {
        trace_init();

        let visited = Arc::new(AtomicBool::new(false));
        let routes = warp::any().and_then(move || {
            assert!(!visited.load(Ordering::SeqCst), "Should not retry request.");
            visited.store(true, Ordering::SeqCst);

            ok::<_, Infallible>(warp::reply::with_status(
                "Code: 117",
                StatusCode::INTERNAL_SERVER_ERROR,
            ))
        });
        let server = warp::serve(routes).bind("0.0.0.0:8124".parse::<SocketAddr>().unwrap());
        tokio::spawn(server);

        let host = String::from("http://localhost:8124");

        let mut batch = BatchConfig::default();
        batch.max_events = Some(1);

        let config = ClickhouseConfig {
            endpoint: host.parse().unwrap(),
            table: gen_table(),
            batch,
            ..Default::default()
        };
        let (sink, _hc) = config.build(SinkContext::new_test()).await.unwrap();

        let (input_event, mut receiver) = make_event();

        // Retries should go on forever, so if we are retrying incorrectly
        // this timeout should trigger.
        timeout(Duration::from_secs(5), sink.run_events(vec![input_event]))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(receiver.try_recv(), Ok(BatchStatus::Errored));
    }

    fn make_event() -> (Event, BatchStatusReceiver) {
        let (batch, receiver) = BatchNotifier::new_with_receiver();
        let mut event = LogEvent::from("raw log line").with_batch_notifier(&batch);
        event.insert("host", "example.com");
        (event.into(), receiver)
    }

    struct ClickhouseClient {
        host: String,
        client: reqwest::Client,
    }

    impl ClickhouseClient {
        fn new(host: String) -> Self {
            ClickhouseClient {
                host,
                client: reqwest::Client::new(),
            }
        }

        async fn create_table(&self, table: &str, schema: &str) {
            let response = self
                .client
                .post(&self.host)
                //
                .body(format!(
                    "CREATE TABLE {}
                     ({})
                     ENGINE = MergeTree()
                     ORDER BY (host, timestamp);",
                    table, schema
                ))
                .send()
                .await
                .unwrap();

            if !response.status().is_success() {
                panic!("create table failed: {}", response.text().await.unwrap())
            }
        }

        async fn select_all(&self, table: &str) -> QueryResponse {
            let response = self
                .client
                .post(&self.host)
                .body(format!("SELECT * FROM {} FORMAT JSON", table))
                .send()
                .await
                .unwrap();

            if !response.status().is_success() {
                panic!("select all failed: {}", response.text().await.unwrap())
            } else {
                let text = response.text().await.unwrap();
                match serde_json::from_str(&text) {
                    Ok(value) => value,
                    Err(_) => panic!("json failed: {:?}", text),
                }
            }
        }
    }

    #[derive(Debug, Deserialize)]
    #[allow(dead_code)] // deserialize all fields
    struct QueryResponse {
        data: Vec<Value>,
        meta: Vec<Value>,
        rows: usize,
        statistics: Stats,
    }

    #[derive(Debug, Deserialize)]
    #[allow(dead_code)] // deserialize all fields
    struct Stats {
        bytes_read: usize,
        elapsed: f64,
        rows_read: usize,
    }

    fn gen_table() -> String {
        format!("test_{}", random_string(10).to_lowercase())
    }
}
