use std::collections::{BTreeMap, HashMap, HashSet};

use bytes::{Bytes, BytesMut};
use futures::SinkExt;
use http::{Request, Uri};
use indoc::indoc;
use serde::{Deserialize, Serialize};

use crate::{
    config::{
        log_schema, AcknowledgementsConfig, GenerateConfig, Input, SinkConfig, SinkContext,
        SinkDescription,
    },
    event::{Event, Value},
    http::HttpClient,
    sinks::{
        influxdb::{
            encode_timestamp, healthcheck, influx_line_protocol, influxdb_settings, Field,
            InfluxDb1Settings, InfluxDb2Settings, ProtocolVersion,
        },
        util::{
            encoding::Transformer,
            http::{BatchedHttpSink, HttpEventEncoder, HttpSink},
            BatchConfig, Buffer, Compression, SinkBatchSettings, TowerRequestConfig,
        },
        Healthcheck, VectorSink,
    },
    tls::{TlsConfig, TlsSettings},
};

#[derive(Clone, Copy, Debug, Default)]
pub struct InfluxDbLogsDefaultBatchSettings;

impl SinkBatchSettings for InfluxDbLogsDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = None;
    const MAX_BYTES: Option<usize> = Some(1_000_000);
    const TIMEOUT_SECS: f64 = 1.0;
}

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct InfluxDbLogsConfig {
    pub namespace: Option<String>,
    pub measurement: Option<String>,
    pub endpoint: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(flatten)]
    pub influxdb1_settings: Option<InfluxDb1Settings>,
    #[serde(flatten)]
    pub influxdb2_settings: Option<InfluxDb2Settings>,
    #[serde(
        skip_serializing_if = "crate::serde::skip_serializing_if_default",
        default
    )]
    pub encoding: Transformer,
    #[serde(default)]
    pub batch: BatchConfig<InfluxDbLogsDefaultBatchSettings>,
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

#[derive(Debug)]
struct InfluxDbLogsSink {
    uri: Uri,
    token: String,
    protocol_version: ProtocolVersion,
    measurement: String,
    tags: HashSet<String>,
    transformer: Transformer,
}

inventory::submit! {
    SinkDescription::new::<InfluxDbLogsConfig>("influxdb_logs")
}

impl GenerateConfig for InfluxDbLogsConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(indoc! {r#"
            endpoint = "http://localhost:8086/"
            namespace = "my-namespace"
            tags = []
            org = "my-org"
            bucket = "my-bucket"
            token = "${INFLUXDB_TOKEN}"
        "#})
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "influxdb_logs")]
impl SinkConfig for InfluxDbLogsConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let measurement = self.get_measurement()?;
        let mut tags: HashSet<String> = self.tags.clone().into_iter().collect();
        tags.insert(log_schema().host_key().to_string());
        tags.insert(log_schema().source_type_key().to_string());
        tags.insert("metric_type".to_string());

        let tls_settings = TlsSettings::from_options(&self.tls)?;
        let client = HttpClient::new(tls_settings, cx.proxy())?;
        let healthcheck = self.healthcheck(client.clone())?;

        let batch = self.batch.into_batch_settings()?;
        let request = self.request.unwrap_with(&TowerRequestConfig {
            retry_attempts: Some(5),
            ..Default::default()
        });

        let settings = influxdb_settings(
            self.influxdb1_settings.clone(),
            self.influxdb2_settings.clone(),
        )
        .unwrap();

        let endpoint = self.endpoint.clone();
        let uri = settings.write_uri(endpoint).unwrap();

        let token = settings.token();
        let protocol_version = settings.protocol_version();

        let sink = InfluxDbLogsSink {
            uri,
            token,
            protocol_version,
            measurement,
            tags,
            transformer: self.encoding.clone(),
        };

        let sink = BatchedHttpSink::new(
            sink,
            Buffer::new(batch.size, Compression::None),
            request,
            batch.timeout,
            client,
            cx.acker(),
        )
        .sink_map_err(|error| error!(message = "Fatal influxdb_logs sink error.", %error));

        Ok((VectorSink::from_event_sink(sink), healthcheck))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn sink_type(&self) -> &'static str {
        "influxdb_logs"
    }

    fn acknowledgements(&self) -> Option<&AcknowledgementsConfig> {
        Some(&self.acknowledgements)
    }
}

struct InfluxDbLogsEncoder {
    protocol_version: ProtocolVersion,
    measurement: String,
    tags: HashSet<String>,
    transformer: Transformer,
}

impl HttpEventEncoder<BytesMut> for InfluxDbLogsEncoder {
    fn encode_event(&mut self, event: Event) -> Option<BytesMut> {
        let mut log = event.into_log();
        log.insert("metric_type", "logs".to_string());
        let mut log = {
            let mut event = Event::from(log);
            self.transformer.transform(&mut event);
            event.into_log()
        };

        // Timestamp
        let timestamp = encode_timestamp(match log.remove(log_schema().timestamp_key()) {
            Some(Value::Timestamp(ts)) => Some(ts),
            _ => None,
        });

        // Tags + Fields
        let mut tags: BTreeMap<String, String> = BTreeMap::new();
        let mut fields: HashMap<String, Field> = HashMap::new();
        log.all_fields().for_each(|(key, value)| {
            if self.tags.contains(&key) {
                tags.insert(key, value.to_string_lossy());
            } else {
                fields.insert(key, to_field(value));
            }
        });

        let mut output = BytesMut::new();
        if let Err(error) = influx_line_protocol(
            self.protocol_version,
            &self.measurement,
            Some(tags),
            Some(fields),
            timestamp,
            &mut output,
        ) {
            warn!(message = "Failed to encode event; dropping event.", %error, internal_log_rate_secs = 30);
            return None;
        };

        Some(output)
    }
}

#[async_trait::async_trait]
impl HttpSink for InfluxDbLogsSink {
    type Input = BytesMut;
    type Output = BytesMut;
    type Encoder = InfluxDbLogsEncoder;

    fn build_encoder(&self) -> Self::Encoder {
        InfluxDbLogsEncoder {
            protocol_version: self.protocol_version,
            measurement: self.measurement.clone(),
            tags: self.tags.clone(),
            transformer: self.transformer.clone(),
        }
    }

    async fn build_request(&self, events: Self::Output) -> crate::Result<Request<Bytes>> {
        Request::post(&self.uri)
            .header("Content-Type", "text/plain")
            .header("Authorization", format!("Token {}", &self.token))
            .body(events.freeze())
            .map_err(Into::into)
    }
}

impl InfluxDbLogsConfig {
    fn get_measurement(&self) -> Result<String, &'static str> {
        match (self.measurement.as_ref(), self.namespace.as_ref()) {
            (Some(measure), Some(_)) => {
                warn!("Option `namespace` has been superseded by `measurement`.");
                Ok(measure.clone())
            }
            (Some(measure), None) => Ok(measure.clone()),
            (None, Some(namespace)) => {
                warn!(
                    "Option `namespace` has been deprecated. Use `measurement` instead. \
                       For example, you can use `measurement=<namespace>.vector` for the \
                       same effect."
                );
                Ok(format!("{}.vector", namespace))
            }
            (None, None) => Err("The `measurement` option is required."),
        }
    }

    fn healthcheck(&self, client: HttpClient) -> crate::Result<Healthcheck> {
        let config = self.clone();

        let healthcheck = healthcheck(
            config.endpoint,
            config.influxdb1_settings,
            config.influxdb2_settings,
            client,
        )?;

        Ok(healthcheck)
    }
}

fn to_field(value: &Value) -> Field {
    match value {
        Value::Integer(num) => Field::Int(*num),
        Value::Float(num) => Field::Float(num.into_inner()),
        Value::Boolean(b) => Field::Bool(*b),
        _ => Field::String(value.to_string_lossy()),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{offset::TimeZone, Utc};
    use futures::{channel::mpsc, StreamExt};
    use http::{request::Parts, StatusCode};
    use indoc::indoc;
    use vector_core::event::{BatchNotifier, BatchStatus, Event, LogEvent};

    use super::*;
    use crate::{
        sinks::{
            influxdb::test_util::{assert_fields, split_line_protocol, ts},
            util::test::{build_test_server_status, load_sink},
        },
        test_util::{components, components::HTTP_SINK_TAGS, next_addr},
    };

    type Receiver = mpsc::Receiver<(Parts, bytes::Bytes)>;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<InfluxDbLogsConfig>();
    }

    #[test]
    fn test_config_without_tags() {
        let config = indoc! {r#"
            namespace = "vector-logs"
            endpoint = "http://localhost:9999"
            bucket = "my-bucket"
            org = "my-org"
            token = "my-token"
        "#};

        toml::from_str::<InfluxDbLogsConfig>(config).unwrap();
    }

    #[test]
    fn test_config_measurement_from_namespace() {
        let config = indoc! {r#"
            namespace = "ns"
            endpoint = "http://localhost:9999"
        "#};

        let sink_config = toml::from_str::<InfluxDbLogsConfig>(config).unwrap();
        assert_eq!("ns.vector", sink_config.get_measurement().unwrap());
    }

    #[test]
    fn test_encode_event_apply_rules() {
        let mut event = Event::from("hello");
        event.as_mut_log().insert("host", "aws.cloud.eur");
        event.as_mut_log().insert("timestamp", ts());

        let mut sink = create_sink(
            "http://localhost:9999",
            "my-token",
            ProtocolVersion::V1,
            "vector",
            ["metric_type", "host"].to_vec(),
        );
        sink.transformer
            .set_except_fields(Some(vec!["host".into()]))
            .unwrap();
        let mut encoder = sink.build_encoder();

        let bytes = encoder.encode_event(event.clone()).unwrap();
        let string = std::str::from_utf8(&bytes).unwrap();

        let line_protocol = split_line_protocol(string);
        assert_eq!("vector", line_protocol.0);
        assert_eq!("metric_type=logs", line_protocol.1);
        assert_fields(line_protocol.2.to_string(), ["message=\"hello\""].to_vec());
        assert_eq!("1542182950000000011\n", line_protocol.3);

        sink.transformer
            .set_except_fields(Some(vec!["metric_type".into()]))
            .unwrap();
        let mut encoder = sink.build_encoder();
        let bytes = encoder.encode_event(event.clone()).unwrap();
        let string = std::str::from_utf8(&bytes).unwrap();
        let line_protocol = split_line_protocol(string);
        assert_eq!(
            "host=aws.cloud.eur", line_protocol.1,
            "metric_type tag should be excluded"
        );
        assert_fields(line_protocol.2, ["message=\"hello\""].to_vec());
    }

    #[test]
    fn test_encode_event_v1() {
        let mut event = Event::from("hello");
        event.as_mut_log().insert("host", "aws.cloud.eur");
        event.as_mut_log().insert("source_type", "file");

        event.as_mut_log().insert("int", 4i32);
        event.as_mut_log().insert("float", 5.5);
        event.as_mut_log().insert("bool", true);
        event.as_mut_log().insert("string", "thisisastring");
        event.as_mut_log().insert("timestamp", ts());

        let sink = create_sink(
            "http://localhost:9999",
            "my-token",
            ProtocolVersion::V1,
            "vector",
            ["source_type", "host", "metric_type"].to_vec(),
        );
        let mut encoder = sink.build_encoder();

        let bytes = encoder.encode_event(event).unwrap();
        let string = std::str::from_utf8(&bytes).unwrap();

        let line_protocol = split_line_protocol(string);
        assert_eq!("vector", line_protocol.0);
        assert_eq!(
            "host=aws.cloud.eur,metric_type=logs,source_type=file",
            line_protocol.1
        );
        assert_fields(
            line_protocol.2.to_string(),
            [
                "int=4i",
                "float=5.5",
                "bool=true",
                "string=\"thisisastring\"",
                "message=\"hello\"",
            ]
            .to_vec(),
        );

        assert_eq!("1542182950000000011\n", line_protocol.3);
    }

    #[test]
    fn test_encode_event() {
        let mut event = Event::from("hello");
        event.as_mut_log().insert("host", "aws.cloud.eur");
        event.as_mut_log().insert("source_type", "file");

        event.as_mut_log().insert("int", 4i32);
        event.as_mut_log().insert("float", 5.5);
        event.as_mut_log().insert("bool", true);
        event.as_mut_log().insert("string", "thisisastring");
        event.as_mut_log().insert("timestamp", ts());

        let sink = create_sink(
            "http://localhost:9999",
            "my-token",
            ProtocolVersion::V2,
            "vector",
            ["source_type", "host", "metric_type"].to_vec(),
        );
        let mut encoder = sink.build_encoder();

        let bytes = encoder.encode_event(event).unwrap();
        let string = std::str::from_utf8(&bytes).unwrap();

        let line_protocol = split_line_protocol(string);
        assert_eq!("vector", line_protocol.0);
        assert_eq!(
            "host=aws.cloud.eur,metric_type=logs,source_type=file",
            line_protocol.1
        );
        assert_fields(
            line_protocol.2.to_string(),
            [
                "int=4i",
                "float=5.5",
                "bool=true",
                "string=\"thisisastring\"",
                "message=\"hello\"",
            ]
            .to_vec(),
        );

        assert_eq!("1542182950000000011\n", line_protocol.3);
    }

    #[test]
    fn test_encode_event_without_tags() {
        let mut event = Event::from("hello");

        event.as_mut_log().insert("value", 100);
        event.as_mut_log().insert("timestamp", ts());

        let sink = create_sink(
            "http://localhost:9999",
            "my-token",
            ProtocolVersion::V2,
            "vector",
            ["metric_type"].to_vec(),
        );
        let mut encoder = sink.build_encoder();

        let bytes = encoder.encode_event(event).unwrap();
        let string = std::str::from_utf8(&bytes).unwrap();

        let line_protocol = split_line_protocol(string);
        assert_eq!("vector", line_protocol.0);
        assert_eq!("metric_type=logs", line_protocol.1);
        assert_fields(
            line_protocol.2.to_string(),
            ["value=100i", "message=\"hello\""].to_vec(),
        );

        assert_eq!("1542182950000000011\n", line_protocol.3);
    }

    #[test]
    fn test_encode_nested_fields() {
        let mut event = Event::new_empty_log();

        event.as_mut_log().insert("a", 1);
        event.as_mut_log().insert("nested.field", "2");
        event.as_mut_log().insert("nested.bool", true);
        event
            .as_mut_log()
            .insert("nested.array[0]", "example-value");
        event
            .as_mut_log()
            .insert("nested.array[2]", "another-value");
        event.as_mut_log().insert("nested.array[3]", 15);

        let sink = create_sink(
            "http://localhost:9999",
            "my-token",
            ProtocolVersion::V2,
            "vector",
            ["metric_type"].to_vec(),
        );
        let mut encoder = sink.build_encoder();

        let bytes = encoder.encode_event(event).unwrap();
        let string = std::str::from_utf8(&bytes).unwrap();

        let line_protocol = split_line_protocol(string);
        assert_eq!("vector", line_protocol.0);
        assert_eq!("metric_type=logs", line_protocol.1);
        assert_fields(
            line_protocol.2,
            [
                "a=1i",
                "nested.array[0]=\"example-value\"",
                "nested.array[1]=\"<null>\"",
                "nested.array[2]=\"another-value\"",
                "nested.array[3]=15i",
                "nested.bool=true",
                "nested.field=\"2\"",
            ]
            .to_vec(),
        );
    }

    #[test]
    fn test_add_tag() {
        let mut event = Event::from("hello");
        event.as_mut_log().insert("source_type", "file");

        event.as_mut_log().insert("as_a_tag", 10);
        event.as_mut_log().insert("timestamp", ts());

        let sink = create_sink(
            "http://localhost:9999",
            "my-token",
            ProtocolVersion::V2,
            "vector",
            ["as_a_tag", "not_exists_field", "source_type", "metric_type"].to_vec(),
        );
        let mut encoder = sink.build_encoder();

        let bytes = encoder.encode_event(event).unwrap();
        let string = std::str::from_utf8(&bytes).unwrap();

        let line_protocol = split_line_protocol(string);
        assert_eq!("vector", line_protocol.0);
        assert_eq!(
            "as_a_tag=10,metric_type=logs,source_type=file",
            line_protocol.1
        );
        assert_fields(line_protocol.2.to_string(), ["message=\"hello\""].to_vec());

        assert_eq!("1542182950000000011\n", line_protocol.3);
    }

    #[tokio::test]
    async fn smoke_v1() {
        let rx = smoke_test(
            r#"database = "my-database""#,
            StatusCode::OK,
            BatchStatus::Delivered,
        )
        .await;

        let query = receive_response(rx).await;
        assert!(query.contains("db=my-database"));
        assert!(query.contains("precision=ns"));
    }

    #[tokio::test]
    async fn smoke_v1_failure() {
        smoke_test(
            r#"database = "my-database""#,
            StatusCode::BAD_REQUEST,
            BatchStatus::Rejected,
        )
        .await;
    }

    #[tokio::test]
    async fn smoke_v2() {
        let rx = smoke_test(
            indoc! {r#"
            bucket = "my-bucket"
            org = "my-org"
            token = "my-token"
        "#},
            StatusCode::OK,
            BatchStatus::Delivered,
        )
        .await;

        let query = receive_response(rx).await;
        assert!(query.contains("org=my-org"));
        assert!(query.contains("bucket=my-bucket"));
        assert!(query.contains("precision=ns"));
    }

    #[tokio::test]
    async fn smoke_v2_failure() {
        smoke_test(
            indoc! {r#"
            bucket = "my-bucket"
            org = "my-org"
            token = "my-token"
        "#},
            StatusCode::BAD_REQUEST,
            BatchStatus::Rejected,
        )
        .await;
    }

    async fn smoke_test(
        config: &str,
        status_code: StatusCode,
        batch_status: BatchStatus,
    ) -> Receiver {
        let config = format!(
            indoc! {r#"
            measurement = "vector"
            endpoint = "http://localhost:9999"
            {}
        "#},
            config
        );
        let (mut config, cx) = load_sink::<InfluxDbLogsConfig>(&config).unwrap();

        // Make sure we can build the config
        let _ = config.build(cx.clone()).await.unwrap();

        let addr = next_addr();
        // Swap out the host so we can force send it
        // to our local server
        let host = format!("http://{}", addr);
        config.endpoint = host;

        let (sink, _) = config.build(cx).await.unwrap();

        let (rx, _trigger, server) = build_test_server_status(addr, status_code);
        tokio::spawn(server);

        let (batch, mut receiver) = BatchNotifier::new_with_receiver();

        let lines = std::iter::repeat(())
            .map(move |_| "message_value")
            .take(5)
            .collect::<Vec<_>>();
        let mut events = Vec::new();

        // Create 5 events with custom field
        for (i, line) in lines.iter().enumerate() {
            let mut event = LogEvent::from(line.to_string()).with_batch_notifier(&batch);
            event.insert(format!("key{}", i).as_str(), format!("value{}", i));

            let timestamp = Utc.ymd(1970, 1, 1).and_hms_nano(0, 0, (i as u32) + 1, 0);
            event.insert("timestamp", timestamp);
            event.insert("source_type", "file");

            events.push(Event::Log(event));
        }
        drop(batch);

        components::init_test();
        sink.run_events(events).await.unwrap();
        if batch_status == BatchStatus::Delivered {
            components::SINK_TESTS.assert(&HTTP_SINK_TAGS);
        }

        assert_eq!(receiver.try_recv(), Ok(batch_status));

        rx
    }

    async fn receive_response(mut rx: Receiver) -> String {
        let output = rx.next().await.unwrap();

        let request = &output.0;
        let query = request.uri.query().unwrap();

        let body = std::str::from_utf8(&output.1[..]).unwrap();
        let mut lines = body.lines();

        assert_eq!(5, lines.clone().count());
        assert_line_protocol(0, lines.next());

        query.into()
    }

    fn assert_line_protocol(i: i64, value: Option<&str>) {
        //vector,metric_type=logs key0="value0",message="message_value" 1000000000
        let line_protocol = split_line_protocol(value.unwrap());
        assert_eq!("vector", line_protocol.0);
        assert_eq!("metric_type=logs,source_type=file", line_protocol.1);
        assert_fields(
            line_protocol.2.to_string(),
            [
                &*format!("key{}=\"value{}\"", i, i),
                "message=\"message_value\"",
            ]
            .to_vec(),
        );

        assert_eq!(((i + 1) * 1000000000).to_string(), line_protocol.3);
    }

    fn create_sink(
        uri: &str,
        token: &str,
        protocol_version: ProtocolVersion,
        measurement: &str,
        tags: Vec<&str>,
    ) -> InfluxDbLogsSink {
        let uri = uri.parse::<Uri>().unwrap();
        let token = token.to_string();
        let measurement = measurement.to_string();
        let tags: HashSet<String> = tags.into_iter().map(|tag| tag.to_string()).collect();
        InfluxDbLogsSink {
            uri,
            token,
            protocol_version,
            measurement,
            tags,
            transformer: Default::default(),
        }
    }
}

#[cfg(feature = "influxdb-integration-tests")]
#[cfg(test)]
mod integration_tests {
    use chrono::Utc;
    use futures::stream;
    use vector_core::event::{BatchNotifier, BatchStatus, Event, LogEvent};

    use super::*;
    use crate::{
        config::SinkContext,
        sinks::influxdb::{
            logs::InfluxDbLogsConfig,
            test_util::{address_v2, onboarding_v2, BUCKET, ORG, TOKEN},
            InfluxDb2Settings,
        },
        test_util::components::{run_and_assert_sink_compliance, HTTP_SINK_TAGS},
    };

    #[tokio::test]
    async fn influxdb2_logs_put_data() {
        let endpoint = address_v2();
        onboarding_v2(&endpoint).await;

        let measure = format!("vector-{}", Utc::now().timestamp_nanos());

        let cx = SinkContext::new_test();

        let config = InfluxDbLogsConfig {
            namespace: None,
            measurement: Some(measure.clone()),
            endpoint: endpoint.clone(),
            tags: Default::default(),
            influxdb1_settings: None,
            influxdb2_settings: Some(InfluxDb2Settings {
                org: ORG.to_string(),
                bucket: BUCKET.to_string(),
                token: TOKEN.to_string(),
            }),
            encoding: Default::default(),
            batch: Default::default(),
            request: Default::default(),
            tls: None,
            acknowledgements: Default::default(),
        };

        let (sink, _) = config.build(cx).await.unwrap();

        let (batch, mut receiver) = BatchNotifier::new_with_receiver();

        let mut event1 = LogEvent::from("message_1").with_batch_notifier(&batch);
        event1.insert("host", "aws.cloud.eur");
        event1.insert("source_type", "file");

        let mut event2 = LogEvent::from("message_2").with_batch_notifier(&batch);
        event2.insert("host", "aws.cloud.eur");
        event2.insert("source_type", "file");

        drop(batch);

        let events = vec![Event::Log(event1), Event::Log(event2)];

        run_and_assert_sink_compliance(sink, stream::iter(events), &HTTP_SINK_TAGS).await;

        assert_eq!(receiver.try_recv(), Ok(BatchStatus::Delivered));

        let mut body = std::collections::HashMap::new();
        body.insert("query", format!("from(bucket:\"my-bucket\") |> range(start: 0) |> filter(fn: (r) => r._measurement == \"{}\")", measure.clone()));
        body.insert("type", "flux".to_owned());

        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();

        let res = client
            .post(format!("{}/api/v2/query?org=my-org", endpoint))
            .json(&body)
            .header("accept", "application/json")
            .header("Authorization", "Token my-token")
            .send()
            .await
            .unwrap();
        let string = res.text().await.unwrap();

        let lines = string.split('\n').collect::<Vec<&str>>();
        let header = lines[0].split(',').collect::<Vec<&str>>();
        let record1 = lines[1].split(',').collect::<Vec<&str>>();
        let record2 = lines[2].split(',').collect::<Vec<&str>>();

        // measurement
        assert_eq!(
            record1[header
                .iter()
                .position(|&r| r.trim() == "_measurement")
                .unwrap()]
            .trim(),
            measure.clone()
        );
        assert_eq!(
            record2[header
                .iter()
                .position(|&r| r.trim() == "_measurement")
                .unwrap()]
            .trim(),
            measure.clone()
        );

        // tags
        assert_eq!(
            record1[header
                .iter()
                .position(|&r| r.trim() == "metric_type")
                .unwrap()]
            .trim(),
            "logs"
        );
        assert_eq!(
            record2[header
                .iter()
                .position(|&r| r.trim() == "metric_type")
                .unwrap()]
            .trim(),
            "logs"
        );
        assert_eq!(
            record1[header.iter().position(|&r| r.trim() == "host").unwrap()].trim(),
            "aws.cloud.eur"
        );
        assert_eq!(
            record2[header.iter().position(|&r| r.trim() == "host").unwrap()].trim(),
            "aws.cloud.eur"
        );
        assert_eq!(
            record1[header
                .iter()
                .position(|&r| r.trim() == "source_type")
                .unwrap()]
            .trim(),
            "file"
        );
        assert_eq!(
            record2[header
                .iter()
                .position(|&r| r.trim() == "source_type")
                .unwrap()]
            .trim(),
            "file"
        );

        // field
        assert_eq!(
            record1[header.iter().position(|&r| r.trim() == "_field").unwrap()].trim(),
            "message"
        );
        assert_eq!(
            record2[header.iter().position(|&r| r.trim() == "_field").unwrap()].trim(),
            "message"
        );
        assert_eq!(
            record1[header.iter().position(|&r| r.trim() == "_value").unwrap()].trim(),
            "message_1"
        );
        assert_eq!(
            record2[header.iter().position(|&r| r.trim() == "_value").unwrap()].trim(),
            "message_2"
        );
    }
}
