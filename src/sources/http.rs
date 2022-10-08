use std::{collections::HashMap, net::SocketAddr};

use bytes::{Bytes, BytesMut};
use chrono::Utc;
use codecs::{
    decoding::{DeserializerConfig, FramingConfig},
    BytesDecoderConfig, BytesDeserializerConfig, JsonDeserializerConfig,
    NewlineDelimitedDecoderConfig,
};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use tokio_util::codec::Decoder as _;
use warp::http::{HeaderMap, HeaderValue};

use crate::{
    codecs::{Decoder, DecodingConfig},
    config::{
        log_schema, AcknowledgementsConfig, DataType, GenerateConfig, Output, Resource,
        SourceConfig, SourceContext, SourceDescription,
    },
    event::{Event, Value},
    serde::{bool_or_struct, default_decoding},
    sources::util::{
        add_query_parameters, Encoding, ErrorMessage, HttpSource, HttpSourceAuthConfig,
    },
    tls::TlsEnableableConfig,
};

#[derive(Clone, Copy, Debug, Derivative, Deserialize, Serialize)]
#[derivative(Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    Head,
    Get,
    #[derivative(Default)]
    Post,
    Put,
    Patch,
    Delete,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SimpleHttpConfig {
    pub address: SocketAddr,
    #[serde(default)]
    pub encoding: Option<Encoding>,
    #[serde(default)]
    pub headers: Vec<String>,
    #[serde(default)]
    pub query_parameters: Vec<String>,
    pub tls: Option<TlsEnableableConfig>,
    pub auth: Option<HttpSourceAuthConfig>,
    #[serde(default = "crate::serde::default_true")]
    pub strict_path: bool,
    #[serde(default = "default_path")]
    pub path: String,
    #[serde(default = "default_path_key")]
    pub path_key: String,
    #[serde(default)]
    pub method: HttpMethod,
    pub framing: Option<FramingConfig>,
    pub decoding: Option<DeserializerConfig>,
    #[serde(default, deserialize_with = "bool_or_struct")]
    pub acknowledgements: AcknowledgementsConfig,
}

inventory::submit! {
    SourceDescription::new::<SimpleHttpConfig>("http")
}

impl GenerateConfig for SimpleHttpConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            address: "0.0.0.0:8080".parse().unwrap(),
            encoding: None,
            headers: Vec::new(),
            query_parameters: Vec::new(),
            tls: None,
            auth: None,
            path: "/".to_string(),
            path_key: "path".to_string(),
            method: HttpMethod::Post,
            strict_path: true,
            framing: None,
            decoding: Some(default_decoding()),
            acknowledgements: AcknowledgementsConfig::default(),
        })
        .unwrap()
    }
}

fn default_path() -> String {
    "/".to_string()
}

fn default_path_key() -> String {
    "path".to_string()
}

#[derive(Clone)]
struct SimpleHttpSource {
    headers: Vec<String>,
    query_parameters: Vec<String>,
    path_key: String,
    decoder: Decoder,
}

impl HttpSource for SimpleHttpSource {
    fn build_events(
        &self,
        body: Bytes,
        header_map: HeaderMap,
        query_parameters: HashMap<String, String>,
        request_path: &str,
    ) -> Result<Vec<Event>, ErrorMessage> {
        let mut decoder = self.decoder.clone();
        let mut events = Vec::new();
        let mut bytes = BytesMut::new();
        bytes.extend_from_slice(&body);

        loop {
            match decoder.decode_eof(&mut bytes) {
                Ok(Some((next, _))) => {
                    events.extend(next.into_iter());
                }
                Ok(None) => break,
                Err(error) => {
                    return Err(ErrorMessage::new(
                        StatusCode::BAD_REQUEST,
                        format!("Failed decoding body: {}", error),
                    ))
                }
            }
        }

        add_headers(&mut events, &self.headers, header_map);
        add_query_parameters(&mut events, &self.query_parameters, query_parameters);
        add_path(&mut events, self.path_key.as_str(), request_path);

        let now = Utc::now();
        for event in &mut events {
            let log = event.as_mut_log();

            log.try_insert(log_schema().source_type_key(), Bytes::from("http"));
            log.try_insert(log_schema().timestamp_key(), now);
        }

        Ok(events)
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "http")]
impl SourceConfig for SimpleHttpConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        if self.encoding.is_some() && (self.framing.is_some() || self.decoding.is_some()) {
            return Err("Using `encoding` is deprecated and does not have any effect when `decoding` or `framing` is provided. Configure `framing` and `decoding` instead.".into());
        }

        let (framing, decoding) = if let Some(encoding) = self.encoding {
            match encoding {
                Encoding::Text => (
                    NewlineDelimitedDecoderConfig::new().into(),
                    BytesDeserializerConfig::new().into(),
                ),
                Encoding::Json => (
                    BytesDecoderConfig::new().into(),
                    JsonDeserializerConfig::new().into(),
                ),
                Encoding::Ndjson => (
                    NewlineDelimitedDecoderConfig::new().into(),
                    JsonDeserializerConfig::new().into(),
                ),
                Encoding::Binary => (
                    BytesDecoderConfig::new().into(),
                    BytesDeserializerConfig::new().into(),
                ),
            }
        } else {
            let decoding = self.decoding.clone().unwrap_or_else(default_decoding);
            let framing = self
                .framing
                .clone()
                .unwrap_or_else(|| decoding.default_stream_framing());
            (framing, decoding)
        };

        let decoder = DecodingConfig::new(framing, decoding).build();
        let source = SimpleHttpSource {
            headers: self.headers.clone(),
            query_parameters: self.query_parameters.clone(),
            path_key: self.path_key.clone(),
            decoder,
        };
        source.run(
            self.address,
            self.path.as_str(),
            self.method,
            self.strict_path,
            &self.tls,
            &self.auth,
            cx,
            self.acknowledgements,
        )
    }

    fn outputs(&self) -> Vec<Output> {
        vec![Output::default(
            self.decoding
                .as_ref()
                .map(|d| d.output_type())
                .unwrap_or(DataType::Log),
        )]
    }

    fn source_type(&self) -> &'static str {
        "http"
    }

    fn resources(&self) -> Vec<Resource> {
        vec![Resource::tcp(self.address)]
    }

    fn can_acknowledge(&self) -> bool {
        true
    }
}

fn add_path(events: &mut [Event], key: &str, path: &str) {
    for event in events.iter_mut() {
        event
            .as_mut_log()
            .try_insert(key, Value::from(path.to_string()));
    }
}

fn add_headers(events: &mut [Event], headers_config: &[String], headers: HeaderMap) {
    for header_name in headers_config {
        let value = headers.get(header_name).map(HeaderValue::as_bytes);

        for event in events.iter_mut() {
            event.as_mut_log().try_insert_flat(
                header_name as &str,
                Value::from(value.map(Bytes::copy_from_slice)),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::{collections::BTreeMap, io::Write, net::SocketAddr};

    use codecs::{
        decoding::{DeserializerConfig, FramingConfig},
        BytesDecoderConfig, JsonDeserializerConfig,
    };
    use flate2::{
        write::{GzEncoder, ZlibEncoder},
        Compression,
    };
    use futures::Stream;
    use http::{HeaderMap, Method};
    use pretty_assertions::assert_eq;

    use super::SimpleHttpConfig;
    use crate::sources::http::HttpMethod;
    use crate::{
        config::{log_schema, SourceConfig, SourceContext},
        event::{Event, EventStatus, Value},
        test_util::{
            components::{self, assert_source_compliance, HTTP_PUSH_SOURCE_TAGS},
            next_addr, spawn_collect_n, trace_init, wait_for_tcp,
        },
        SourceSender,
    };

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<SimpleHttpConfig>();
    }

    #[allow(clippy::too_many_arguments)]
    async fn source<'a>(
        headers: Vec<String>,
        query_parameters: Vec<String>,
        path_key: &'a str,
        path: &'a str,
        method: &'a str,
        strict_path: bool,
        status: EventStatus,
        acknowledgements: bool,
        framing: Option<FramingConfig>,
        decoding: Option<DeserializerConfig>,
    ) -> (impl Stream<Item = Event> + 'a, SocketAddr) {
        components::init_test();
        let (sender, recv) = SourceSender::new_test_finalize(status);
        let address = next_addr();
        let path = path.to_owned();
        let path_key = path_key.to_owned();
        let context = SourceContext::new_test(sender, None);
        let method = match Method::from_str(method).unwrap() {
            Method::GET => HttpMethod::Get,
            Method::POST => HttpMethod::Post,
            _ => HttpMethod::Post,
        };

        tokio::spawn(async move {
            SimpleHttpConfig {
                address,
                headers,
                encoding: None,
                query_parameters,
                tls: None,
                auth: None,
                strict_path,
                path_key,
                path,
                method,
                framing,
                decoding,
                acknowledgements: acknowledgements.into(),
            }
            .build(context)
            .await
            .unwrap()
            .await
            .unwrap();
        });
        wait_for_tcp(address).await;
        (recv, address)
    }

    async fn send(address: SocketAddr, body: &str) -> u16 {
        reqwest::Client::new()
            .post(&format!("http://{}/", address))
            .body(body.to_owned())
            .send()
            .await
            .unwrap()
            .status()
            .as_u16()
    }

    async fn send_with_headers(address: SocketAddr, body: &str, headers: HeaderMap) -> u16 {
        reqwest::Client::new()
            .post(&format!("http://{}/", address))
            .headers(headers)
            .body(body.to_owned())
            .send()
            .await
            .unwrap()
            .status()
            .as_u16()
    }

    async fn send_with_query(address: SocketAddr, body: &str, query: &str) -> u16 {
        reqwest::Client::new()
            .post(&format!("http://{}?{}", address, query))
            .body(body.to_owned())
            .send()
            .await
            .unwrap()
            .status()
            .as_u16()
    }

    async fn send_with_path(address: SocketAddr, body: &str, path: &str) -> u16 {
        reqwest::Client::new()
            .post(&format!("http://{}{}", address, path))
            .body(body.to_owned())
            .send()
            .await
            .unwrap()
            .status()
            .as_u16()
    }

    async fn send_request(address: SocketAddr, method: &str, body: &str, path: &str) -> u16 {
        let method = Method::from_bytes(method.to_owned().as_bytes()).unwrap();
        format!("method: {}", method.as_str());
        reqwest::Client::new()
            .request(method, &format!("http://{}{}", address, path))
            .body(body.to_owned())
            .send()
            .await
            .unwrap()
            .status()
            .as_u16()
    }

    async fn send_bytes(address: SocketAddr, body: Vec<u8>, headers: HeaderMap) -> u16 {
        reqwest::Client::new()
            .post(&format!("http://{}/", address))
            .headers(headers)
            .body(body)
            .send()
            .await
            .unwrap()
            .status()
            .as_u16()
    }

    async fn spawn_ok_collect_n(
        send: impl std::future::Future<Output = u16> + Send + 'static,
        rx: impl Stream<Item = Event> + Unpin,
        n: usize,
    ) -> Vec<Event> {
        assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async move {
            spawn_collect_n(async move { assert_eq!(200, send.await) }, rx, n).await
        })
        .await
    }

    #[tokio::test]
    async fn http_multiline_text() {
        let body = "test body\ntest body 2";

        let (rx, addr) = source(
            vec![],
            vec![],
            "http_path",
            "/",
            "POST",
            true,
            EventStatus::Delivered,
            true,
            None,
            None,
        )
        .await;

        let mut events = spawn_ok_collect_n(send(addr, body), rx, 2).await;

        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log[log_schema().message_key()], "test body".into());
            assert!(log.get(log_schema().timestamp_key()).is_some());
            assert_eq!(log[log_schema().source_type_key()], "http".into());
            assert_eq!(log["http_path"], "/".into());
        }
        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log[log_schema().message_key()], "test body 2".into());
            assert!(log.get(log_schema().timestamp_key()).is_some());
            assert_eq!(log[log_schema().source_type_key()], "http".into());
            assert_eq!(log["http_path"], "/".into());
        }
    }

    #[tokio::test]
    async fn http_multiline_text2() {
        //same as above test but with a newline at the end
        let body = "test body\ntest body 2\n";

        let (rx, addr) = source(
            vec![],
            vec![],
            "http_path",
            "/",
            "POST",
            true,
            EventStatus::Delivered,
            true,
            None,
            None,
        )
        .await;

        let mut events = spawn_ok_collect_n(send(addr, body), rx, 2).await;

        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log[log_schema().message_key()], "test body".into());
            assert!(log.get(log_schema().timestamp_key()).is_some());
            assert_eq!(log[log_schema().source_type_key()], "http".into());
            assert_eq!(log["http_path"], "/".into());
        }
        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log[log_schema().message_key()], "test body 2".into());
            assert!(log.get(log_schema().timestamp_key()).is_some());
            assert_eq!(log[log_schema().source_type_key()], "http".into());
            assert_eq!(log["http_path"], "/".into());
        }
    }

    #[tokio::test]
    async fn http_bytes_codec_preserves_newlines() {
        trace_init();

        let body = "foo\nbar";

        let (rx, addr) = source(
            vec![],
            vec![],
            "http_path",
            "/",
            "POST",
            true,
            EventStatus::Delivered,
            true,
            Some(BytesDecoderConfig::new().into()),
            None,
        )
        .await;

        let mut events = spawn_ok_collect_n(send(addr, body), rx, 1).await;

        assert_eq!(events.len(), 1);

        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log[log_schema().message_key()], "foo\nbar".into());
            assert!(log.get(log_schema().timestamp_key()).is_some());
            assert_eq!(log[log_schema().source_type_key()], "http".into());
            assert_eq!(log["http_path"], "/".into());
        }
    }

    #[tokio::test]
    async fn http_json_parsing() {
        let mut events = assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
            let (rx, addr) = source(
                vec![],
                vec![],
                "http_path",
                "/",
                "POST",
                true,
                EventStatus::Delivered,
                true,
                None,
                Some(JsonDeserializerConfig::new().into()),
            )
            .await;

            spawn_collect_n(
                async move {
                    assert_eq!(400, send(addr, "{").await); //malformed
                    assert_eq!(400, send(addr, r#"{"key"}"#).await); //key without value

                    assert_eq!(200, send(addr, "{}").await); //can be one object or array of objects
                    assert_eq!(200, send(addr, "[{},{},{}]").await);
                },
                rx,
                2,
            )
            .await
        })
        .await;

        assert!(events
            .remove(1)
            .as_log()
            .get(log_schema().timestamp_key())
            .is_some());
        assert!(events
            .remove(0)
            .as_log()
            .get(log_schema().timestamp_key())
            .is_some());
    }

    #[tokio::test]
    async fn http_json_values() {
        let mut events = assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
            let (rx, addr) = source(
                vec![],
                vec![],
                "http_path",
                "/",
                "POST",
                true,
                EventStatus::Delivered,
                true,
                None,
                Some(JsonDeserializerConfig::new().into()),
            )
            .await;

            spawn_collect_n(
                async move {
                    assert_eq!(200, send(addr, r#"[{"key":"value"}]"#).await);
                    assert_eq!(200, send(addr, r#"{"key2":"value2"}"#).await);
                },
                rx,
                2,
            )
            .await
        })
        .await;

        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log["key"], "value".into());
            assert!(log.get(log_schema().timestamp_key()).is_some());
            assert_eq!(log[log_schema().source_type_key()], "http".into());
            assert_eq!(log["http_path"], "/".into());
        }
        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log["key2"], "value2".into());
            assert!(log.get(log_schema().timestamp_key()).is_some());
            assert_eq!(log[log_schema().source_type_key()], "http".into());
            assert_eq!(log["http_path"], "/".into());
        }
    }

    #[tokio::test]
    async fn http_json_dotted_keys() {
        let mut events = assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
            let (rx, addr) = source(
                vec![],
                vec![],
                "http_path",
                "/",
                "POST",
                true,
                EventStatus::Delivered,
                true,
                None,
                Some(JsonDeserializerConfig::new().into()),
            )
            .await;

            spawn_collect_n(
                async move {
                    assert_eq!(200, send(addr, r#"[{"dotted.key":"value"}]"#).await);
                    assert_eq!(
                        200,
                        send(addr, r#"{"nested":{"dotted.key2":"value2"}}"#).await
                    );
                },
                rx,
                2,
            )
            .await
        })
        .await;

        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log.get_flat("dotted.key").unwrap(), &Value::from("value"));
        }
        {
            let event = events.remove(0);
            let log = event.as_log();
            let mut map = BTreeMap::new();
            map.insert("dotted.key2".to_string(), Value::from("value2"));
            assert_eq!(log["nested"], map.into());
        }
    }

    #[tokio::test]
    async fn http_ndjson() {
        let mut events = assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
            let (rx, addr) = source(
                vec![],
                vec![],
                "http_path",
                "/",
                "POST",
                true,
                EventStatus::Delivered,
                true,
                None,
                Some(JsonDeserializerConfig::new().into()),
            )
            .await;

            spawn_collect_n(
                async move {
                    assert_eq!(
                        200,
                        send(addr, r#"[{"key1":"value1"},{"key2":"value2"}]"#).await
                    );

                    assert_eq!(
                        200,
                        send(addr, "{\"key1\":\"value1\"}\n\n{\"key2\":\"value2\"}").await
                    );
                },
                rx,
                4,
            )
            .await
        })
        .await;

        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log["key1"], "value1".into());
            assert!(log.get(log_schema().timestamp_key()).is_some());
            assert_eq!(log[log_schema().source_type_key()], "http".into());
            assert_eq!(log["http_path"], "/".into());
        }
        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log["key2"], "value2".into());
            assert!(log.get(log_schema().timestamp_key()).is_some());
            assert_eq!(log[log_schema().source_type_key()], "http".into());
            assert_eq!(log["http_path"], "/".into());
        }
        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log["key1"], "value1".into());
            assert!(log.get(log_schema().timestamp_key()).is_some());
            assert_eq!(log[log_schema().source_type_key()], "http".into());
            assert_eq!(log["http_path"], "/".into());
        }
        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log["key2"], "value2".into());
            assert!(log.get(log_schema().timestamp_key()).is_some());
            assert_eq!(log[log_schema().source_type_key()], "http".into());
            assert_eq!(log["http_path"], "/".into());
        }
    }

    #[tokio::test]
    async fn http_headers() {
        let mut events = assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
            let mut headers = HeaderMap::new();
            headers.insert("User-Agent", "test_client".parse().unwrap());
            headers.insert("Upgrade-Insecure-Requests", "false".parse().unwrap());

            let (rx, addr) = source(
                vec![
                    "User-Agent".to_string(),
                    "Upgrade-Insecure-Requests".to_string(),
                    "AbsentHeader".to_string(),
                ],
                vec![],
                "http_path",
                "/",
                "POST",
                true,
                EventStatus::Delivered,
                true,
                None,
                Some(JsonDeserializerConfig::new().into()),
            )
            .await;

            spawn_ok_collect_n(
                send_with_headers(addr, "{\"key1\":\"value1\"}", headers),
                rx,
                1,
            )
            .await
        })
        .await;

        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log["key1"], "value1".into());
            assert_eq!(log["\"User-Agent\""], "test_client".into());
            assert_eq!(log["\"Upgrade-Insecure-Requests\""], "false".into());
            assert_eq!(log["AbsentHeader"], Value::Null);
            assert_eq!(log["http_path"], "/".into());
            assert!(log.get(log_schema().timestamp_key()).is_some());
            assert_eq!(log[log_schema().source_type_key()], "http".into());
        }
    }

    #[tokio::test]
    async fn http_query() {
        let mut events = assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
            let (rx, addr) = source(
                vec![],
                vec![
                    "source".to_string(),
                    "region".to_string(),
                    "absent".to_string(),
                ],
                "http_path",
                "/",
                "POST",
                true,
                EventStatus::Delivered,
                true,
                None,
                Some(JsonDeserializerConfig::new().into()),
            )
            .await;

            spawn_ok_collect_n(
                send_with_query(addr, "{\"key1\":\"value1\"}", "source=staging&region=gb"),
                rx,
                1,
            )
            .await
        })
        .await;

        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log["key1"], "value1".into());
            assert_eq!(log["source"], "staging".into());
            assert_eq!(log["region"], "gb".into());
            assert_eq!(log["absent"], Value::Null);
            assert_eq!(log["http_path"], "/".into());
            assert!(log.get(log_schema().timestamp_key()).is_some());
            assert_eq!(log[log_schema().source_type_key()], "http".into());
        }
    }

    #[tokio::test]
    async fn http_gzip_deflate() {
        let mut events = assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
            let body = "test body";

            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(body.as_bytes()).unwrap();
            let body = encoder.finish().unwrap();

            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(body.as_slice()).unwrap();
            let body = encoder.finish().unwrap();

            let mut headers = HeaderMap::new();
            headers.insert("Content-Encoding", "gzip, deflate".parse().unwrap());

            let (rx, addr) = source(
                vec![],
                vec![],
                "http_path",
                "/",
                "POST",
                true,
                EventStatus::Delivered,
                true,
                None,
                None,
            )
            .await;

            spawn_ok_collect_n(send_bytes(addr, body, headers), rx, 1).await
        })
        .await;

        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log[log_schema().message_key()], "test body".into());
            assert!(log.get(log_schema().timestamp_key()).is_some());
            assert_eq!(log[log_schema().source_type_key()], "http".into());
            assert_eq!(log["http_path"], "/".into());
        }
    }

    #[tokio::test]
    async fn http_path() {
        let mut events = assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
            let (rx, addr) = source(
                vec![],
                vec![],
                "vector_http_path",
                "/event/path",
                "POST",
                true,
                EventStatus::Delivered,
                true,
                None,
                Some(JsonDeserializerConfig::new().into()),
            )
            .await;

            spawn_ok_collect_n(
                send_with_path(addr, "{\"key1\":\"value1\"}", "/event/path"),
                rx,
                1,
            )
            .await
        })
        .await;

        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log["key1"], "value1".into());
            assert_eq!(log["vector_http_path"], "/event/path".into());
            assert!(log.get(log_schema().timestamp_key()).is_some());
            assert_eq!(log[log_schema().source_type_key()], "http".into());
        }
    }

    #[tokio::test]
    async fn http_path_no_restriction() {
        let mut events = assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
            let (rx, addr) = source(
                vec![],
                vec![],
                "vector_http_path",
                "/event",
                "POST",
                false,
                EventStatus::Delivered,
                true,
                None,
                Some(JsonDeserializerConfig::new().into()),
            )
            .await;

            spawn_collect_n(
                async move {
                    assert_eq!(
                        200,
                        send_with_path(addr, "{\"key1\":\"value1\"}", "/event/path1").await
                    );
                    assert_eq!(
                        200,
                        send_with_path(addr, "{\"key2\":\"value2\"}", "/event/path2").await
                    );
                },
                rx,
                2,
            )
            .await
        })
        .await;

        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log["key1"], "value1".into());
            assert_eq!(log["vector_http_path"], "/event/path1".into());
            assert!(log.get(log_schema().timestamp_key()).is_some());
            assert_eq!(log[log_schema().source_type_key()], "http".into());
        }
        {
            let event = events.remove(0);
            let log = event.as_log();
            assert_eq!(log["key2"], "value2".into());
            assert_eq!(log["vector_http_path"], "/event/path2".into());
            assert!(log.get(log_schema().timestamp_key()).is_some());
            assert_eq!(log[log_schema().source_type_key()], "http".into());
        }
    }

    #[tokio::test]
    async fn http_wrong_path() {
        let (_rx, addr) = source(
            vec![],
            vec![],
            "vector_http_path",
            "/",
            "POST",
            true,
            EventStatus::Delivered,
            true,
            None,
            Some(JsonDeserializerConfig::new().into()),
        )
        .await;

        assert_eq!(
            404,
            send_with_path(addr, "{\"key1\":\"value1\"}", "/event/path").await
        );
    }

    #[tokio::test]
    async fn http_delivery_failure() {
        assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
            let (rx, addr) = source(
                vec![],
                vec![],
                "http_path",
                "/",
                "POST",
                true,
                EventStatus::Rejected,
                true,
                None,
                None,
            )
            .await;

            spawn_collect_n(
                async move {
                    assert_eq!(400, send(addr, "test body\n").await);
                },
                rx,
                1,
            )
            .await;
        })
        .await;
    }

    #[tokio::test]
    async fn ignores_disabled_acknowledgements() {
        let events = assert_source_compliance(&HTTP_PUSH_SOURCE_TAGS, async {
            let (rx, addr) = source(
                vec![],
                vec![],
                "http_path",
                "/",
                "POST",
                true,
                EventStatus::Rejected,
                false,
                None,
                None,
            )
            .await;

            spawn_collect_n(
                async move {
                    assert_eq!(200, send(addr, "test body\n").await);
                },
                rx,
                1,
            )
            .await
        })
        .await;

        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn http_get_method() {
        let (_rx, addr) = source(
            vec![],
            vec![],
            "http_path",
            "/",
            "GET",
            true,
            EventStatus::Delivered,
            true,
            None,
            None,
        )
        .await;

        assert_eq!(200, send_request(addr, "GET", "", "/").await);
    }
}
