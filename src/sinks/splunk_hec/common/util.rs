use std::num::NonZeroU64;

use crate::{
    http::HttpClient,
    internal_events::TemplateRenderingFailed,
    sinks::{
        self,
        util::{Compression, SinkBatchSettings},
        UriParseError,
    },
    template::Template,
    tls::{TlsOptions, TlsSettings},
};
use http::{Request, StatusCode, Uri};
use hyper::Body;
use snafu::{ResultExt, Snafu};
use uuid::Uuid;
use vector_core::{config::proxy::ProxyConfig, event::EventRef};

// A Splunk channel must be a GUID/UUID formatted value
// https://docs.splunk.com/Documentation/Splunk/8.2.3/Data/AboutHECIDXAck#About_channels_and_sending_data
lazy_static::lazy_static! {
    static ref SPLUNK_CHANNEL: String = {
        let mut buf = Uuid::encode_buffer();
        Uuid::new_v4().to_hyphenated().encode_lower(&mut buf).to_string()
    };
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SplunkHecDefaultBatchSettings;

impl SinkBatchSettings for SplunkHecDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = None;
    const MAX_BYTES: Option<usize> = Some(1_000_000);
    const TIMEOUT_SECS: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(1) };
}

#[derive(Debug, Snafu)]
pub enum HealthcheckError {
    #[snafu(display("Invalid HEC token"))]
    InvalidToken,
    #[snafu(display("Queues are full"))]
    QueuesFull,
}

pub fn create_client(
    tls: &Option<TlsOptions>,
    proxy_config: &ProxyConfig,
) -> crate::Result<HttpClient> {
    let tls_settings = TlsSettings::from_options(tls)?;
    Ok(HttpClient::new(tls_settings, proxy_config)?)
}

pub async fn build_healthcheck(
    endpoint: String,
    token: String,
    client: HttpClient,
) -> crate::Result<()> {
    let uri =
        build_uri(endpoint.as_str(), "/services/collector/health/1.0").context(UriParseError)?;

    let request = Request::get(uri)
        .header("Authorization", format!("Splunk {}", token))
        .body(Body::empty())
        .unwrap();

    let response = client.send(request).await?;
    match response.status() {
        StatusCode::OK => Ok(()),
        StatusCode::BAD_REQUEST => Err(HealthcheckError::InvalidToken.into()),
        StatusCode::SERVICE_UNAVAILABLE => Err(HealthcheckError::QueuesFull.into()),
        other => Err(sinks::HealthcheckError::UnexpectedStatus { status: other }.into()),
    }
}

pub async fn build_request(
    endpoint: &str,
    token: &str,
    compression: Compression,
    events: Vec<u8>,
    path: &str,
) -> crate::Result<Request<Vec<u8>>> {
    let uri = build_uri(endpoint, path).context(UriParseError)?;

    let mut builder = Request::post(uri)
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Splunk {}", token))
        .header(
            "X-Splunk-Request-Channel",
            format!("{}", SPLUNK_CHANNEL.as_str()),
        );

    if let Some(ce) = compression.content_encoding() {
        builder = builder.header("Content-Encoding", ce);
    }

    builder.body(events).map_err(Into::into)
}

pub fn build_uri(host: &str, path: &str) -> Result<Uri, http::uri::InvalidUri> {
    format!("{}{}", host.trim_end_matches('/'), path).parse::<Uri>()
}

pub fn host_key() -> String {
    crate::config::log_schema().host_key().to_string()
}

pub fn render_template_string<'a>(
    template: &Template,
    event: impl Into<EventRef<'a>>,
    field_name: &str,
) -> Option<String> {
    template
        .render_string(event)
        .map_err(|error| {
            emit!(&TemplateRenderingFailed {
                error,
                field: Some(field_name),
                drop_event: false
            });
        })
        .ok()
}

#[cfg(test)]
mod tests {
    use http::{HeaderValue, Uri};
    use vector_core::config::proxy::ProxyConfig;
    use wiremock::{
        matchers::{header, method, path},
        Mock, MockServer, ResponseTemplate,
    };

    use crate::sinks::{
        splunk_hec::common::{build_healthcheck, build_request, create_client},
        util::Compression,
    };

    #[tokio::test]
    async fn test_build_healthcheck_200_response_returns_ok() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/services/collector/health/1.0"))
            .and(header("Authorization", "Splunk token"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let client = create_client(&None, &ProxyConfig::default()).unwrap();
        let healthcheck = build_healthcheck(mock_server.uri(), "token".to_string(), client);

        assert!(healthcheck.await.is_ok())
    }

    #[tokio::test]
    async fn test_build_healthcheck_400_response_returns_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/services/collector/health/1.0"))
            .and(header("Authorization", "Splunk token"))
            .respond_with(ResponseTemplate::new(400))
            .mount(&mock_server)
            .await;

        let client = create_client(&None, &ProxyConfig::default()).unwrap();
        let healthcheck = build_healthcheck(mock_server.uri(), "token".to_string(), client);

        assert_eq!(
            &healthcheck.await.unwrap_err().to_string(),
            "Invalid HEC token"
        );
    }

    #[tokio::test]
    async fn test_build_healthcheck_503_response_returns_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/services/collector/health/1.0"))
            .and(header("Authorization", "Splunk token"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&mock_server)
            .await;

        let client = create_client(&None, &ProxyConfig::default()).unwrap();
        let healthcheck = build_healthcheck(mock_server.uri(), "token".to_string(), client);

        assert_eq!(
            &healthcheck.await.unwrap_err().to_string(),
            "Queues are full"
        );
    }

    #[tokio::test]
    async fn test_build_healthcheck_500_response_returns_error() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/services/collector/health/1.0"))
            .and(header("Authorization", "Splunk token"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&mock_server)
            .await;

        let client = create_client(&None, &ProxyConfig::default()).unwrap();
        let healthcheck = build_healthcheck(mock_server.uri(), "token".to_string(), client);

        assert_eq!(
            &healthcheck.await.unwrap_err().to_string(),
            "Unexpected status: 500 Internal Server Error"
        );
    }

    #[tokio::test]
    async fn test_build_request_compression_none_returns_expected_request() {
        let endpoint = "http://localhost:8888";
        let token = "token";
        let compression = Compression::None;
        let events = "events".as_bytes().to_vec();

        let request = build_request(
            endpoint,
            token,
            compression,
            events.clone(),
            "/services/collector/event",
        )
        .await
        .unwrap();

        assert_eq!(
            request.uri(),
            &Uri::from_static("http://localhost:8888/services/collector/event")
        );

        assert_eq!(
            request.headers().get("Content-Type"),
            Some(&HeaderValue::from_static("application/json"))
        );

        assert_eq!(
            request.headers().get("Authorization"),
            Some(&HeaderValue::from_static("Splunk token"))
        );

        assert_eq!(request.headers().get("Content-Encoding"), None);

        assert_eq!(request.body(), &events)
    }

    #[tokio::test]
    async fn test_build_request_compression_gzip_returns_expected_request() {
        let endpoint = "http://localhost:8888";
        let token = "token";
        let compression = Compression::gzip_default();
        let events = "events".as_bytes().to_vec();

        let request = build_request(
            endpoint,
            token,
            compression,
            events.clone(),
            "/services/collector/event",
        )
        .await
        .unwrap();

        assert_eq!(
            request.uri(),
            &Uri::from_static("http://localhost:8888/services/collector/event")
        );

        assert_eq!(
            request.headers().get("Content-Type"),
            Some(&HeaderValue::from_static("application/json"))
        );

        assert_eq!(
            request.headers().get("Authorization"),
            Some(&HeaderValue::from_static("Splunk token"))
        );

        assert_eq!(
            request.headers().get("Content-Encoding"),
            Some(&HeaderValue::from_static("gzip"))
        );

        assert_eq!(request.body(), &events)
    }

    #[tokio::test]
    async fn test_build_request_uri_invalid_uri_returns_error() {
        let endpoint = "invalid";
        let token = "token";
        let compression = Compression::gzip_default();
        let events = "events".as_bytes().to_vec();

        let err = build_request(
            endpoint,
            token,
            compression,
            events.clone(),
            "/services/collector/event",
        )
        .await
        .unwrap_err();
        assert_eq!(err.to_string(), "URI parse error: invalid format")
    }
}

#[cfg(all(test, feature = "splunk-integration-tests"))]
mod integration_tests {
    use super::{build_healthcheck, create_client, integration_test_helpers::get_token};
    use crate::{assert_downcast_matches, sinks::splunk_hec::common::HealthcheckError};
    use http::StatusCode;
    use std::net::SocketAddr;
    use vector_core::config::proxy::ProxyConfig;
    use warp::Filter;

    #[tokio::test]
    async fn splunk_healthcheck_ok() {
        let client = create_client(&None, &ProxyConfig::default()).unwrap();
        let healthcheck = build_healthcheck(
            "http://localhost:8088/".to_string(),
            get_token().await,
            client,
        );

        healthcheck.await.unwrap();
    }

    #[tokio::test]
    async fn splunk_healthcheck_server_not_listening() {
        let client = create_client(&None, &ProxyConfig::default()).unwrap();
        let healthcheck = build_healthcheck(
            "http://localhost:1111/".to_string(),
            get_token().await,
            client,
        );

        healthcheck.await.unwrap_err();
    }

    #[tokio::test]
    async fn splunk_healthcheck_server_unavailable() {
        let client = create_client(&None, &ProxyConfig::default()).unwrap();
        let healthcheck = build_healthcheck(
            "http://localhost:5503/".to_string(),
            get_token().await,
            client,
        );

        let unhealthy = warp::any()
            .map(|| warp::reply::with_status("i'm sad", StatusCode::SERVICE_UNAVAILABLE));
        let server = warp::serve(unhealthy).bind("0.0.0.0:5503".parse::<SocketAddr>().unwrap());
        tokio::spawn(server);

        assert_downcast_matches!(
            healthcheck.await.unwrap_err(),
            HealthcheckError,
            HealthcheckError::QueuesFull
        );
    }
}

#[cfg(all(test, feature = "splunk-integration-tests"))]
pub mod integration_test_helpers {
    use crate::test_util::retry_until;
    use serde_json::Value as JsonValue;
    use tokio::time::Duration;

    const USERNAME: &str = "admin";
    const PASSWORD: &str = "password";

    pub async fn get_token() -> String {
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();

        let res = retry_until(
            || {
                client
                    .get("https://localhost:8089/services/data/inputs/http?output_mode=json")
                    .basic_auth(USERNAME, Some(PASSWORD))
                    .send()
            },
            Duration::from_millis(500),
            Duration::from_secs(30),
        )
        .await;

        let json: JsonValue = res.json().await.unwrap();
        let entries = json["entry"].as_array().unwrap().clone();

        if entries.is_empty() {
            panic!("You don't have any HTTP Event Collector inputs set up in Splunk");
        }

        entries[0]["content"]["token"].as_str().unwrap().to_owned()
    }
}
