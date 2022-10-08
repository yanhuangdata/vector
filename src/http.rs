use std::{
    fmt,
    task::{Context, Poll},
};

use futures::future::BoxFuture;
use headers::{Authorization, HeaderMapExt};
use http::{header::HeaderValue, request::Builder, uri::InvalidUri, HeaderMap, Request, Uri};
use hyper::{
    body::{Body, HttpBody},
    client,
    client::{Client, HttpConnector},
};
use hyper_openssl::HttpsConnector;
use hyper_proxy::ProxyConnector;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use tower::Service;
use tracing::Instrument;

use crate::{
    config::ProxyConfig,
    internal_events::http_client,
    tls::{tls_connector_builder, MaybeTlsSettings, TlsError},
};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum HttpError {
    #[snafu(display("Failed to build TLS connector: {}", source))]
    BuildTlsConnector { source: TlsError },
    #[snafu(display("Failed to build HTTPS connector: {}", source))]
    MakeHttpsConnector { source: openssl::error::ErrorStack },
    #[snafu(display("Failed to build Proxy connector: {}", source))]
    MakeProxyConnector { source: InvalidUri },
    #[snafu(display("Failed to make HTTP(S) request: {}", source))]
    CallRequest { source: hyper::Error },
    #[snafu(display("Failed to build HTTP request: {}", source))]
    BuildRequest { source: http::Error },
}

pub type HttpClientFuture = <HttpClient as Service<http::Request<Body>>>::Future;

pub struct HttpClient<B = Body> {
    client: Client<ProxyConnector<HttpsConnector<HttpConnector>>, B>,
    user_agent: HeaderValue,
}

impl<B> HttpClient<B>
where
    B: fmt::Debug + HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<crate::Error>,
{
    pub fn new(
        tls_settings: impl Into<MaybeTlsSettings>,
        proxy_config: &ProxyConfig,
    ) -> Result<HttpClient<B>, HttpError> {
        HttpClient::new_with_custom_client(tls_settings, proxy_config, &mut Client::builder())
    }

    pub fn new_with_custom_client(
        tls_settings: impl Into<MaybeTlsSettings>,
        proxy_config: &ProxyConfig,
        client_builder: &mut client::Builder,
    ) -> Result<HttpClient<B>, HttpError> {
        let proxy = build_proxy_connector(tls_settings.into(), proxy_config)?;
        let client = client_builder.build(proxy);

        let version = crate::get_version();
        let user_agent = HeaderValue::from_str(&format!("Vector/{}", version))
            .expect("Invalid header value for version!");

        Ok(HttpClient { client, user_agent })
    }

    pub fn send(
        &self,
        mut request: Request<B>,
    ) -> BoxFuture<'static, Result<http::Response<Body>, HttpError>> {
        let span = tracing::info_span!("http");
        let _enter = span.enter();

        default_request_headers(&mut request, &self.user_agent);

        emit!(http_client::AboutToSendHttpRequest { request: &request });

        let response = self.client.request(request);

        let fut = async move {
            // Capture the time right before we issue the request.
            // Request doesn't start the processing until we start polling it.
            let before = std::time::Instant::now();

            // Send request and wait for the result.
            let response_result = response.await;

            // Compute the roundtrip time it took to send the request and get
            // the response or error.
            let roundtrip = before.elapsed();

            // Handle the errors and extract the response.
            let response = response_result
                .map_err(|error| {
                    // Emit the error into the internal events system.
                    emit!(http_client::GotHttpError {
                        error: &error,
                        roundtrip
                    });
                    error
                })
                .context(CallRequestSnafu)?;

            // Emit the response into the internal events system.
            emit!(http_client::GotHttpResponse {
                response: &response,
                roundtrip
            });
            Ok(response)
        }
        .instrument(span.clone().or_current());

        Box::pin(fut)
    }
}

pub fn build_proxy_connector(
    tls_settings: MaybeTlsSettings,
    proxy_config: &ProxyConfig,
) -> Result<ProxyConnector<HttpsConnector<HttpConnector>>, HttpError> {
    let https = build_tls_connector(tls_settings)?;
    let mut proxy = ProxyConnector::new(https).unwrap();
    proxy_config
        .configure(&mut proxy)
        .context(MakeProxyConnectorSnafu)?;
    Ok(proxy)
}

pub fn build_tls_connector(
    tls_settings: MaybeTlsSettings,
) -> Result<HttpsConnector<HttpConnector>, HttpError> {
    let mut http = HttpConnector::new();
    http.enforce_http(false);

    let tls = tls_connector_builder(&tls_settings).context(BuildTlsConnectorSnafu)?;
    let mut https = HttpsConnector::with_connector(http, tls).context(MakeHttpsConnectorSnafu)?;

    let settings = tls_settings.tls().cloned();
    https.set_callback(move |c, _uri| {
        if let Some(settings) = &settings {
            settings.apply_connect_configuration(c);
        }

        Ok(())
    });
    Ok(https)
}

fn default_request_headers<B>(request: &mut Request<B>, user_agent: &HeaderValue) {
    if !request.headers().contains_key("User-Agent") {
        request
            .headers_mut()
            .insert("User-Agent", user_agent.clone());
    }

    if !request.headers().contains_key("Accept-Encoding") {
        // hardcoding until we support compressed responses:
        // https://github.com/vectordotdev/vector/issues/5440
        request
            .headers_mut()
            .insert("Accept-Encoding", HeaderValue::from_static("identity"));
    }
}

impl<B> Service<Request<B>> for HttpClient<B>
where
    B: fmt::Debug + HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<crate::Error> + Send,
{
    type Response = http::Response<Body>;
    type Error = HttpError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        self.send(request)
    }
}

impl<B> Clone for HttpClient<B> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            user_agent: self.user_agent.clone(),
        }
    }
}

impl<B> fmt::Debug for HttpClient<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpClient")
            .field("client", &self.client)
            .field("user_agent", &self.user_agent)
            .finish()
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "strategy")]
pub enum Auth {
    Basic { user: String, password: String },
    Bearer { token: String },
}

pub trait MaybeAuth: Sized {
    fn choose_one(&self, other: &Self) -> crate::Result<Self>;
}

impl MaybeAuth for Option<Auth> {
    fn choose_one(&self, other: &Self) -> crate::Result<Self> {
        if self.is_some() && other.is_some() {
            Err("Two authorization credentials was provided.".into())
        } else {
            Ok(self.clone().or_else(|| other.clone()))
        }
    }
}

impl Auth {
    pub fn apply<B>(&self, req: &mut Request<B>) {
        self.apply_headers_map(req.headers_mut())
    }

    pub fn apply_builder(&self, mut builder: Builder) -> Builder {
        if let Some(map) = builder.headers_mut() {
            self.apply_headers_map(map)
        }
        builder
    }

    pub fn apply_headers_map(&self, map: &mut HeaderMap) {
        match &self {
            Auth::Basic { user, password } => {
                let auth = Authorization::basic(user, password);
                map.typed_insert(auth);
            }
            Auth::Bearer { token } => match Authorization::bearer(token) {
                Ok(auth) => map.typed_insert(auth),
                Err(error) => error!(message = "Invalid bearer token.", token = %token, %error),
            },
        }
    }
}

pub fn get_http_scheme_from_uri(uri: &Uri) -> &'static str {
    // If there's no scheme, we just use "http" since it provides the most semantic relevance without inadvertantly
    // implying things it can't know i.e. returning "https" when we're not actually sure HTTPS was used.
    uri.scheme_str().map_or("http", |scheme| match scheme {
        "http" => "http",
        "https" => "https",
        // `http::Uri` ensures that we always get "http" or "https" if the URI is created with a well-formed scheme, but
        // it also supports arbitrary schemes, which is where we bomb out down here, since we can't generate a static
        // string for an arbitrary input string... and anything other than "http" and "https" makes no sense for an HTTP
        // client anyways.
        s => panic!("invalid URI scheme for HTTP client: {}", s),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_request_headers_defaults() {
        let user_agent = HeaderValue::from_static("vector");
        let mut request = Request::post("http://example.com").body(()).unwrap();
        default_request_headers(&mut request, &user_agent);
        assert_eq!(
            request.headers().get("Accept-Encoding"),
            Some(&HeaderValue::from_static("identity")),
        );
        assert_eq!(request.headers().get("User-Agent"), Some(&user_agent));
    }

    #[test]
    fn test_default_request_headers_does_not_overwrite() {
        let mut request = Request::post("http://example.com")
            .header("Accept-Encoding", "gzip")
            .header("User-Agent", "foo")
            .body(())
            .unwrap();
        default_request_headers(&mut request, &HeaderValue::from_static("vector"));
        assert_eq!(
            request.headers().get("Accept-Encoding"),
            Some(&HeaderValue::from_static("gzip")),
        );
        assert_eq!(
            request.headers().get("User-Agent"),
            Some(&HeaderValue::from_static("foo"))
        );
    }
}
