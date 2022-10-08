use std::{
    collections::HashMap,
    future::ready,
    time::{Duration, Instant},
};

use futures::{stream, FutureExt, StreamExt, TryFutureExt};
use hyper::{Body, Request};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use tokio_stream::wrappers::IntervalStream;
use vector_core::ByteSizeOf;

use super::parser;
use crate::{
    config::{
        self, GenerateConfig, Output, ProxyConfig, SourceConfig, SourceContext, SourceDescription,
    },
    http::{Auth, HttpClient},
    internal_events::{
        EndpointBytesReceived, PrometheusEventsReceived, PrometheusHttpError,
        PrometheusHttpResponseError, PrometheusParseError, RequestCompleted, StreamClosedError,
    },
    shutdown::ShutdownSignal,
    sources,
    tls::{TlsConfig, TlsSettings},
    SourceSender,
};

// pulled up, and split over multiple lines, because the long lines trip up rustfmt such that it
// gave up trying to format, but reported no error
static PARSE_ERROR_NO_PATH: &str = "No path is set on the endpoint and we got a parse error,\
                                    did you mean to use /metrics? This behavior changed in version 0.11.";
static NOT_FOUND_NO_PATH: &str = "No path is set on the endpoint and we got a 404,\
                                  did you mean to use /metrics?\
                                  This behavior changed in version 0.11.";

#[derive(Debug, Snafu)]
enum ConfigError {
    #[snafu(display("Cannot set both `endpoints` and `hosts`"))]
    BothEndpointsAndHosts,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
struct PrometheusScrapeConfig {
    // Deprecated name
    #[serde(alias = "hosts")]
    endpoints: Vec<String>,
    #[serde(default = "default_scrape_interval_secs")]
    scrape_interval_secs: u64,
    instance_tag: Option<String>,
    endpoint_tag: Option<String>,
    #[serde(default = "crate::serde::default_false")]
    honor_labels: bool,
    query: Option<HashMap<String, Vec<String>>>,
    tls: Option<TlsConfig>,
    auth: Option<Auth>,
}

pub(crate) const fn default_scrape_interval_secs() -> u64 {
    15
}

inventory::submit! {
    SourceDescription::new::<PrometheusScrapeConfig>("prometheus")
}

inventory::submit! {
    SourceDescription::new::<PrometheusScrapeConfig>("prometheus_scrape")
}

impl GenerateConfig for PrometheusScrapeConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            endpoints: vec!["http://localhost:9090/metrics".to_string()],
            scrape_interval_secs: default_scrape_interval_secs(),
            instance_tag: Some("instance".to_string()),
            endpoint_tag: Some("endpoint".to_string()),
            honor_labels: false,
            query: None,
            tls: None,
            auth: None,
        })
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "prometheus_scrape")]
impl SourceConfig for PrometheusScrapeConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<sources::Source> {
        let urls = self
            .endpoints
            .iter()
            .map(|s| s.parse::<http::Uri>().context(sources::UriParseSnafu))
            .map(|r| {
                r.map(|uri| {
                    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
                    if let Some(query) = uri.query() {
                        serializer.extend_pairs(url::form_urlencoded::parse(query.as_bytes()));
                    };
                    if let Some(query) = &self.query {
                        for (k, l) in query {
                            for v in l {
                                serializer.append_pair(k, v);
                            }
                        }
                    };
                    let mut builder = http::Uri::builder();
                    if let Some(scheme) = uri.scheme() {
                        builder = builder.scheme(scheme.clone());
                    };
                    if let Some(authority) = uri.authority() {
                        builder = builder.authority(authority.clone());
                    };
                    builder = builder.path_and_query(match serializer.finish() {
                        query if !query.is_empty() => format!("{}?{}", uri.path(), query),
                        _ => uri.path().to_string(),
                    });
                    builder.build().expect("error building URI")
                })
            })
            .collect::<Result<Vec<http::Uri>, sources::BuildError>>()?;
        let tls = TlsSettings::from_options(&self.tls)?;
        Ok(prometheus(
            self.clone(),
            urls,
            tls,
            cx.proxy.clone(),
            cx.shutdown,
            cx.out,
        )
        .boxed())
    }

    fn outputs(&self) -> Vec<Output> {
        vec![Output::default(config::DataType::Metric)]
    }

    fn source_type(&self) -> &'static str {
        "prometheus_scrape"
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

// Add a compatibility alias to avoid breaking existing configs
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PrometheusCompatConfig {
    // Clone of PrometheusScrapeConfig to work around serde bug
    // https://github.com/serde-rs/serde/issues/1504
    #[serde(alias = "hosts")]
    endpoints: Vec<String>,
    instance_tag: Option<String>,
    endpoint_tag: Option<String>,
    #[serde(default = "crate::serde::default_false")]
    honor_labels: bool,
    query: Option<HashMap<String, Vec<String>>>,
    #[serde(default = "default_scrape_interval_secs")]
    scrape_interval_secs: u64,
    tls: Option<TlsConfig>,
    auth: Option<Auth>,
}

#[async_trait::async_trait]
#[typetag::serde(name = "prometheus")]
impl SourceConfig for PrometheusCompatConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<sources::Source> {
        // Workaround for serde bug
        // https://github.com/serde-rs/serde/issues/1504
        let config = PrometheusScrapeConfig {
            endpoints: self.endpoints.clone(),
            instance_tag: self.instance_tag.clone(),
            endpoint_tag: self.endpoint_tag.clone(),
            honor_labels: self.honor_labels,
            query: self.query.clone(),
            scrape_interval_secs: self.scrape_interval_secs,
            tls: self.tls.clone(),
            auth: self.auth.clone(),
        };
        config.build(cx).await
    }

    fn outputs(&self) -> Vec<Output> {
        vec![Output::default(config::DataType::Metric)]
    }

    fn source_type(&self) -> &'static str {
        "prometheus_scrape"
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

// InstanceInfo stores the scraped instance info and the tag to insert into the log event with. It
// is used to join these two pieces of info to avoid storing the instance if instance_tag is not
// configured
#[derive(Clone)]
struct InstanceInfo {
    tag: String,
    instance: String,
    honor_label: bool,
}

// EndpointInfo stores the scraped endpoint info and the tag to insert into the log event with. It
// is used to join these two pieces of info to avoid storing the endpoint if endpoint_tag is not
// configured
#[derive(Clone)]
struct EndpointInfo {
    tag: String,
    endpoint: String,
    honor_label: bool,
}

async fn prometheus(
    config: PrometheusScrapeConfig,
    urls: Vec<http::Uri>,
    tls: TlsSettings,
    proxy: ProxyConfig,
    shutdown: ShutdownSignal,
    mut out: SourceSender,
) -> Result<(), ()> {
    let mut stream = IntervalStream::new(tokio::time::interval(Duration::from_secs(
        config.scrape_interval_secs,
    )))
    .take_until(shutdown)
    .map(move |_| stream::iter(urls.clone()))
    .flatten()
    .map(move |url| {
        let client = HttpClient::new(tls.clone(), &proxy).expect("Building HTTP client failed");
        let endpoint = url.to_string();

        let mut request = Request::get(&url)
            .body(Body::empty())
            .expect("error creating request");
        if let Some(auth) = &config.auth {
            auth.apply(&mut request);
        }

        let instance_info = config.instance_tag.as_ref().map(|tag| {
            let instance = format!(
                "{}:{}",
                url.host().unwrap_or_default(),
                url.port_u16().unwrap_or_else(|| match url.scheme() {
                    Some(scheme) if scheme == &http::uri::Scheme::HTTP => 80,
                    Some(scheme) if scheme == &http::uri::Scheme::HTTPS => 443,
                    _ => 0,
                })
            );
            InstanceInfo {
                tag: tag.to_string(),
                instance,
                honor_label: config.honor_labels,
            }
        });
        let endpoint_info = config.endpoint_tag.as_ref().map(|tag| EndpointInfo {
            tag: tag.to_string(),
            endpoint: url.to_string(),
            honor_label: config.honor_labels,
        });

        let start = Instant::now();
        client
            .send(request)
            .map_err(crate::Error::from)
            .and_then(|response| async move {
                let (header, body) = response.into_parts();
                let body = hyper::body::to_bytes(body).await?;
                emit!(EndpointBytesReceived {
                    byte_size: body.len(),
                    protocol: "http",
                    endpoint: endpoint.as_str(),
                });
                Ok((header, body))
            })
            .into_stream()
            .filter_map(move |response| {
                let instance_info = instance_info.clone();
                let endpoint_info = endpoint_info.clone();

                ready(match response {
                    Ok((header, body)) if header.status == hyper::StatusCode::OK => {
                        emit!(RequestCompleted {
                            start,
                            end: Instant::now()
                        });

                        let body = String::from_utf8_lossy(&body);

                        match parser::parse_text(&body) {
                            Ok(events) => {
                                emit!(PrometheusEventsReceived {
                                    byte_size: events.size_of(),
                                    count: events.len(),
                                    uri: url.clone()
                                });
                                Some(stream::iter(events).map(move |mut event| {
                                    let metric = event.as_mut_metric();
                                    if let Some(InstanceInfo {
                                        tag,
                                        instance,
                                        honor_label,
                                    }) = &instance_info
                                    {
                                        match (honor_label, metric.tag_value(tag)) {
                                            (false, Some(old_instance)) => {
                                                metric.insert_tag(
                                                    format!("exported_{}", tag),
                                                    old_instance,
                                                );
                                                metric.insert_tag(tag.clone(), instance.clone());
                                            }
                                            (true, Some(_)) => {}
                                            (_, None) => {
                                                metric.insert_tag(tag.clone(), instance.clone());
                                            }
                                        }
                                    }
                                    if let Some(EndpointInfo {
                                        tag,
                                        endpoint,
                                        honor_label,
                                    }) = &endpoint_info
                                    {
                                        match (honor_label, metric.tag_value(tag)) {
                                            (false, Some(old_endpoint)) => {
                                                metric.insert_tag(
                                                    format!("exported_{}", tag),
                                                    old_endpoint,
                                                );
                                                metric.insert_tag(tag.clone(), endpoint.clone());
                                            }
                                            (true, Some(_)) => {}
                                            (_, None) => {
                                                metric.insert_tag(tag.clone(), endpoint.clone());
                                            }
                                        }
                                    }
                                    event
                                }))
                            }
                            Err(error) => {
                                if url.path() == "/" {
                                    // https://github.com/vectordotdev/vector/pull/3801#issuecomment-700723178
                                    warn!(
                                        message = PARSE_ERROR_NO_PATH,
                                        endpoint = %url,
                                    );
                                }
                                emit!(PrometheusParseError {
                                    error,
                                    url: url.clone(),
                                    body,
                                });
                                None
                            }
                        }
                    }
                    Ok((header, _)) => {
                        if header.status == hyper::StatusCode::NOT_FOUND && url.path() == "/" {
                            // https://github.com/vectordotdev/vector/pull/3801#issuecomment-700723178
                            warn!(
                                message = NOT_FOUND_NO_PATH,
                                endpoint = %url,
                            );
                        }
                        emit!(PrometheusHttpResponseError {
                            code: header.status,
                            url: url.clone(),
                        });
                        None
                    }
                    Err(error) => {
                        emit!(PrometheusHttpError {
                            error,
                            url: url.clone(),
                        });
                        None
                    }
                })
            })
            .flatten()
    })
    .flatten()
    .boxed();

    match out.send_event_stream(&mut stream).await {
        Ok(()) => {
            info!("Finished sending.");
            Ok(())
        }
        Err(error) => {
            let (count, _) = stream.size_hint();
            emit!(StreamClosedError { error, count });
            Err(())
        }
    }
}

#[cfg(all(test, feature = "sinks-prometheus"))]
mod test {
    use hyper::{
        service::{make_service_fn, service_fn},
        Body, Client, Response, Server,
    };
    use pretty_assertions::assert_eq;
    use tokio::time::{sleep, Duration};
    use warp::Filter;

    use super::*;
    use crate::{
        config,
        sinks::prometheus::exporter::PrometheusExporterConfig,
        test_util::{
            components::{
                assert_source_compliance, run_and_assert_source_compliance, HTTP_PULL_SOURCE_TAGS,
            },
            next_addr, start_topology,
        },
        Error,
    };

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<PrometheusScrapeConfig>();
    }

    #[tokio::test]
    async fn test_prometheus_honor_labels() {
        let in_addr = next_addr();

        let dummy_endpoint = warp::path!("metrics").map(|| {
                r#"
                    promhttp_metric_handler_requests_total{endpoint="http://example.com", instance="localhost:9999", code="200"} 100 1612411516789
                    "#
        });

        tokio::spawn(warp::serve(dummy_endpoint).run(in_addr));

        let config = PrometheusScrapeConfig {
            endpoints: vec![format!("http://{}/metrics", in_addr)],
            scrape_interval_secs: 1,
            instance_tag: Some("instance".to_string()),
            endpoint_tag: Some("endpoint".to_string()),
            honor_labels: true,
            query: None,
            auth: None,
            tls: None,
        };

        let events = run_and_assert_source_compliance(
            config,
            Duration::from_secs(1),
            &HTTP_PULL_SOURCE_TAGS,
        )
        .await;
        assert!(!events.is_empty());

        let metrics: Vec<_> = events
            .into_iter()
            .map(|event| event.into_metric())
            .collect();

        for metric in metrics {
            assert_eq!(
                metric.tag_value("instance"),
                Some(String::from("localhost:9999"))
            );
            assert_eq!(
                metric.tag_value("endpoint"),
                Some(String::from("http://example.com"))
            );
            assert_eq!(metric.tag_value("exported_instance"), None,);
            assert_eq!(metric.tag_value("exported_endpoint"), None,);
        }
    }

    #[tokio::test]
    async fn test_prometheus_do_not_honor_labels() {
        let in_addr = next_addr();

        let dummy_endpoint = warp::path!("metrics").map(|| {
                r#"
                    promhttp_metric_handler_requests_total{endpoint="http://example.com", instance="localhost:9999", code="200"} 100 1612411516789
                "#
        });

        tokio::spawn(warp::serve(dummy_endpoint).run(in_addr));

        let config = PrometheusScrapeConfig {
            endpoints: vec![format!("http://{}/metrics", in_addr)],
            scrape_interval_secs: 1,
            instance_tag: Some("instance".to_string()),
            endpoint_tag: Some("endpoint".to_string()),
            honor_labels: false,
            query: None,
            auth: None,
            tls: None,
        };

        let events = run_and_assert_source_compliance(
            config,
            Duration::from_secs(1),
            &HTTP_PULL_SOURCE_TAGS,
        )
        .await;
        assert!(!events.is_empty());

        let metrics: Vec<_> = events
            .into_iter()
            .map(|event| event.into_metric())
            .collect();

        for metric in metrics {
            assert_eq!(
                metric.tag_value("instance"),
                Some(format!("{}:{}", in_addr.ip(), in_addr.port()))
            );
            assert_eq!(
                metric.tag_value("endpoint"),
                Some(format!(
                    "http://{}:{}/metrics",
                    in_addr.ip(),
                    in_addr.port()
                ))
            );
            assert_eq!(
                metric.tag_value("exported_instance"),
                Some(String::from("localhost:9999"))
            );
            assert_eq!(
                metric.tag_value("exported_endpoint"),
                Some(String::from("http://example.com"))
            );
        }
    }

    #[tokio::test]
    async fn test_prometheus_request_query() {
        let in_addr = next_addr();

        let dummy_endpoint = warp::path!("metrics").and(warp::query::raw()).map(|query| {
            format!(
                r#"
                    promhttp_metric_handler_requests_total{{query="{}"}} 100 1612411516789
                "#,
                query
            )
        });

        tokio::spawn(warp::serve(dummy_endpoint).run(in_addr));

        let config = PrometheusScrapeConfig {
            endpoints: vec![format!("http://{}/metrics?key1=val1", in_addr)],
            scrape_interval_secs: 1,
            instance_tag: Some("instance".to_string()),
            endpoint_tag: Some("endpoint".to_string()),
            honor_labels: false,
            query: Some(HashMap::from([
                ("key1".to_string(), vec!["val2".to_string()]),
                (
                    "key2".to_string(),
                    vec!["val1".to_string(), "val2".to_string()],
                ),
            ])),
            auth: None,
            tls: None,
        };

        let events = run_and_assert_source_compliance(
            config,
            Duration::from_secs(1),
            &HTTP_PULL_SOURCE_TAGS,
        )
        .await;
        assert!(!events.is_empty());

        let metrics: Vec<_> = events
            .into_iter()
            .map(|event| event.into_metric())
            .collect();

        let expected = HashMap::from([
            (
                "key1".to_string(),
                vec!["val1".to_string(), "val2".to_string()],
            ),
            (
                "key2".to_string(),
                vec!["val1".to_string(), "val2".to_string()],
            ),
        ]);

        for metric in metrics {
            let query = metric.tag_value("query").expect("query must be tagged");
            let mut got: HashMap<String, Vec<String>> = HashMap::new();
            for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
                got.entry(k.to_string())
                    .or_insert_with(Vec::new)
                    .push(v.to_string());
            }
            for v in got.values_mut() {
                v.sort();
            }
            assert_eq!(got, expected);
        }
    }

    #[tokio::test]
    async fn test_prometheus_routing() {
        let in_addr = next_addr();
        let out_addr = next_addr();

        let make_svc = make_service_fn(|_| async {
            Ok::<_, Error>(service_fn(|_| async {
                Ok::<_, Error>(Response::new(Body::from(
                    r##"
                    # HELP promhttp_metric_handler_requests_total Total number of scrapes by HTTP status code.
                    # TYPE promhttp_metric_handler_requests_total counter
                    promhttp_metric_handler_requests_total{code="200"} 100 1612411516789
                    promhttp_metric_handler_requests_total{code="404"} 7 1612411516789
                    prometheus_remote_storage_samples_in_total 57011636 1612411516789
                    # A histogram, which has a pretty complex representation in the text format:
                    # HELP http_request_duration_seconds A histogram of the request duration.
                    # TYPE http_request_duration_seconds histogram
                    http_request_duration_seconds_bucket{le="0.05"} 24054 1612411516789
                    http_request_duration_seconds_bucket{le="0.1"} 33444 1612411516789
                    http_request_duration_seconds_bucket{le="0.2"} 100392 1612411516789
                    http_request_duration_seconds_bucket{le="0.5"} 129389 1612411516789
                    http_request_duration_seconds_bucket{le="1"} 133988 1612411516789
                    http_request_duration_seconds_bucket{le="+Inf"} 144320 1612411516789
                    http_request_duration_seconds_sum 53423 1612411516789
                    http_request_duration_seconds_count 144320 1612411516789
                    # Finally a summary, which has a complex representation, too:
                    # HELP rpc_duration_seconds A summary of the RPC duration in seconds.
                    # TYPE rpc_duration_seconds summary
                    rpc_duration_seconds{code="200",quantile="0.01"} 3102 1612411516789
                    rpc_duration_seconds{code="200",quantile="0.05"} 3272 1612411516789
                    rpc_duration_seconds{code="200",quantile="0.5"} 4773 1612411516789
                    rpc_duration_seconds{code="200",quantile="0.9"} 9001 1612411516789
                    rpc_duration_seconds{code="200",quantile="0.99"} 76656 1612411516789
                    rpc_duration_seconds_sum{code="200"} 1.7560473e+07 1612411516789
                    rpc_duration_seconds_count{code="200"} 2693 1612411516789
                    "##,
                )))
            }))
        });

        tokio::spawn(async move {
            if let Err(error) = Server::bind(&in_addr).serve(make_svc).await {
                error!(message = "Server error.", %error);
            }
        });

        let mut config = config::Config::builder();
        config.add_source(
            "in",
            PrometheusScrapeConfig {
                endpoints: vec![format!("http://{}", in_addr)],
                instance_tag: None,
                endpoint_tag: None,
                honor_labels: false,
                query: None,
                scrape_interval_secs: 1,
                tls: None,
                auth: None,
            },
        );
        config.add_sink(
            "out",
            &["in"],
            PrometheusExporterConfig {
                address: out_addr,
                tls: None,
                default_namespace: Some("vector".into()),
                buckets: vec![1.0, 2.0, 4.0],
                quantiles: vec![],
                distributions_as_summaries: false,
                flush_period_secs: Duration::from_secs(1),
            },
        );

        assert_source_compliance(&HTTP_PULL_SOURCE_TAGS, async move {
            let (topology, _crash) = start_topology(config.build().unwrap(), false).await;
            sleep(Duration::from_secs(1)).await;

            let response = Client::new()
                .get(format!("http://{}/metrics", out_addr).parse().unwrap())
                .await
                .unwrap();

            assert!(response.status().is_success());
            let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
            let lines = std::str::from_utf8(&body)
                .unwrap()
                .lines()
                .collect::<Vec<_>>();

            assert_eq!(lines, vec![
                "# HELP vector_http_request_duration_seconds http_request_duration_seconds",
                "# TYPE vector_http_request_duration_seconds histogram",
                "vector_http_request_duration_seconds_bucket{le=\"0.05\"} 24054 1612411516789",
                "vector_http_request_duration_seconds_bucket{le=\"0.1\"} 33444 1612411516789",
                "vector_http_request_duration_seconds_bucket{le=\"0.2\"} 100392 1612411516789",
                "vector_http_request_duration_seconds_bucket{le=\"0.5\"} 129389 1612411516789",
                "vector_http_request_duration_seconds_bucket{le=\"1\"} 133988 1612411516789",
                "vector_http_request_duration_seconds_bucket{le=\"+Inf\"} 144320 1612411516789",
                "vector_http_request_duration_seconds_sum 53423 1612411516789",
                "vector_http_request_duration_seconds_count 144320 1612411516789",
                "# HELP vector_prometheus_remote_storage_samples_in_total prometheus_remote_storage_samples_in_total",
                "# TYPE vector_prometheus_remote_storage_samples_in_total gauge",
                "vector_prometheus_remote_storage_samples_in_total 57011636 1612411516789",
                "# HELP vector_promhttp_metric_handler_requests_total promhttp_metric_handler_requests_total",
                "# TYPE vector_promhttp_metric_handler_requests_total counter",
                "vector_promhttp_metric_handler_requests_total{code=\"200\"} 100 1612411516789",
                "vector_promhttp_metric_handler_requests_total{code=\"404\"} 7 1612411516789",
                "# HELP vector_rpc_duration_seconds rpc_duration_seconds",
                "# TYPE vector_rpc_duration_seconds summary",
                "vector_rpc_duration_seconds{code=\"200\",quantile=\"0.01\"} 3102 1612411516789",
                "vector_rpc_duration_seconds{code=\"200\",quantile=\"0.05\"} 3272 1612411516789",
                "vector_rpc_duration_seconds{code=\"200\",quantile=\"0.5\"} 4773 1612411516789",
                "vector_rpc_duration_seconds{code=\"200\",quantile=\"0.9\"} 9001 1612411516789",
                "vector_rpc_duration_seconds{code=\"200\",quantile=\"0.99\"} 76656 1612411516789",
                "vector_rpc_duration_seconds_sum{code=\"200\"} 17560473 1612411516789",
                "vector_rpc_duration_seconds_count{code=\"200\"} 2693 1612411516789",
                ],
            );

            topology.stop().await;
        }).await;
    }
}

#[cfg(all(test, feature = "prometheus-integration-tests"))]
mod integration_tests {
    use tokio::time::Duration;

    use super::*;
    use crate::{
        event::{MetricKind, MetricValue},
        test_util::components::{run_and_assert_source_compliance, HTTP_PULL_SOURCE_TAGS},
    };

    #[tokio::test]
    async fn scrapes_metrics() {
        let config = PrometheusScrapeConfig {
            endpoints: vec!["http://localhost:9090/metrics".into()],
            scrape_interval_secs: 1,
            instance_tag: Some("instance".to_string()),
            endpoint_tag: Some("endpoint".to_string()),
            honor_labels: false,
            query: None,
            auth: None,
            tls: None,
        };

        let events = run_and_assert_source_compliance(
            config,
            Duration::from_secs(1),
            &HTTP_PULL_SOURCE_TAGS,
        )
        .await;
        assert!(!events.is_empty());

        let metrics: Vec<_> = events
            .into_iter()
            .map(|event| event.into_metric())
            .collect();

        let find_metric = |name: &str| {
            metrics
                .iter()
                .find(|metric| metric.name() == name)
                .unwrap_or_else(|| panic!("Missing metric {:?}", name))
        };

        // Sample some well-known metrics
        let build = find_metric("prometheus_build_info");
        assert!(matches!(build.kind(), MetricKind::Absolute));
        assert!(matches!(build.value(), &MetricValue::Gauge { .. }));
        assert!(build.tags().unwrap().contains_key("branch"));
        assert!(build.tags().unwrap().contains_key("version"));
        assert_eq!(
            build.tag_value("instance"),
            Some("localhost:9090".to_string())
        );
        assert_eq!(
            build.tag_value("endpoint"),
            Some("http://localhost:9090/metrics".to_string())
        );

        let queries = find_metric("prometheus_engine_queries");
        assert!(matches!(queries.kind(), MetricKind::Absolute));
        assert!(matches!(queries.value(), &MetricValue::Gauge { .. }));
        assert_eq!(
            queries.tag_value("instance"),
            Some("localhost:9090".to_string())
        );
        assert_eq!(
            queries.tag_value("endpoint"),
            Some("http://localhost:9090/metrics".to_string())
        );

        let go_info = find_metric("go_info");
        assert!(matches!(go_info.kind(), MetricKind::Absolute));
        assert!(matches!(go_info.value(), &MetricValue::Gauge { .. }));
        assert!(go_info.tags().unwrap().contains_key("version"));
        assert_eq!(
            go_info.tag_value("instance"),
            Some("localhost:9090".to_string())
        );
        assert_eq!(
            go_info.tag_value("endpoint"),
            Some("http://localhost:9090/metrics".to_string())
        );
    }
}
