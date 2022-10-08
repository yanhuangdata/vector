use bytes::Bytes;
use chrono::Utc;
use codecs::{
    decoding::{DeserializerConfig, FramingConfig},
    StreamDecodingError,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use tokio_util::codec::FramedRead;
use vector_core::ByteSizeOf;

use crate::{
    codecs::{Decoder, DecodingConfig},
    config::{log_schema, GenerateConfig, Output, SourceConfig, SourceContext, SourceDescription},
    event::Event,
    internal_events::{BytesReceived, EventsReceived, StreamClosedError},
    serde::{default_decoding, default_framing_message_based},
    SourceSender,
};

mod channel;
mod list;

#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("Failed to build redis client: {}", source))]
    Client { source: redis::RedisError },
}

#[derive(Copy, Clone, Debug, Derivative, Deserialize, Serialize)]
#[derivative(Default)]
#[serde(rename_all = "lowercase")]
pub enum DataTypeConfig {
    #[derivative(Default)]
    List,
    Channel,
}

#[derive(Copy, Clone, Debug, Default, Derivative, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub struct ListOption {
    method: Method,
}

#[derive(Clone, Copy, Debug, Derivative, Deserialize, Serialize, Eq, PartialEq)]
#[derivative(Default)]
#[serde(rename_all = "lowercase")]
pub enum Method {
    #[derivative(Default)]
    Lpop,
    Rpop,
}

pub struct ConnectionInfo {
    protocol: &'static str,
    endpoint: String,
}

impl From<&redis::ConnectionInfo> for ConnectionInfo {
    fn from(redis_conn_info: &redis::ConnectionInfo) -> Self {
        let (protocol, endpoint) = match &redis_conn_info.addr {
            redis::ConnectionAddr::Tcp(host, port)
            | redis::ConnectionAddr::TcpTls { host, port, .. } => {
                ("tcp", format!("{}:{}", host, port))
            }
            redis::ConnectionAddr::Unix(path) => ("uds", path.to_string_lossy().to_string()),
        };

        Self { protocol, endpoint }
    }
}

#[derive(Clone, Debug, Derivative, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RedisSourceConfig {
    #[serde(default)]
    data_type: DataTypeConfig,
    list: Option<ListOption>,
    url: String,
    key: String,
    redis_key: Option<String>,
    #[serde(default = "default_framing_message_based")]
    #[derivative(Default(value = "default_framing_message_based()"))]
    framing: FramingConfig,
    #[serde(default = "default_decoding")]
    #[derivative(Default(value = "default_decoding()"))]
    decoding: DeserializerConfig,
}

impl GenerateConfig for RedisSourceConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"
            url = "redis://127.0.0.1:6379/0"
            key = "vector"
            data_type = "list"
            list.method = "lpop"
            redis_key = "redis_key"
            "#,
        )
        .unwrap()
    }
}

inventory::submit! {
    SourceDescription::new::<RedisSourceConfig>("redis")
}

#[async_trait::async_trait]
#[typetag::serde(name = "redis")]
impl SourceConfig for RedisSourceConfig {
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        // A key must be specified to actually query i.e. the list to pop from, or the channel to subscribe to.
        if self.key.is_empty() {
            return Err("`key` cannot be empty.".into());
        }

        let client = redis::Client::open(self.url.as_str()).context(ClientSnafu {})?;
        let connection_info = client.get_connection_info().into();
        let decoder = DecodingConfig::new(self.framing.clone(), self.decoding.clone()).build();

        match self.data_type {
            DataTypeConfig::List => {
                let list = self.list.unwrap_or_default();
                list::watch(
                    client,
                    connection_info,
                    self.key.clone(),
                    self.redis_key.clone(),
                    list.method,
                    decoder,
                    cx,
                )
                .await
            }
            DataTypeConfig::Channel => {
                channel::subscribe(
                    client,
                    connection_info,
                    self.key.clone(),
                    self.redis_key.clone(),
                    decoder,
                    cx,
                )
                .await
            }
        }
    }

    fn outputs(&self) -> Vec<Output> {
        vec![Output::default(self.decoding.output_type())]
    }

    fn source_type(&self) -> &'static str {
        "redis"
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

async fn handle_line(
    connection_info: &ConnectionInfo,
    line: String,
    key: &str,
    redis_key: Option<&str>,
    decoder: Decoder,
    out: &mut SourceSender,
) -> Result<(), ()> {
    let now = Utc::now();

    emit!(BytesReceived {
        byte_size: line.len(),
        protocol: connection_info.protocol,
    });

    let mut stream = FramedRead::new(line.as_ref(), decoder.clone());
    while let Some(next) = stream.next().await {
        match next {
            Ok((events, _byte_size)) => {
                let count = events.len();
                emit!(EventsReceived {
                    byte_size: events.size_of(),
                    count,
                });

                let events = events.into_iter().map(|mut event| {
                    if let Event::Log(ref mut log) = event {
                        log.try_insert(log_schema().source_type_key(), Bytes::from("redis"));
                        log.try_insert(log_schema().timestamp_key(), now);
                        if let Some(redis_key) = redis_key {
                            event.as_mut_log().insert(redis_key, key);
                        }
                    }
                    event
                });

                if let Err(error) = out.send_batch(events).await {
                    emit!(StreamClosedError { error, count });
                    return Err(());
                }
            }
            Err(error) => {
                // Error is logged by `crate::codecs::Decoder`, no further
                // handling is needed here.
                if !error.can_continue() {
                    break;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<RedisSourceConfig>();
    }
}

#[cfg(all(test, feature = "redis-integration-tests"))]
mod integration_test {
    use redis::AsyncCommands;

    use super::*;
    use crate::config::log_schema;
    use crate::test_util::components::{run_and_assert_source_compliance_n, SOURCE_TAGS};
    use crate::{
        test_util::{collect_n, random_string},
        SourceSender,
    };

    const REDIS_SERVER: &str = "redis://redis:6379/0";

    #[tokio::test]
    async fn redis_source_list_rpop() {
        // Push some test data into a list object which we'll read from.
        let client = redis::Client::open(REDIS_SERVER).unwrap();
        let mut conn = client.get_tokio_connection_manager().await.unwrap();

        let key = format!("test-key-{}", random_string(10));
        debug!("Test key name: {}.", key);

        let _: i32 = conn.rpush(&key, "1").await.unwrap();
        let _: i32 = conn.rpush(&key, "2").await.unwrap();
        let _: i32 = conn.rpush(&key, "3").await.unwrap();

        // Now run the source and make sure we get all three events.
        let config = RedisSourceConfig {
            data_type: DataTypeConfig::List,
            list: Some(ListOption {
                method: Method::Rpop,
            }),
            url: REDIS_SERVER.to_owned(),
            key: key.clone(),
            redis_key: None,
            framing: default_framing_message_based(),
            decoding: default_decoding(),
        };

        let events = run_and_assert_source_compliance_n(config, 3, &SOURCE_TAGS).await;

        assert_eq!(events[0].as_log()[log_schema().message_key()], "3".into());
        assert_eq!(events[1].as_log()[log_schema().message_key()], "2".into());
        assert_eq!(events[2].as_log()[log_schema().message_key()], "1".into());
    }

    #[tokio::test]
    async fn redis_source_list_lpop() {
        // Push some test data into a list object which we'll read from.
        let client = redis::Client::open(REDIS_SERVER).unwrap();
        let mut conn = client.get_tokio_connection_manager().await.unwrap();

        let key = format!("test-key-{}", random_string(10));
        debug!("Test key name: {}.", key);

        let _: i32 = conn.rpush(&key, "1").await.unwrap();
        let _: i32 = conn.rpush(&key, "2").await.unwrap();
        let _: i32 = conn.rpush(&key, "3").await.unwrap();

        // Now run the source and make sure we get all three events.
        let config = RedisSourceConfig {
            data_type: DataTypeConfig::List,
            list: Some(ListOption {
                method: Method::Lpop,
            }),
            url: REDIS_SERVER.to_owned(),
            key: key.clone(),
            redis_key: None,
            framing: default_framing_message_based(),
            decoding: default_decoding(),
        };

        let events = run_and_assert_source_compliance_n(config, 3, &SOURCE_TAGS).await;

        assert_eq!(events[0].as_log()[log_schema().message_key()], "1".into());
        assert_eq!(events[1].as_log()[log_schema().message_key()], "2".into());
        assert_eq!(events[2].as_log()[log_schema().message_key()], "3".into());
    }

    #[tokio::test]
    async fn redis_source_channel_consume_event() {
        let key = format!("test-channel-{}", random_string(10));
        let text = "test message for channel";

        // Create the source and spawn it in the background, so that we're already listening before we publish any messages.
        let config = RedisSourceConfig {
            data_type: DataTypeConfig::Channel,
            list: None,
            url: REDIS_SERVER.to_owned(),
            key: key.clone(),
            redis_key: None,
            framing: default_framing_message_based(),
            decoding: default_decoding(),
        };

        let (tx, rx) = SourceSender::new_test();
        let context = SourceContext::new_test(tx, None);
        let source = config
            .build(context)
            .await
            .expect("source should not fail to build");

        tokio::spawn(source);

        // Briefly wait to ensure the source is subscribed.
        //
        // TODO: This is a prime example of where being able to check if the shutdown signal had been polled at least
        // once would serve as the most precise indicator of "is the source ready and waiting to receieve?".
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // Now create a normal Redis client and use it to publish a bunch of message, which we'll ensure the source consumes.
        let client = redis::Client::open(REDIS_SERVER).unwrap();

        let mut async_conn = client
            .get_async_connection()
            .await
            .expect("Failed to get redis async connection.");

        for _i in 0..10000 {
            let _: i32 = async_conn.publish(key.clone(), text).await.unwrap();
        }

        let events = collect_n(rx, 10000).await;
        assert_eq!(events.len(), 10000);

        for event in events {
            assert_eq!(event.as_log()[log_schema().message_key()], text.into());
            assert_eq!(
                event.as_log()[log_schema().source_type_key()],
                "redis".into()
            );
        }
    }
}
