use std::path::PathBuf;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use vector_core::ByteSizeOf;

use super::util::framestream::{build_framestream_unix_source, FrameHandler};
use crate::{
    config::{log_schema, DataType, Output, SourceConfig, SourceContext, SourceDescription},
    event::Event,
    internal_events::{BytesReceived, DnstapParseError, EventsReceived},
    Result,
};

pub mod parser;
pub use parser::{parse_dnstap_data, DnstapParser};

pub mod schema;
use dnsmsg_parser::{dns_message, dns_message_parser};
pub use schema::DnstapEventSchema;

#[derive(Deserialize, Serialize, Debug)]
pub struct DnstapConfig {
    #[serde(default = "default_max_frame_length")]
    pub max_frame_length: usize,
    pub host_key: Option<String>,
    pub socket_path: PathBuf,
    raw_data_only: Option<bool>,
    pub multithreaded: Option<bool>,
    pub max_frame_handling_tasks: Option<u32>,
    pub(self) socket_file_mode: Option<u32>,
    pub socket_receive_buffer_size: Option<usize>,
    pub socket_send_buffer_size: Option<usize>,
}

fn default_max_frame_length() -> usize {
    bytesize::kib(100u64) as usize
}

impl DnstapConfig {
    pub fn new(socket_path: PathBuf) -> Self {
        Self {
            host_key: None,
            socket_path,
            ..Self::default()
        }
    }

    fn content_type(&self) -> String {
        "protobuf:dnstap.Dnstap".to_string() //content-type for framestream
    }
}

impl Default for DnstapConfig {
    fn default() -> Self {
        Self {
            host_key: Some("host".to_string()),
            max_frame_length: default_max_frame_length(),
            socket_path: PathBuf::from("/run/bind/dnstap.sock"),
            raw_data_only: None,
            multithreaded: None,
            max_frame_handling_tasks: None,
            socket_file_mode: None,
            socket_receive_buffer_size: None,
            socket_send_buffer_size: None,
        }
    }
}

inventory::submit! {
    SourceDescription::new::<DnstapConfig>("dnstap")
}

impl_generate_config_from_default!(DnstapConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "dnstap")]
impl SourceConfig for DnstapConfig {
    async fn build(&self, cx: SourceContext) -> Result<super::Source> {
        let frame_handler = DnstapFrameHandler::new(self);
        build_framestream_unix_source(frame_handler, cx.shutdown, cx.out)
    }

    fn outputs(&self) -> Vec<Output> {
        vec![Output::default(DataType::Log)]
    }

    fn source_type(&self) -> &'static str {
        "dnstap"
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}

#[derive(Clone)]
pub struct DnstapFrameHandler {
    max_frame_length: usize,
    socket_path: PathBuf,
    content_type: String,
    schema: DnstapEventSchema,
    raw_data_only: bool,
    multithreaded: bool,
    max_frame_handling_tasks: u32,
    socket_file_mode: Option<u32>,
    socket_receive_buffer_size: Option<usize>,
    socket_send_buffer_size: Option<usize>,
    host_key: String,
    timestamp_key: String,
}

impl DnstapFrameHandler {
    pub fn new(config: &DnstapConfig) -> Self {
        let timestamp_key = log_schema().timestamp_key();

        let mut schema = DnstapEventSchema::new();
        schema
            .dnstap_root_data_schema_mut()
            .set_timestamp(timestamp_key);

        let host_key = config
            .host_key
            .clone()
            .unwrap_or_else(|| log_schema().host_key().to_string());

        Self {
            max_frame_length: config.max_frame_length,
            socket_path: config.socket_path.clone(),
            content_type: config.content_type(),
            schema,
            raw_data_only: config.raw_data_only.unwrap_or(false),
            multithreaded: config.multithreaded.unwrap_or(false),
            max_frame_handling_tasks: config.max_frame_handling_tasks.unwrap_or(1000),
            socket_file_mode: config.socket_file_mode,
            socket_receive_buffer_size: config.socket_receive_buffer_size,
            socket_send_buffer_size: config.socket_send_buffer_size,
            host_key,
            timestamp_key: timestamp_key.to_string(),
        }
    }
}

impl FrameHandler for DnstapFrameHandler {
    fn content_type(&self) -> String {
        self.content_type.clone()
    }

    fn max_frame_length(&self) -> usize {
        self.max_frame_length
    }

    /**
     * Function to pass into util::framestream::build_framestream_unix_source
     * Takes a data frame from the unix socket and turns it into a Vector Event.
     **/
    fn handle_event(&self, received_from: Option<Bytes>, frame: Bytes) -> Option<Event> {
        emit!(BytesReceived {
            byte_size: frame.len(),
            protocol: "protobuf",
        });
        let mut event = Event::new_empty_log();

        let log_event = event.as_mut_log();

        if let Some(host) = received_from {
            log_event.insert(self.host_key().as_str(), host);
        }

        if self.raw_data_only {
            log_event.insert(
                self.schema.dnstap_root_data_schema().raw_data(),
                base64::encode(&frame),
            );
            emit!(EventsReceived {
                count: 1,
                byte_size: event.size_of(),
            });
            Some(event)
        } else {
            match parse_dnstap_data(&self.schema, log_event, frame) {
                Err(err) => {
                    emit!(DnstapParseError {
                        error: format!("Dnstap protobuf decode error {:?}.", err).as_str()
                    });
                    None
                }
                Ok(_) => {
                    emit!(EventsReceived {
                        count: 1,
                        byte_size: event.size_of(),
                    });
                    Some(event)
                }
            }
        }
    }
    fn socket_path(&self) -> PathBuf {
        self.socket_path.clone()
    }

    fn multithreaded(&self) -> bool {
        self.multithreaded
    }

    fn max_frame_handling_tasks(&self) -> u32 {
        self.max_frame_handling_tasks
    }

    fn socket_file_mode(&self) -> Option<u32> {
        self.socket_file_mode
    }

    fn socket_receive_buffer_size(&self) -> Option<usize> {
        self.socket_receive_buffer_size
    }

    fn socket_send_buffer_size(&self) -> Option<usize> {
        self.socket_send_buffer_size
    }

    fn host_key(&self) -> String {
        self.host_key.clone()
    }

    fn timestamp_key(&self) -> String {
        self.timestamp_key.clone()
    }
}

#[cfg(all(test, feature = "dnstap-integration-tests"))]
mod integration_tests {
    #![allow(clippy::print_stdout)] // tests

    use bollard::exec::{CreateExecOptions, StartExecOptions};
    use bollard::Docker;
    use futures::StreamExt;
    use serde_json::json;
    use tokio::time;

    use super::*;
    use crate::{
        event::Value,
        test_util::{
            components::{assert_source_compliance, SOURCE_TAGS},
            wait_for,
        },
        SourceSender,
    };

    async fn test_dnstap(raw_data: bool, query_type: &'static str) {
        assert_source_compliance(&SOURCE_TAGS, async {
            let (sender, mut recv) = SourceSender::new_test();

            tokio::spawn(async move {
                let socket = get_socket(raw_data, query_type);

                DnstapConfig {
                    max_frame_length: 102400,
                    host_key: Some("key".to_string()),
                    socket_path: socket,
                    raw_data_only: Some(raw_data),
                    multithreaded: Some(false),
                    max_frame_handling_tasks: Some(100000),
                    socket_file_mode: Some(511),
                    socket_receive_buffer_size: Some(10485760),
                    socket_send_buffer_size: Some(10485760),
                }
                .build(SourceContext::new_test(sender, None))
                .await
                .unwrap()
                .await
                .unwrap()
            });

            send_query(raw_data, query_type);

            let event = time::timeout(time::Duration::from_secs(10), recv.next())
                .await
                .expect("fetch dnstap source event timeout")
                .expect("failed to get dnstap source event from a stream");
            let mut events = vec![event];
            loop {
                match time::timeout(time::Duration::from_secs(1), recv.next()).await {
                    Ok(Some(event)) => events.push(event),
                    Ok(None) => {
                        println!("None: No event");
                        break;
                    }
                    Err(e) => {
                        println!("Error: {}", e);
                        break;
                    }
                }
            }

            verify_events(raw_data, query_type, &events);
        })
        .await;
    }

    fn send_query(raw_data: bool, query_type: &'static str) {
        tokio::spawn(async move {
            let socket_path = get_socket(raw_data, query_type);
            let (query_port, control_port) = get_bind_ports(raw_data, query_type);

            // Wait for the source to create its respective socket before telling BIND to reload, causing it to open
            // that new socket file.
            wait_for(move || {
                let path = socket_path.clone();
                async move { path.exists() }
            })
            .await;

            // Now instruct BIND to reopen its DNSTAP socket file and execute the given query.
            reload_bind_dnstap_socket(control_port).await;

            match query_type {
                "query" => {
                    nslookup(query_port).await;
                }
                "update" => {
                    nsupdate().await;
                }
                _ => (),
            }
        });
    }

    fn verify_events(raw_data: bool, query_event: &'static str, events: &[Event]) {
        if raw_data {
            assert_eq!(events.len(), 2);
            assert!(
                events.iter().all(|v| v.as_log().get("rawData") != None),
                "No rawData field!"
            );
        } else if query_event == "query" {
            assert_eq!(events.len(), 2);
            assert!(
                events
                    .iter()
                    .any(|v| v.as_log().get("messageType")
                        == Some(&Value::Bytes("ClientQuery".into()))),
                "No ClientQuery event!"
            );
            assert!(
                events.iter().any(|v| v.as_log().get("messageType")
                    == Some(&Value::Bytes("ClientResponse".into()))),
                "No ClientResponse event!"
            );
        } else if query_event == "update" {
            assert_eq!(events.len(), 4);
            assert!(
                events
                    .iter()
                    .any(|v| v.as_log().get("messageType")
                        == Some(&Value::Bytes("UpdateQuery".into()))),
                "No UpdateQuery event!"
            );
            assert!(
                events.iter().any(|v| v.as_log().get("messageType")
                    == Some(&Value::Bytes("UpdateResponse".into()))),
                "No UpdateResponse event!"
            );
            assert!(
                events
                    .iter()
                    .any(|v| v.as_log().get("messageType")
                        == Some(&Value::Bytes("AuthQuery".into()))),
                "No UpdateQuery event!"
            );
            assert!(
                events
                    .iter()
                    .any(|v| v.as_log().get("messageType")
                        == Some(&Value::Bytes("AuthResponse".into()))),
                "No UpdateResponse event!"
            );
        }

        for event in events {
            let json = serde_json::to_value(event.as_log().all_fields()).unwrap();
            match query_event {
                "query" => {
                    if json["messageType"] == json!("ClientQuery") {
                        assert_eq!(
                            json["requestData.question[0].domainName"],
                            json!("h1.example.com.")
                        );
                        assert_eq!(json["requestData.rcodeName"], json!("NoError"));
                    } else if json["messageType"] == json!("ClientResponse") {
                        assert_eq!(
                            json["responseData.answers[0].domainName"],
                            json!("h1.example.com.")
                        );
                        assert_eq!(json["responseData.answers[0].rData"], json!("10.0.0.11"));
                        assert_eq!(json["responseData.rcodeName"], json!("NoError"));
                    }
                }
                "update" => {
                    if json["messageType"] == json!("UpdateQuery") {
                        assert_eq!(
                            json["requestData.update[0].domainName"],
                            json!("dh1.example.com.")
                        );
                        assert_eq!(json["requestData.update[0].rData"], json!("10.0.0.21"));
                        assert_eq!(json["requestData.rcodeName"], json!("NoError"));
                    } else if json["messageType"] == json!("UpdateResponse") {
                        assert_eq!(json["responseData.rcodeName"], json!("NoError"));
                    }
                }
                _ => (),
            }
        }
    }

    fn get_container() -> String {
        std::env::var("CONTAINER_NAME").unwrap_or_else(|_| "vector_dnstap".into())
    }

    fn get_socket(raw_data: bool, query_type: &'static str) -> PathBuf {
        let socket_folder = std::env::var("BIND_SOCKET")
            .map(PathBuf::from)
            .expect("BIND socket directory must be specified via BIND_SOCKET");

        match query_type {
            "query" if raw_data => socket_folder.join("dnstap.sock1"),
            "query" => socket_folder.join("dnstap.sock2"),
            "update" => socket_folder.join("dnstap.sock3"),
            _ => unreachable!("no other test variants should exist"),
        }
    }

    fn get_bind_ports(raw_data: bool, query_type: &'static str) -> (&str, &str) {
        // Returns the query port and control port, respectively, for the given BIND instance.
        match query_type {
            "query" if raw_data => ("8001", "9001"),
            "query" => ("8002", "9002"),
            "update" => ("8003", "9003"),
            _ => unreachable!("no other test variants should exist"),
        }
    }

    async fn dnstap_exec(cmd: Vec<&str>) {
        let docker = Docker::connect_with_unix_defaults().expect("failed binding to docker socket");
        let config = CreateExecOptions {
            cmd: Some(cmd),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            ..Default::default()
        };
        let result = docker
            .create_exec(get_container().as_str(), config)
            .await
            .expect("failed to execute command");
        docker
            .start_exec(&result.id, None::<StartExecOptions>)
            .await
            .expect("failed to execute command");
    }

    async fn reload_bind_dnstap_socket(control_port: &str) {
        dnstap_exec(vec![
            "/usr/sbin/rndc",
            "-p",
            control_port,
            "dnstap",
            "-reopen",
        ])
        .await
    }

    async fn nslookup(port: &str) {
        dnstap_exec(vec![
            "nslookup",
            "-type=A",
            format!("-port={}", port).as_str(),
            "h1.example.com",
            "localhost",
        ])
        .await
    }

    async fn nsupdate() {
        dnstap_exec(vec!["nsupdate", "-v", "/bind3/etc/bind/nsupdate.txt"]).await
    }

    #[tokio::test]
    async fn test_dnstap_raw_event() {
        test_dnstap(true, "query").await;
    }

    #[tokio::test]
    async fn test_dnstap_query_event() {
        test_dnstap(false, "query").await;
    }

    #[tokio::test]
    async fn test_dnstap_update_event() {
        test_dnstap(false, "update").await;
    }
}
