#![cfg(feature = "aws-kinesis-streams-integration-tests")]
#![cfg(test)]

use aws_sdk_kinesis::model::{Record, ShardIteratorType};
use aws_sdk_kinesis::types::DateTime;
use tokio::time::{sleep, Duration};

use super::*;
use crate::{
    aws::{create_client, AwsAuthentication, RegionOrEndpoint},
    config::{ProxyConfig, SinkConfig, SinkContext},
    sinks::{
        aws_kinesis_streams::config::KinesisClientBuilder,
        util::{
            encoding::{EncodingConfig, StandardEncodings},
            BatchConfig, Compression,
        },
    },
    test_util::{
        components::{run_and_assert_sink_compliance, AWS_SINK_TAGS},
        random_lines_with_stream, random_string,
    },
};

fn kinesis_address() -> String {
    std::env::var("KINESIS_ADDRESS").unwrap_or_else(|_| "http://localhost:4566".into())
}

#[tokio::test]
async fn kinesis_put_records() {
    let stream = gen_stream();

    ensure_stream(stream.clone()).await;

    let mut batch = BatchConfig::default();
    batch.max_events = Some(2);

    let config = KinesisSinkConfig {
        stream_name: stream.clone(),
        partition_key_field: None,
        region: RegionOrEndpoint::with_both("localstack", kinesis_address().as_str()),
        encoding: EncodingConfig::from(StandardEncodings::Text).into(),
        compression: Compression::None,
        batch,
        request: Default::default(),
        tls: Default::default(),
        auth: Default::default(),
        acknowledgements: Default::default(),
    };

    let cx = SinkContext::new_test();

    let sink = config.build(cx).await.unwrap().0;

    let timestamp = chrono::Utc::now().timestamp_millis();

    let (mut input_lines, events) = random_lines_with_stream(100, 11, None);

    run_and_assert_sink_compliance(sink, events, &AWS_SINK_TAGS).await;

    sleep(Duration::from_secs(1)).await;

    let records = fetch_records(stream, timestamp).await.unwrap();

    let mut output_lines = records
        .into_iter()
        .map(|e| String::from_utf8(e.data.unwrap().into_inner()).unwrap())
        .collect::<Vec<_>>();

    input_lines.sort();
    output_lines.sort();
    assert_eq!(output_lines, input_lines)
}

async fn fetch_records(stream_name: String, timestamp: i64) -> crate::Result<Vec<Record>> {
    let client = client().await;

    let resp = client
        .describe_stream()
        .stream_name(stream_name.clone())
        .send()
        .await?;

    let shard = resp
        .stream_description
        .unwrap()
        .shards
        .unwrap()
        .into_iter()
        .next()
        .expect("No shards");

    let resp = client
        .get_shard_iterator()
        .stream_name(stream_name)
        .shard_id(shard.shard_id.unwrap())
        .shard_iterator_type(ShardIteratorType::AtTimestamp)
        .timestamp(DateTime::from_millis(timestamp))
        .send()
        .await?;
    let shard_iterator = resp.shard_iterator.expect("No iterator age produced");

    let resp = client
        .get_records()
        .shard_iterator(shard_iterator)
        .set_limit(None)
        .send()
        .await?;
    Ok(resp.records.unwrap_or_default())
}

async fn client() -> aws_sdk_kinesis::Client {
    let auth = AwsAuthentication::test_auth();
    let proxy = ProxyConfig::default();
    let region = RegionOrEndpoint::with_both("localstack", kinesis_address());
    create_client::<KinesisClientBuilder>(
        &auth,
        region.region(),
        region.endpoint().unwrap(),
        &proxy,
        &None,
        true,
    )
    .await
    .unwrap()
}

async fn ensure_stream(stream_name: String) {
    let client = client().await;

    match client
        .create_stream()
        .stream_name(stream_name)
        .shard_count(1)
        .send()
        .await
    {
        Ok(_) => (),
        Err(error) => panic!("Unable to check the stream {:?}", error),
    };

    // Wait for localstack to persist stream, otherwise it returns ResourceNotFound errors
    // during PutRecords
    //
    // I initially tried using `wait_for` with `DescribeStream` but localstack would
    // successfully return the stream before it was able to accept PutRecords requests
    sleep(Duration::from_secs(1)).await;
}

fn gen_stream() -> String {
    format!("test-{}", random_string(10).to_lowercase())
}
