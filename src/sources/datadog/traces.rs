use crate::{
    event::Event,
    sources::datadog::agent::{decode, handle_request, ApiKeyExtractor, ApiKeyQueryParams},
    sources::util::ErrorMessage,
    Pipeline,
};
use bytes::Bytes;
use futures::future;
use http::StatusCode;
use prost::Message;
use std::sync::Arc;
use warp::{filters::BoxedFilter, path, path::FullPath, reply::Response, Reply, Rejection, Filter};

mod dd_proto {
    include!(concat!(env!("OUT_DIR"), "/pb.rs"));
}

use dd_proto::TracePayload;

pub(crate) fn build_warp_filter(
    acknowledgements: bool,
    out: Pipeline,
    api_key_extractor: ApiKeyExtractor,
) -> BoxedFilter<(Response,)> {
    build_trace_filter(acknowledgements, out, api_key_extractor)
        .or(build_stats_filter())
        .unify()
        .boxed()
}

fn build_trace_filter(
    acknowledgements: bool,
    out: Pipeline,
    api_key_extractor: ApiKeyExtractor,
) -> BoxedFilter<(Response,)> {
    warp::post()
        .and(path!("api" / "v0.2" / "traces" / ..))
        .and(warp::path::full())
        .and(warp::header::optional::<String>("content-encoding"))
        .and(warp::header::optional::<String>("dd-api-key"))
        .and(warp::header::optional::<String>(
            "X-Datadog-Reported-Languages",
        ))
        .and(warp::query::<ApiKeyQueryParams>())
        .and(warp::body::bytes())
        .and_then(
            move |path: FullPath,
                  encoding_header: Option<String>,
                  api_token: Option<String>,
                  _reported_language: Option<String>,
                  query_params: ApiKeyQueryParams,
                  body: Bytes| {
                error!(message = "/api/v0.2/traces route is not yet fully supported.");

                let events = decode(&encoding_header, body).and_then(|body| {
                    decode_dd_trace_payload(
                        body,
                        api_key_extractor.extract(
                            path.as_str(),
                            api_token,
                            query_params.dd_api_key,
                        ),
                    ).map_err(|error| {
                        ErrorMessage::new(
                            StatusCode::UNPROCESSABLE_ENTITY,
                            format!("Error decoding Datadog traces: {:?}", error),
                        )
                    })
                });
                handle_request(events, acknowledgements, out.clone())
            },
        )
        .boxed()
}

fn build_stats_filter() -> BoxedFilter<(Response,)> {
    warp::post()
        .and(path!("api" / "v0.2" / "stats" / ..))
        .and_then(|| {
            warn!(message = "/api/v0.2/stats route is yet not supported.");
            let response: Result<Response, Rejection> = Ok(warp::reply().into_response());
            future::ready(response)
        })
        .boxed()
}

fn decode_dd_trace_payload(frame: Bytes, _: Option<Arc<str>>) -> crate::Result<Vec<Event>> {
    // Just check that we actually successfully reads a trace payload
    let payload = TracePayload::decode(frame)?;
    for _t in payload.traces.iter() {
        debug!(
            message = "Deserialized a datadog traces payload - /api/beta/sketches",
        );
    }
    Ok(vec![])
}
