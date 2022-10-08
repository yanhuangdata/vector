package metadata

components: sinks: _splunk_hec: {
	configuration: {
		acknowledgements: {
			type: object: {
				options: {
					indexer_acknowledgements_enabled: {
						common:      false
						description: "Controls if the sink will integrate with [Splunk HEC indexer acknowledgements](\(urls.splunk_hec_indexer_acknowledgements)) for end-to-end acknowledgements."
						required:    false
						type: bool: {
							default: true
						}
					}
					query_interval: {
						common:      false
						description: "The amount of time to wait in between queries to the Splunk HEC indexer acknowledgement endpoint. Minimum of `1`."
						required:    false
						type: uint: {
							default: 10
							unit:    "seconds"
						}
					}
					retry_limit: {
						common:      false
						description: "The maximum number of times an ack id will be queried for its status. Minimum of `1`."
						required:    false
						type: uint: {
							default: 30
							unit:    null
						}
					}
					max_pending_acks: {
						common:      false
						description: "The maximum number of ack ids pending query. Once reached, the sink will begin applying backpressure."
						required:    false
						type: uint: {
							default: 1_000_000
							unit:    null
						}
					}
				}
			}
		}
		default_token: {
			description: "Default Splunk HEC token. If an event has a token set in its metadata, it will prevail over the one set here."
			required:    true
			type: string: {
				examples: ["${SPLUNK_HEC_TOKEN}", "A94A8FE5CCB19BA61C4C08"]
			}
		}
	}
	how_it_works: {
		indexer_acknowledgements: {
			title: "Indexer Acknowledgements"
			body:  """
				To provide more accurate end-to-end acknowledgements, this sink will automatically integrate (unless explicitly disabled) with
				[Splunk HEC indexer acknowledgements](\(urls.splunk_hec_indexer_acknowledgements))
				if the provided Splunk HEC token has the feature enabled. In other words, if `ackID`'s are present in Splunk
				HEC responses, this sink will store and query for the status of said `ackID`'s to confirm that data has been successfully
				delivered. Upstream sources with the Vector end-to-end acknowledgements feature enabled will wait for this sink to confirm
				delivery of events before acknowledging receipt.

				The Splunk channel required for indexer acknowledgements is created using a randomly generated UUID. By default, this sink uses the
				recommended Splunk indexer acknowledgements client behavior: querying for ack statuses every 10 seconds for a maximum of 30 attempts
				(5 minutes) per `ackID`.
				"""
		}
		splunk_channel: {
			title: "Splunk HEC Channel Header"
			body:  """
				Splunk HEC requests will include the `X-Splunk-Request-Channel` header with a randomly generated UUID as the channel value.
				Splunk requires [a channel value](\(urls.splunk_hec_channel_header)) when using indexer acknowledgements, but also accepts
				channel values when indexer acknowledgements is disabled. Thus, this channel value is included regardless of indexer
				acknowledgement settings.
				"""
		}
	}
}
