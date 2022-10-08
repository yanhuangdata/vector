package metadata

components: sinks: elasticsearch: {
	title: "Elasticsearch"

	classes: {
		commonly_used: true
		delivery:      "at_least_once"
		development:   "stable"
		egress_method: "batch"
		service_providers: ["AWS", "Azure", "Elastic", "GCP"]
		stateful: false
	}

	features: {
		acknowledgements: true
		healthcheck: enabled: true
		send: {
			batch: {
				enabled:      true
				common:       false
				max_bytes:    10_000_000
				timeout_secs: 1.0
			}
			compression: {
				enabled: true
				default: "none"
				algorithms: ["none", "gzip"]
				levels: ["none", "fast", "default", "best", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
			}
			encoding: {
				enabled: true
				codec: enabled: false
			}
			proxy: enabled: true
			request: {
				enabled: true
				headers: true
			}
			tls: {
				enabled:                true
				can_verify_certificate: true
				can_verify_hostname:    true
				enabled_default:        false
			}
			to: {
				service: services.elasticsearch

				interface: {
					socket: {
						api: {
							title: "Elasticsearch bulk API"
							url:   urls.elasticsearch_bulk
						}
						direction: "outgoing"
						protocols: ["http"]
						ssl: "optional"
					}
				}
			}
		}
	}

	support: {
		requirements: [
			#"""
				Elasticsearch's Data streams feature requires Vector to be configured with the `create` `bulk.action`.
				This is *not* enabled by default.
				"""#,
		]
		warnings: []
		notices: []
	}

	configuration: {
		auth: {
			common:      false
			description: "Options for the authentication strategy."
			required:    false
			type: object: {
				examples: []
				options: components._aws.configuration.auth.type.object.options & {
					password: {
						description: "The basic authentication password."
						required:    true
						type: string: {
							examples: ["${ELASTICSEARCH_PASSWORD}", "password"]
						}
					}
					strategy: {
						description: "The authentication strategy to use."
						required:    true
						type: string: {
							enum: {
								aws:   "Authentication strategy used for [AWS' hosted Elasticsearch service](\(urls.aws_elasticsearch))."
								basic: "The [basic authentication strategy](\(urls.basic_auth))."
							}
						}
					}
					user: {
						description: "The basic authentication user name."
						required:    true
						type: string: {
							examples: ["${ELASTICSEARCH_USERNAME}", "username"]
						}
					}
				}
			}
		}
		aws: {
			common:      false
			description: "Options for the AWS connections."
			required:    false
			type: object: {
				examples: []
				options: {
					region: {
						common:      true
						description: "The [AWS region](\(urls.aws_regions)) of the target service. This defaults to the region named in the endpoint parameter, or the value of the `$AWS_REGION` or `$AWS_DEFAULT_REGION` environment variables if that cannot be determined, or \"us-east-1\"."
						required:    false
						type: string: {
							default: null
							examples: ["us-east-1"]
						}
					}
				}
			}
		}
		bulk: {
			common:      true
			description: "Options for the bulk mode."
			required:    false
			type: object: {
				examples: []
				options: {
					action: {
						common:      false
						description: """
							Action to use when making requests to the [Elasticsearch Bulk API](\(urls.elasticsearch_bulk)).
							Currently, Vector only supports `index` and `create`. `update` and `delete` actions are not supported.
							"""
						required:    false
						type: string: {
							default: "index"
							examples: ["index", "create", "{{ action }}"]
							syntax: "template"
						}
					}
					index: {
						common:      true
						description: "Index name to write events to."
						required:    false
						type: string: {
							default: "vector-%F"
							examples: ["application-{{ application_id }}-%Y-%m-%d", "vector-%Y-%m-%d"]
							syntax: "template"
						}
					}
				}
			}
		}
		data_stream: {
			common:      false
			description: "Options for the data stream mode."
			required:    false
			type: object: {
				examples: []
				options: {
					auto_routing: {
						common: false
						description: """
							Automatically routes events by deriving the data stream name using specific event fields with the `data_stream.type-data_stream.dataset-data_stream.namespace` format.

							If enabled, the data_stream.* event fields will take precedence over the data_stream.type, data_stream.dataset, and data_stream.namespace settings, but will fall back to them if any of the fields are missing from the event.
							"""
						required: false
						type: bool: default: true
					}
					dataset: {
						common:      false
						description: "The data stream dataset used to construct the data stream at index time."
						required:    false
						type: string: {
							default: "generic"
							examples: ["generic", "nginx", "{{ service }}"]
							syntax: "template"
						}
					}
					namespace: {
						common:      false
						description: "The data stream namespace used to construct the data stream at index time."
						required:    false
						type: string: {
							default: "default"
							examples: ["default", "{{ environment }}"]
							syntax: "template"
						}
					}
					sync_fields: {
						common:      false
						description: "Automatically adds and syncs the data_stream.* event fields if they are missing from the event. This ensures that fields match the name of the data stream that is receiving events."
						required:    false
						type: bool: default: true
					}
					type: {
						common:      false
						description: "The data stream type used to construct the data stream at index time."
						required:    false
						type: string: {
							default: "logs"
							examples: ["logs", "metrics", "synthetics", "{{ type }}"]
							syntax: "template"
						}
					}
				}
			}
		}
		doc_type: {
			common:      false
			description: "The `doc_type` for your index data. This is only relevant for Elasticsearch <= 6.X. If you are using >= 7.0 you do not need to set this option since Elasticsearch has removed it."
			required:    false
			type: string: {
				default: "_doc"
			}
		}
		endpoint: {
			description: "The Elasticsearch endpoint to send logs to. This should be the full URL as shown in the example."
			required:    true
			type: string: {
				examples: ["http://10.24.32.122:9000", "https://example.com", "https://user:password@example.com"]
			}
		}
		id_key: {
			common:      false
			description: "The name of the event key that should map to Elasticsearch's [`_id` field](\(urls.elasticsearch_id_field)). By default, Vector does not set the `_id` field, which allows Elasticsearch to set this automatically. You should think carefully about setting your own Elasticsearch IDs, since this can [hinder performance](\(urls.elasticsearch_id_performance))."
			required:    false
			type: string: {
				default: null
				examples: ["id", "_id"]
			}
		}
		metrics: {
			common:      false
			description: "Options for metrics."
			required:    false
			type: object: {
				examples: []
				options: {
					host_tag: {
						common:      false
						description: "Tag key that identifies the source host."
						required:    false
						type: string: {
							default: "hostname"
							examples: ["host", "hostname"]
						}
					}
					timezone: configuration._timezone
				}
			}
		}
		mode: {
			common:      true
			description: "The type of index mechanism. If `data_stream` mode is enabled, the `bulk.action` is set to `create`."
			required:    false
			type: string: {
				default: "bulk"
				examples: ["bulk", "data_stream"]
			}
		}
		pipeline: {
			common:      true
			description: "Name of the pipeline to apply."
			required:    false
			type: string: {
				default: null
				examples: ["pipeline-name"]
			}
		}
		query: {
			common:      false
			description: "Custom parameters to Elasticsearch query string."
			required:    false
			type: object: {
				examples: [{"X-Powered-By": "Vector"}]
				options: {
					"*": {
						common:      false
						description: "Any query key"
						required:    false
						type: string: {
							default: null
							examples: ["Vector"]
							syntax: "literal"
						}
					}
				}
			}
		}
		suppress_type_name: {
			common: false
			description: """
				Stop Vector from sending the `type` to Elasticsearch, which was deprecated in Elasticsearch 7.x
				and removed in Elasticsearch 8.x

				If enabled the `doc_type` option will be ignored.
				"""
			required: false
			type: bool: default: false
		}
	}

	input: {
		logs:    true
		metrics: null
		traces:  false
	}

	how_it_works: {
		conflicts: {
			title: "Conflicts"
			body:  """
				Vector [batches](#buffers-and-batches) data and flushes it to Elasticsearch's
				[`_bulk` API endpoint](\(urls.elasticsearch_bulk)). By default, all events are
				inserted via the `index` action, which replaces documents if an existing
				one has the same `id`. If `bulk.action` is configured with `create`, Elasticsearch
				does _not_ replace an existing document and instead returns a conflict error.
				"""
		}

		data_streams: {
			title: "Data streams"
			body:  """
				By default, Vector uses the `index` action with Elasticsearch's Bulk API.
				To use [Data streams](\(urls.elasticsearch_data_streams)), set the `mode` to
				`data_stream`. Use the combination of `data_stream.type`, `data_stream.dataset` and
				`data_stream.namespace` instead of `index`.
				"""
		}

		partial_failures: {
			title: "Partial Failures"
			body:  """
				By default, Elasticsearch allows partial bulk ingestion failures. This is typically
				due to Elasticsearch index mapping errors, where data keys aren't consistently
				typed. To change this behavior, refer to the Elasticsearch [`ignore_malformed`
				setting](\(urls.elasticsearch_ignore_malformed)).
				"""
		}

		aws_authentication: components._aws.how_it_works.aws_authentication
	}

	telemetry: metrics: {
		component_sent_bytes_total:       components.sources.internal_metrics.output.metrics.component_sent_bytes_total
		component_sent_events_total:      components.sources.internal_metrics.output.metrics.component_sent_events_total
		component_sent_event_bytes_total: components.sources.internal_metrics.output.metrics.component_sent_event_bytes_total
		events_discarded_total:           components.sources.internal_metrics.output.metrics.events_discarded_total
		events_out_total:                 components.sources.internal_metrics.output.metrics.events_out_total
		processing_errors_total:          components.sources.internal_metrics.output.metrics.processing_errors_total
	}
}
