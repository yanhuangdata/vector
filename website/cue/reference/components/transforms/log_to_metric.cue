package metadata

components: transforms: log_to_metric: {
	title: "Log to Metric"

	description: """
		Derives one or more metric events from a log event.
		"""

	classes: {
		commonly_used: false
		development:   "stable"
		egress_method: "batch"
		stateful:      false
	}

	features: {
		convert: {}
	}

	support: {
		requirements: []
		warnings: []
		notices: []
	}

	configuration: {
		metrics: {
			description: "A table of key/value pairs representing the keys to be added to the event."
			required:    true
			type: array: items: type: object: {
				examples: []
				options: {
					field: {
						description: "The log field to use as the metric."
						required:    true
						type: string: {
							examples: ["duration", "parent.child"]
						}
					}
					increment_by_value: {
						description: """
							If `true` the metric will be incremented by the `field` value.
							If `false` the metric will be incremented by 1 regardless of the `field` value.
							"""
						required:      false
						common:        false
						relevant_when: #"type = "counter""#
						type: bool: {
							default: false
						}
					}
					kind: {
						description: """
							The kind of the metric.
							"""
						required:      false
						common:        false
						relevant_when: #"type = "counter""#
						type: string: {
							enum: {
								absolute:    "An absolute counter value."
								incremental: "In incremental counter value."
							}
							default: "incremental"
						}
					}
					name: {
						description: "The name of the metric. Defaults to `<field>_total` for `counter` and `<field>` for `gauge`."
						required:    false
						common:      true
						type: string: {
							default: null
							examples: ["duration_total"]
							syntax: "template"
						}
					}
					namespace: {
						description: "The namespace of the metric."
						required:    false
						common:      true
						type: string: {
							default: null
							examples: ["service"]
							syntax: "template"
						}
					}
					tags: {
						description: "Key/value pairs representing [metric tags](\(urls.vector_metric)#tags)."
						required:    false
						common:      true
						type: object: {
							examples: [
								{
									host:   "${HOSTNAME}"
									region: "us-east-1"
									status: "{{status}}"
								},
							]
							options: {
								"*": {
									description: """
	                      Key/value pairs representing [metric tags](\(urls.vector_metric)#tags).
	                      Environment variables and field interpolation is allowed.
	                      """
									required:    true
									type: "*": {}
								}
							}
						}
					}
					type: {
						description: "The metric type."
						required:    true
						type: string: {
							enum: {
								counter:   "A [counter metric type](\(urls.vector_metric)#counter)."
								gauge:     "A [gauge metric type](\(urls.vector_metric)#gauge)."
								histogram: "A [distribution metric type](\(urls.vector_metric)#histogram) with histogram statistic."
								set:       "A [set metric type](\(urls.vector_metric)#set)."
								summary:   "A [distribution metric type](\(urls.vector_metric)#distribution) with summary statistic."
							}
						}
					}
				}
			}
		}
	}

	input: {
		logs:    true
		metrics: null
		traces:  false
	}

	output: metrics: {
		counter:      output._passthrough_counter
		distribution: output._passthrough_distribution
		gauge:        output._passthrough_gauge
		set:          output._passthrough_set
	}

	examples: [
		{
			title: "Counter"
			notes: "This example demonstrates counting HTTP status codes."

			configuration: {
				metrics: [
					{
						type:      "counter"
						field:     "status"
						name:      "response_total"
						namespace: "service"
						tags: {
							status: "{{status}}"
							host:   "{{host}}"
						}
					},
				]
			}

			input: log: {
				host:    "10.22.11.222"
				message: "Sent 200 in 54.2ms"
				status:  200
			}
			output: [{metric: {
				kind:      "incremental"
				name:      "response_total"
				namespace: "service"
				tags: {
					status: "200"
					host:   "10.22.11.222"
				}
				counter: {
					value: 1.0
				}
			}}]
		},
		{
			title: "Sum"
			notes: "In this example we'll demonstrate computing a sum by computing the total of orders placed."

			configuration: {
				metrics: [
					{
						type:               "counter"
						field:              "total"
						name:               "order_total"
						increment_by_value: true
						tags: {
							host: "{{host}}"
						}
					},
				]
			}

			input: log: {
				host:    "10.22.11.222"
				message: "Order placed for $122.20"
				total:   122.2
			}
			output: [{metric: {
				kind: "incremental"
				name: "order_total"
				tags: {
					host: "10.22.11.222"
				}
				counter: {
					value: 122.2
				}
			}}]
		},
		{
			title: "Gauges"
			notes: "In this example we'll demonstrate creating a gauge that represents the current CPU load averages."

			configuration: {
				metrics: [
					{
						type:  "gauge"
						field: "1m_load_avg"
						tags: {
							host: "{{host}}"
						}
					},
					{
						type:  "gauge"
						field: "5m_load_avg"
						tags: {
							host: "{{host}}"
						}
					},
					{
						type:  "gauge"
						field: "15m_load_avg"
						tags: {
							host: "{{host}}"
						}
					},
				]
			}

			input: log: {
				host:           "10.22.11.222"
				message:        "CPU activity sample"
				"1m_load_avg":  78.2
				"5m_load_avg":  56.2
				"15m_load_avg": 48.7
			}
			output: [
				{metric: {
					kind: "absolute"
					name: "1m_load_avg"
					tags: {
						host: "10.22.11.222"
					}
					gauge: {
						value: 78.2
					}
				}},
				{metric: {
					kind: "absolute"
					name: "5m_load_avg"
					tags: {
						host: "10.22.11.222"
					}
					gauge: {
						value: 56.2
					}
				}},
				{metric: {
					kind: "absolute"
					name: "15m_load_avg"
					tags: {
						host: "10.22.11.222"
					}
					gauge: {
						value: 48.7
					}
				}},
			]
		},
		{
			title: "Histogram distribution"
			notes: "This example demonstrates capturing timings in your logs to compute histogram."

			configuration: {
				metrics: [
					{
						type:  "histogram"
						field: "time"
						name:  "time_ms"
						tags: {
							status: "{{status}}"
							host:   "{{host}}"
						}
					},
				]
			}

			input: log: {
				host:    "10.22.11.222"
				message: "Sent 200 in 54.2ms"
				status:  200
				time:    54.2
			}
			output: [{metric: {
				kind: "incremental"
				name: "time_ms"
				tags: {
					status: "200"
					host:   "10.22.11.222"
				}
				distribution: {
					samples: [{value: 54.2, rate: 1}]
					statistic: "histogram"
				}
			}}]
		},
		{
			title: "Summary distribution"
			notes: "This example demonstrates capturing timings in your logs to compute summary."

			configuration: {
				metrics: [
					{
						type:  "summary"
						field: "time"
						name:  "time_ms"
						tags: {
							status: "{{status}}"
							host:   "{{host}}"
						}
					},
				]
			}

			input: log: {
				host:    "10.22.11.222"
				message: "Sent 200 in 54.2ms"
				status:  200
				time:    54.2
			}
			output: [{metric: {
				kind: "incremental"
				name: "time_ms"
				tags: {
					status: "200"
					host:   "10.22.11.222"
				}
				distribution: {
					samples: [{value: 54.2, rate: 1}]
					statistic: "summary"
				}
			}}]
		},
		{
			title: "Set"
			notes: """
				In this example we'll demonstrate how to use sets. Sets are primarily a Statsd concept
				that represent the number of unique values seen for a given metric.
				The idea is that you pass the unique/high-cardinality value as the metric value
				and the metric store will count the number of unique values seen.
				"""
			configuration: {
				metrics: [
					{
						type:      "set"
						field:     "remote_addr"
						namespace: "{{branch}}"
						tags: {
							host: "{{host}}"
						}
					},
				]
			}

			input: log: {
				host:        "10.22.11.222"
				message:     "Sent 200 in 54.2ms"
				remote_addr: "233.221.232.22"
				branch:      "dev"
			}
			output: [{metric: {
				kind:      "incremental"
				name:      "remote_addr"
				namespace: "dev"
				tags: {
					host: "10.22.11.222"
				}
				set: {
					values: ["233.221.232.22"]
				}
			}}]
		},
	]

	how_it_works: {
		multiple_metrics: {
			title: "Multiple Metrics"
			body: """
				For clarification, when you convert a single `log` event into multiple `metric`
				events, the `metric` events are not emitted as a single array. They are emitted
				individually, and the downstream components treat them as individual events.
				Downstream components are not aware they were derived from a single log event.
				"""
		}
		reducing: {
			title: "Reducing"
			body:  """
				It's important to understand that this transform does not reduce multiple logs
				to a single metric. Instead, this transform converts logs into granular
				individual metrics that can then be reduced at the edge. Where the reduction
				happens depends on your metrics storage. For example, the
				[`prometheus_exporter` sink](\(urls.vector_sinks)/prometheus_exporter) will reduce logs in the sink itself
				for the next scrape, while other metrics sinks will proceed to forward the
				individual metrics for reduction in the metrics storage itself.
				"""
		}
		null_fields: {
			title: "Null Fields"
			body: """
				If the target log `field` contains a `null` value it will ignored, and a metric
				will not be emitted.
				"""
		}
	}

	telemetry: metrics: {
		processing_errors_total: components.sources.internal_metrics.output.metrics.processing_errors_total
	}
}
