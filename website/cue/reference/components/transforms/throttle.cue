package metadata

components: transforms: throttle: {
	title: "Throttle"

	description: """
		Rate limits one or more log streams to limit load on downstream services, or to enforce usage quotas on users.
		"""

	classes: {
		commonly_used: false
		development:   "beta"
		egress_method: "stream"
		stateful:      true
	}

	features: {
		filter: {}
	}

	support: {
		requirements: []
		warnings: []
		notices: []
	}

	configuration: {
		exclude: {
			common: true
			description: """
				The set of logical conditions to exclude events from rate limiting.
				"""
			required: false
			type: string: {
				default: null
				examples: [
					#".status_code != 200 && !includes(["info", "debug"], .severity)"#,
				]
				syntax: "vrl_boolean_expression"
			}
		}
		key_field: {
			common: false
			description: """
				The name of the log field whose value will be hashed to determine if the event should be rate limited.

				Each unique key will create a buckets of related events to be rate limited separately. If left unspecified,
				or if the event doesn’t have `key_field`, the event be will not be rate limited separately.
				"""
			required: false
			type: string: {
				default: null
				examples: ["message", "{{ hostname }}"]
				syntax: "template"
			}
		}
		threshold: {
			description: """
				The number of events allowed for a given bucket per configured `window`.

				Each unique key will have its own `threshold`.
				"""
			required: true
			type: uint: {
				examples: [100, 10000]
				unit: null
			}
		}
		window: {
			description: """
				The time frame in which the configured `threshold` is applied.
				"""
			required: true
			type: uint: {
				examples: [1, 60, 86400]
				unit: "seconds"
			}
		}
	}

	input: {
		logs:    true
		metrics: null
	}

	telemetry: metrics: {
		events_discarded_total: components.sources.internal_metrics.output.metrics.events_discarded_total
	}

	examples: [
		{
			title: "Rate limiting"
			input: [
				{
					log: {
						timestamp: "2020-10-07T12:33:21.223543Z"
						message:   "First message"
						host:      "host-1.hostname.com"
					}
				},
				{
					log: {
						timestamp: "2020-10-07T12:33:21.223543Z"
						message:   "Second message"
						host:      "host-1.hostname.com"
					}
				},
			]

			configuration: {
				threshold: 1
				window:    60
			}

			output: [
				{
					log: {
						timestamp: "2020-10-07T12:33:21.223543Z"
						message:   "First message"
						host:      "host-1.hostname.com"
					}
				},
			]
		},
	]

	how_it_works: {
		rate_limiting: {
			title: "Rate Limiting"
			body:  """
				The `throttle` transform will spread load across the configured `window`, ensuring that each bucket's
				throughput averages out to the `threshold` per `window`. It utilizes a [Generic Cell Rate Algorithm](\(urls.gcra)) to
				rate limit the event stream.
				"""
			sub_sections: [
				{
					title: "Buckets"
					body: """
						The `throttle` transform buckets events into rate limiters based on the provided `key_field`, or a
						single bucket if not provided. Each bucket is rate limited separately.
						"""
				},
				{
					title: "Quotas"
					body: """
						Rate limiters use "cells" to determine if there is sufficient capacity for an event to successfully
						pass through a rate limiter. Each event passing through the transform consumes an available cell,
						if there is no available cell the event will be rate limited.

						A rate limiter is created with a maximum number of cells equal to the `threshold`, and cells replenish
						at a rate of `window` divided by `threshold`. For example, a `window` of 60 with a `threshold` of 10
						replenishes a cell every 6 seconds and allows a burst of up to 10 events.
						"""
				},
				{
					title: "Rate Limited Events"
					body: """
						The rate limiter will allow up to `threshold` number of events through and drop any further events
						for that particular bucket when the rate limiter is at capacity. Any event passed when the rate
						limiter is at capacity will be discarded and tracked by an `events_discarded_total` metric tagged
						by the bucket's `key`.
						"""
				},
			]
		}
	}
}
