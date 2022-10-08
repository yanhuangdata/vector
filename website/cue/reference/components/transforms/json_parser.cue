package metadata

components: transforms: json_parser: {
	title: "JSON Parser"

	description: """
		Parses a log field value as [JSON](\(urls.json)).
		"""

	classes: {
		commonly_used: false
		development:   "deprecated"
		egress_method: "stream"
		stateful:      false
	}

	features: {
		parse: {
			format: {
				name:     "JSON"
				url:      urls.json
				versions: null
			}
		}
	}

	support: {
		requirements: []
		warnings: [
			"""
			\(json_parser._remap_deprecation_notice)

			```coffee
			.message = parse_json(.message)
			```
			""",
		]
		notices: []
	}

	configuration: {
		drop_field: {
			common:      true
			description: "If the specified `field` should be dropped (removed) after parsing. If parsing fails, the field will not be removed, irrespective of this setting."
			required:    false
			type: bool: default: true
		}
		drop_invalid: {
			description: "If `true` events with invalid JSON will be dropped, otherwise the event will be kept and passed through."
			required:    true
			type: bool: {}
		}
		field: {
			common:      true
			description: "The log field to decode as JSON. Must be a `string` value type."
			required:    false
			type: string: {
				default: "message"
				examples: ["message", "parent.child", "array[0]"]
			}
		}
		overwrite_target: {
			common:      false
			description: "If `target_field` is set and the log contains a field of the same name as the target, it will only be overwritten if this is set to `true`."
			required:    false
			type: bool: default: false
		}
		target_field: {
			common:      false
			description: "If this setting is present, the parsed JSON will be inserted into the log as a sub-object with this name. If a field with the same name already exists, the parser will fail and produce an error."
			required:    false
			type: string: {
				default: null
				examples: ["root_field", "parent.child"]
			}
		}
	}

	input: {
		logs:    true
		metrics: null
		traces:  false
	}

	how_it_works: {
		invalid_json: {
			title: "Invalid JSON"
			body: """
				If the value for the specified `field` is not valid JSON you can control keeping
				or discarding the event with the `drop_invalid` option. Setting it to `true` will
				discard the event and drop it entirely. Setting it to `false` will keep the
				event and pass it through. Note that passing through the event could cause
				problems and violate assumptions about the structure of your event.
				"""
		}

		merge_conflicts: {
			title: "Merge Conflicts"
			body:  ""
			sub_sections: [
				{
					title: "Key Conflicts"
					body: """
						Any key present in the decoded JSON will override existing keys in the event.
						"""
				},
				{
					title: "Object Conflicts"
					body: """
						If the decoded JSON includes nested fields it will be _deep_ merged into the
						event. For example, given the following event:

						```javascript
						{
						  "message": "{\"parent\": {\"child2\": \"value2\"}}",
						  "parent": {
						    "child1": "value1"
						  }
						}
						```

						Parsing the `"message"` field would result the following structure:

						```javascript
						{
						  "parent": {
						    "child1": "value1",
						    "child2": "value2"
						  }
						}
						```

						Notice that the `parent.child1` key was preserved.
						"""
				},
			]
		}
	}

	telemetry: metrics: {
		processing_errors_total: components.sources.internal_metrics.output.metrics.processing_errors_total
	}
}
