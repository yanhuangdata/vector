package metadata

components: transforms: key_value_parser: {
	title: "Key-value Parser"

	description: """
		Loosely parses a log field's value in key-value format.
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
				name:     "KeyValue"
				url:      null
				versions: null
			}
		}
	}

	support: {
		requirements: []
		warnings: [
			"""
			\(key_value_parser._remap_deprecation_notice)

			```coffee
			.message = parse_key_value(.message)
			```
			""",
		]
		notices: [
			"""
				It is likely that the `key_value` transform will replace the `logfmt_parser` transform
				in the future since it offers a more flexible superset of that transform.
				""",
		]
	}

	configuration: {
		drop_field: {
			common:      true
			description: "If `true` will drop the specified `field` after parsing."
			required:    false
			type: bool: default: true
		}

		field: {
			common:      true
			description: "The log field containing key/value pairs to parse. Must be a `string` value."
			required:    false
			type: string: {
				default: "message"
				examples: ["message", "parent.child", "array[0]"]
			}
		}

		field_split: {
			common:      false
			description: "The character(s) to split a key/value pair on which results in a new field with an associated value. Must be a `string` value."
			required:    false
			type: string: {
				default: "="
				examples: [":", "="]
			}
		}

		overwrite_target: {
			common: false
			description: """
				If `target_field` is set and the log contains a field of the same name
				as the target, it will only be overwritten if this is set to `true`.
				"""
			required: false
			type: bool: default: false
		}

		separator: {
			common:      false
			description: "The character(s) that separate key/value pairs. Must be a `string` value."
			required:    false
			type: string: {
				default: "[whitespace]"
				examples: [",", ";", "|"]
			}
		}

		target_field: {
			common: false
			description: """
				If this setting is present, the parsed JSON will be inserted into the
				log as a sub-object with this name.
				If a field with the same name already exists, the parser will fail and
				produce an error.
				"""
			required: false
			type: string: {
				default: null
				examples: ["root_field", "parent.child"]
			}
		}

		trim_key: {
			common: false
			description: """
				Removes characters from the beginning and end of a key until a character that is not listed.
				ex: `<key>=value` would result in `key: value` with this option set to `<>`.
				"""
			required: false
			type: string: {
				default: null
				examples: ["<>", "{}"]
			}
		}

		trim_value: {
			common: false
			description: """
				Removes characters from the beginning and end of a value until a character that is not listed.
				ex: `key=<<>value>>` would result in `key: value` with this option set to `<>`.
				"""
			required: false
			type: string: {
				default: null
				examples: ["<>", "{}"]
			}
		}

		timezone: configuration._timezone
		types:    configuration._types
	}

	input: {
		logs:    true
		metrics: null
		traces:  false
	}

	how_it_works: {
		description: {
			title: "Description"
			body: """
				The Key Value Parser accepts structured data that can be split on a character, or group of characters, and extracts it into a
				json object (dictionary) of key/value pairs. The `separator` option allows you to define the character(s) to perform the initial
				splitting of the message into pairs. The `field_split` option allows you to define the character(s) which split the key from the value.
				"""
		}
	}

	examples: [
		{
			title: "Firewall log message"
			configuration: {
				field:        "message"
				field_split:  ":"
				separator:    ";"
				target_field: "data"
				trim_key:     "\""
				trim_value:   "\""
				type:         "key_value_parser"
			}
			input: log: {
				"message": "action:\"Accept\"; flags:\"802832\"; ifdir:\"inbound\"; ifname:\"eth2-05\"; logid:\"6\"; loguid:\"{0x5f0fa4d6,0x1,0x696ac072,0xc28d839a}\";"
			}
			output: log: {
				"message": "action:\"Accept\"; flags:\"802832\"; ifdir:\"inbound\"; ifname:\"eth2-05\"; logid:\"6\"; loguid:\"{0x5f0fa4d6,0x1,0x696ac072,0xc28d839a}\";"
				"data": {
					"action": "Accept"
					"flags":  "802832"
					"ifdir":  "inbound"
					"ifname": "eth2-05"
					"logid":  "6"
					"loguid": "{0x5f0fa4d6,0x1,0x696ac072,0xc28d839a}"
				}
			}
		},
	]

	telemetry: metrics: {
		processing_errors_total: components.sources.internal_metrics.output.metrics.processing_errors_total
	}
}
