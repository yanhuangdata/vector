package metadata

components: sources: [Name=string]: {
	kind:     "source"
	features: _

	configuration: {
		if features.collect != _|_ {
			if features.collect.checkpoint.enabled {
				data_dir: {
					common:      false
					description: "The directory used to persist file checkpoint positions. By default, the global `data_dir` option is used. Please make sure the Vector project has write permissions to this dir."
					required:    false
					type: string: {
						default: null
						examples: ["/var/lib/vector"]
						syntax: "file_system_path"
					}
				}
			}
		}

		if features.multiline.enabled {
			multiline: {
				common:      false
				description: "Multiline parsing configuration. If not specified, multiline parsing is disabled."
				required:    false
				type: object: options: {
					condition_pattern: {
						description: "Condition regex pattern to look for. Exact behavior is configured via `mode`."
						required:    true
						sort:        3
						type: string: {
							examples: ["^[\\s]+", "\\\\$", "^(INFO|ERROR) ", ";$"]
							syntax: "regex"
						}
					}
					mode: {
						description: "Mode of operation, specifies how the `condition_pattern` is interpreted."
						required:    true
						sort:        2
						type: string: {
							enum: {
								continue_through: "All consecutive lines matching this pattern are included in the group. The first line (the line that matched the start pattern) does not need to match the `ContinueThrough` pattern. This is useful in cases such as a Java stack trace, where some indicator in the line (such as leading whitespace) indicates that it is an extension of the preceding line."
								continue_past:    "All consecutive lines matching this pattern, plus one additional line, are included in the group. This is useful in cases where a log message ends with a continuation marker, such as a backslash, indicating that the following line is part of the same message."
								halt_before:      "All consecutive lines not matching this pattern are included in the group. This is useful where a log line contains a marker indicating that it begins a new message."
								halt_with:        "All consecutive lines, up to and including the first line matching this pattern, are included in the group. This is useful where a log line ends with a termination marker, such as a semicolon."
							}
						}
					}
					start_pattern: {
						description: "Start regex pattern to look for as a beginning of the message."
						required:    true
						sort:        1
						type: string: {
							examples: ["^[^\\s]", "\\\\$", "^(INFO|ERROR) ", "[^;]$"]
							syntax: "regex"
						}
					}
					timeout_ms: {
						description: "The maximum time to wait for the continuation. Once this timeout is reached, the buffered message is guaranteed to be flushed, even if incomplete."
						required:    true
						sort:        4
						type: uint: {
							examples: [1_000, 600_000]
							unit: "milliseconds"
						}
					}
				}
			}
		}

		if features.codecs != _|_ {
			if features.codecs.enabled {
				framing: {
					common:      false
					description: "Configures in which way incoming byte sequences are split up into byte frames."
					required:    false
					type: object: options: {
						method: {
							description: "The framing method."
							required:    false
							common:      true
							type: string: {
								default: features.codecs.default_framing
								enum: {
									bytes:               "Byte frames are passed through as-is according to the underlying I/O boundaries (e.g. split between messages or stream segments)."
									character_delimited: "Byte frames which are delimited by a chosen character."
									length_delimited:    "Byte frames whose length is encoded in a header."
									newline_delimited:   "Byte frames which are delimited by a newline character."
									octet_counting:      "Byte frames according to the [octet counting](\(urls.rfc_6587_3_4_1)) format."
								}
							}
						}
						character_delimited: {
							description:   "Options for `character_delimited` framing."
							required:      true
							relevant_when: "method = `character_delimited`"
							type: object: options: {
								delimiter: {
									description: "The character used to separate frames."
									required:    true
									type: ascii_char: {
										examples: ["\n", "\t"]
									}
								}
								max_length: {
									description: "The maximum frame length limit. Any frames longer than `max_length` bytes will be discarded entirely."
									required:    false
									common:      false
									type: uint: {
										default: null
										examples: [65535, 102400]
										unit: "bytes"
									}
								}
							}
						}
						newline_delimited: {
							description:   "Options for `newline_delimited` framing."
							required:      false
							common:        false
							relevant_when: "method = `newline_delimited`"
							type: object: options: {
								max_length: {
									description: "The maximum frame length limit. Any frames longer than `max_length` bytes will be discarded entirely."
									required:    false
									common:      false
									type: uint: {
										default: null
										examples: [65535, 102400]
										unit: "bytes"
									}
								}
							}
						}
						octet_counting: {
							description:   "Options for `octet_counting` framing."
							required:      false
							common:        false
							relevant_when: "method = `octet_counting`"
							type: object: options: {
								max_length: {
									description: "The maximum frame length limit. Any frames longer than `max_length` bytes will be discarded entirely."
									required:    false
									common:      false
									type: uint: {
										default: null
										examples: [65535, 102400]
										unit: "bytes"
									}
								}
							}
						}
					}
				}
				decoding: {
					common:      false
					description: "Configures in which way frames are decoded into events."
					required:    false
					type: object: options: {
						codec: {
							description: "The decoding method."
							required:    false
							common:      true
							type: string: {
								default: "bytes"
								enum: {
									bytes:       "Events containing the byte frame as-is."
									json:        "Events being parsed from a JSON string."
									syslog:      "Events being parsed from a Syslog message."
									native:      "Events being parsed from Vector's [native protobuf format](\(urls.native_proto_schema)) ([EXPERIMENTAL](/highlights/2022-03-31-native-event-codecs))."
									native_json: "Events being parsed from Vector's [native JSON format](\(urls.native_json_schema)) ([EXPERIMENTAL](/highlights/2022-03-31-native-event-codecs))."
								}
							}
						}
					}
				}
			}
		}

		if features.encoding != _|_ {
			if features.encoding.enabled {
				encoding: {
					common:      false
					description: "Configures the encoding specific source behavior."
					required:    false
					type: object: options: {
						charset: {
							common:      false
							description: "Encoding of the source messages. Takes one of the encoding [label strings](\(urls.encoding_charset_labels)) defined as part of the [Encoding Standard](\(urls.encoding_standard)). When set, the messages are transcoded from the specified encoding to UTF-8, which is the encoding vector assumes internally for string-like data. Enable this transcoding operation if you need your data to be in UTF-8 for further processing. At the time of transcoding, any malformed sequences (that can't be mapped to UTF-8) will be replaced with [replacement character](\(urls.unicode_replacement_character)) and warnings will be logged."
							required:    false
							type: string: {
								default: null
								examples: ["utf-16le", "utf-16be"]
							}
						}
					}
				}
			}
		}

		if features.collect != _|_ {
			if features.collect.proxy != _|_ {
				if features.collect.proxy.enabled {
					proxy: configuration._proxy
				}
			}

			if features.collect.tls != _|_ {
				if features.collect.tls.enabled {
					tls: configuration._tls_connect & {_args: {
						can_verify_certificate: features.collect.tls.can_verify_certificate
						can_verify_hostname:    features.collect.tls.can_verify_hostname
						enabled_default:        features.collect.tls.enabled_default
					}}
				}
			}
		}

		if features.receive != _|_ {
			if features.receive.receive_buffer_bytes != _|_ {
				receive_buffer_bytes: {
					common:      false
					description: "Configures the receive buffer size using the `SO_RCVBUF` option on the socket."
					required:    false
					type: uint: {
						default: null
						examples: [65536]
						unit: "bytes"
					}
					if features.receive.receive_buffer_bytes.relevant_when != _|_ {
						relevant_when: features.receive.receive_buffer_bytes.relevant_when
					}
				}
			}

			if features.receive.keepalive != _|_ {
				keepalive: {
					common:      false
					description: "Configures the TCP keepalive behavior for the connection to the source."
					required:    false
					type: object: {
						examples: []
						options: {
							time_secs: {
								common:      false
								description: "The time a connection needs to be idle before sending TCP keepalive probes."
								required:    false
								type: uint: {
									default: null
									unit:    "seconds"
								}
							}
						}
					}
				}
			}

			if features.receive.tls.enabled {
				tls: configuration._tls_accept & {_args: {
					can_verify_certificate: features.receive.tls.can_verify_certificate
					enabled_default:        features.receive.tls.enabled_default
				}}
			}
		}
	}

	output: {
		logs?: [Name=string]: {
			fields: {
				_current_timestamp: {
					description: string | *"The exact time the event was ingested into Vector."
					required:    true
					type: timestamp: {}
				}

				_local_host: {
					description: string | *"The local hostname, equivalent to the `gethostname` command."
					required:    true
					type: string: {
						examples: [_values.local_host]
					}
				}

				_raw_line: {
					description: "The raw line, unparsed."
					required:    true
					type: string: {
						examples: ["2019-02-13T19:48:34+00:00 [info] Started GET \"/\" for 127.0.0.1"]
					}
				}
			}
		}
	}

	how_it_works: {
		_tls: {
			title: "Transport Layer Security (TLS)"
			body:  """
				  Vector uses [OpenSSL](\(urls.openssl)) for TLS protocols. You can
				  adjust TLS behavior via the `tls.*` options.
				  """
		}

		if features.collect != _|_ {
			if features.collect.checkpoint.enabled {
				checkpointing: {
					title: "Checkpointing"
					body: """
						Vector checkpoints the current read position after each
						successful read. This ensures that Vector resumes where it left
						off if restarted, preventing data from being read twice. The
						checkpoint positions are stored in the data directory which is
						specified via the global `data_dir` option, but can be overridden
						via the `data_dir` option in the file source directly.
						"""
				}
			}
		}

		context: {
			title: "Context"
			body:  """
				By default, the `\( Name )` source augments events with helpful
				context keys.
				"""
		}

		if features.collect != _|_ {
			if features.collect.tls != _|_ {
				if features.collect.tls.enabled {
					tls: _tls
				}
			}
		}

		if features.receive != _|_ {
			if features.receive.tls.enabled {
				tls: _tls
			}
		}
	}

	telemetry: metrics: {
		events_out_total:                 components.sources.internal_metrics.output.metrics.events_out_total
		component_sent_events_total:      components.sources.internal_metrics.output.metrics.component_sent_events_total
		component_sent_event_bytes_total: components.sources.internal_metrics.output.metrics.component_sent_event_bytes_total
	}
}
