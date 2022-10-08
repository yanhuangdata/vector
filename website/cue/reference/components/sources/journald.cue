package metadata

components: sources: journald: {
	title: "Journald"

	classes: {
		commonly_used: true
		delivery:      "at_least_once"
		deployment_roles: ["daemon"]
		development:   "stable"
		egress_method: "batch"
		stateful:      false
	}

	features: {
		acknowledgements: true
		collect: {
			checkpoint: enabled: true
			from: {
				service: services.journald

				interface: binary: {
					name: "journalctl"
					permissions: unix: group: "systemd-journal"
				}
			}
		}
		multiline: enabled: false
	}

	support: {
		targets: {
			"x86_64-apple-darwin":   false
			"x86_64-pc-windows-msv": false
		}

		requirements: []
		warnings: []
		notices: []
	}

	installation: {
		platform_name: null
	}

	configuration: {
		acknowledgements: configuration._source_acknowledgements
		batch_size: {
			common:      false
			description: "The systemd journal is read in batches, and a checkpoint is set at the end of each batch. This option limits the size of the batch."
			required:    false
			type: uint: {
				default: 16
				unit:    null
			}
		}
		current_boot_only: {
			common:      true
			description: "Include only entries from the current boot."
			required:    false
			type: bool: default: true
		}
		since_now: {
			common:      true
			description: "Include only future entries."
			required:    false
			type: bool: default: false
		}
		exclude_units: {
			common:      true
			description: "The list of unit names to exclude from monitoring. Unit names lacking a `\".\"` will have `\".service\"` appended to make them a valid service unit name."
			required:    false
			type: array: {
				default: []
				items: type: string: {
					examples: ["badservice", "sysinit.target"]
				}
			}
		}
		exclude_matches: {
			common:      true
			description: "This list contains sets of field/value pairs that, if any are present in a journal entry, will cause the entry to be excluded from this source. If `exclude_units` is specified, it will be merged into this list."
			required:    false
			type: object: {
				examples: [
					{
						_SYSTEMD_UNIT: ["sshd.service", "ntpd.service"]
						_TRANSPORT: ["kernel"]
					},
				]
				options: {
					"*": {
						common:      false
						description: "The set of field values to match in journal entries that are to be excluded."
						required:    false
						type: array: {
							default: []
							items: type: string: {
								examples: ["sshd.service", "ntpd.service"]
							}
						}
					}
				}
			}
		}
		include_units: {
			common:      true
			description: "The list of unit names to monitor. If empty or not present, all units are accepted. Unit names lacking a `\".\"` will have `\".service\"` appended to make them a valid service unit name."
			required:    false
			type: array: {
				default: []
				items: type: string: {
					examples: ["ntpd", "sysinit.target"]
				}
			}
		}
		include_matches: {
			common:      true
			description: "This list contains sets of field/value pairs to monitor. If empty or not present, all journal fields are accepted. If `include_units` is specified, it will be merged into this list."
			required:    false
			type: object: {
				examples: [
					{
						_SYSTEMD_UNIT: ["sshd.service", "ntpd.service"]
						_TRANSPORT: ["kernel"]
					},
				]
				options: {
					"*": {
						common:      false
						description: "The set of field values to match in journal entries that are to be included."
						required:    false
						type: array: {
							default: []
							items: type: string: {
								examples: ["sshd.service", "ntpd.service"]
							}
						}
					}
				}
			}
		}
		journalctl_path: {
			common:      false
			description: "The full path of the `journalctl` executable. If not set, Vector will search the path for `journalctl`."
			required:    false
			type: string: {
				default: "journalctl"
				examples: ["/usr/local/bin/journalctl"]
			}
		}
		journal_directory: {
			common:      false
			description: "The full path of the journal directory. If not set, `journalctl` will use the default system journal paths"
			required:    false
			type: string: {
				default: null
				examples: ["/run/log/journal"]
			}
		}
	}

	output: logs: {
		event: {
			description: "A Journald event"
			fields: {
				host: fields._local_host
				message: {
					description: "The raw line from the file."
					required:    true
					type: string: {
						examples: ["53.126.150.246 - - [01/Oct/2020:11:25:58 -0400] \"GET /disintermediate HTTP/2.0\" 401 20308"]
					}
				}
				timestamp: fields._current_timestamp
				"*": {
					common:      false
					description: "Any Journald field"
					required:    false
					type: string: {
						default: null
						examples: ["/usr/sbin/ntpd", "c36e9ea52800a19d214cb71b53263a28"]
					}
				}
			}
		}
	}

	examples: [
		{
			title: "Sample Output"

			configuration: {}
			input: """
				2019-07-26 20:30:27 reply from 192.168.1.2: offset -0.001791 delay 0.000176, next query 1500s
				"""
			output: [{
				log: {
					timestamp:                _values.current_timestamp
					message:                  "reply from 192.168.1.2: offset -0.001791 delay 0.000176, next query 1500s"
					host:                     _values.local_host
					"__REALTIME_TIMESTAMP":   "1564173027000443"
					"__MONOTONIC_TIMESTAMP":  "98694000446"
					"_BOOT_ID":               "124c781146e841ae8d9b4590df8b9231"
					"SYSLOG_FACILITY":        "3"
					"_UID":                   "0"
					"_GID":                   "0"
					"_CAP_EFFECTIVE":         "3fffffffff"
					"_MACHINE_ID":            "c36e9ea52800a19d214cb71b53263a28"
					"PRIORITY":               "6"
					"_TRANSPORT":             "stdout"
					"_STREAM_ID":             "92c79f4b45c4457490ebdefece29995e"
					"SYSLOG_IDENTIFIER":      "ntpd"
					"_PID":                   "2156"
					"_COMM":                  "ntpd"
					"_EXE":                   "/usr/sbin/ntpd"
					"_CMDLINE":               "ntpd: [priv]"
					"_SYSTEMD_CGROUP":        "/system.slice/ntpd.service"
					"_SYSTEMD_UNIT":          "ntpd.service"
					"_SYSTEMD_SLICE":         "system.slice"
					"_SYSTEMD_INVOCATION_ID": "496ad5cd046d48e29f37f559a6d176f8"
				}
			}]
		},
	]

	how_it_works: {
		communication_strategy: {
			title: "Communication Strategy"
			body:  """
				To ensure the `journald` source works across all platforms, Vector interacts
				with the Systemd journal via the `journalctl` command. This is accomplished by
				spawning a [subprocess](\(urls.rust_subprocess)) that Vector interacts
				with. If the `journalctl` command is not in the environment path you can
				specify the exact location via the `journalctl_path` option. For more
				information on this communication strategy please see
				[issue #1473](\(urls.vector_issues)/1437).
				"""
		}
		non_ascii: {
			title: "Non-ASCII Messages"
			body: """
				When `journald` has stored a message that is not strict ASCII,
				`journalctl` will output it in an alternate format to prevent data
				loss. Vector handles this alternate format by translating such messages
				into UTF-8 in "lossy" mode, where characters that are not valid UTF-8
				are replaced with the Unicode replacement character, `�`.
				"""
		}
	}

	telemetry: metrics: {
		component_received_bytes_total:       components.sources.internal_metrics.output.metrics.component_received_bytes_total
		component_received_events_total:      components.sources.internal_metrics.output.metrics.component_received_events_total
		component_received_event_bytes_total: components.sources.internal_metrics.output.metrics.component_received_event_bytes_total
		events_in_total:                      components.sources.internal_metrics.output.metrics.events_in_total
		invalid_record_total:                 components.sources.internal_metrics.output.metrics.invalid_record_total
		invalid_record_bytes_total:           components.sources.internal_metrics.output.metrics.invalid_record_bytes_total
		processed_bytes_total:                components.sources.internal_metrics.output.metrics.processed_bytes_total
		processed_events_total:               components.sources.internal_metrics.output.metrics.processed_events_total
	}
}
