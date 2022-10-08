package metadata

components: sinks: gcp_cloud_storage: {
	title: "GCP Cloud Storage (GCS)"

	classes: {
		commonly_used: true
		delivery:      "at_least_once"
		development:   "beta"
		egress_method: "batch"
		service_providers: ["GCP"]
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
				timeout_secs: 300.0
			}
			compression: {
				enabled: true
				default: "none"
				algorithms: ["gzip"]
				levels: ["none", "fast", "default", "best", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
			}
			encoding: {
				enabled: true
				codec: {
					enabled: true
					batched: true
					enum: ["ndjson", "text"]
				}
			}
			proxy: enabled: true
			request: {
				enabled:        true
				rate_limit_num: 1000
				headers:        false
			}
			tls: {
				enabled:                true
				can_verify_certificate: true
				can_verify_hostname:    true
				enabled_default:        false
			}
			to: {
				service: services.gcp_cloud_storage

				interface: {
					socket: {
						api: {
							title: "GCP XML Interface"
							url:   urls.gcp_xml_interface
						}
						direction: "outgoing"
						protocols: ["http"]
						ssl: "required"
					}
				}
			}
		}
	}

	support: {
		requirements: []
		warnings: []
		notices: []
	}

	configuration: {
		acl: {
			category:    "ACL"
			common:      false
			description: "Predefined ACL to apply to the created objects. For more information, see [Predefined ACLs](\(urls.gcs_predefined_acl)). If this is not set, GCS will apply a default ACL when the object is created."
			required:    false
			type: string: {
				default: null
				enum: {
					"authenticated-read":        "Gives the bucket or object owner OWNER permission, and gives all authenticated Google account holders READER permission."
					"bucket-owner-full-control": "Gives the object and bucket owners OWNER permission."
					"bucket-owner-read":         "Gives the object owner OWNER permission, and gives the bucket owner READER permission."
					"private":                   "Gives the bucket or object owner OWNER permission for a bucket or object."
					"project-private":           "Gives permission to the project team based on their roles. Anyone who is part of the team has READER permission. Project owners and project editors have OWNER permission. This the default."
					"public-read":               "Gives the bucket or object owner OWNER permission, and gives all users, both authenticated and anonymous, READER permission. When you apply this to an object, anyone on the Internet can read the object without authenticating."
				}
			}
		}
		bucket: {
			description: "The GCS bucket name."
			required:    true
			type: string: {
				examples: ["my-bucket"]
			}
		}
		credentials_path: {
			category:    "Auth"
			common:      true
			description: "The filename for a Google Cloud service account credentials JSON file used to authenticate access to the Cloud Storage API. If this is unset, Vector checks the `GOOGLE_APPLICATION_CREDENTIALS` environment variable for a filename.\n\nIf no filename is named, Vector will attempt to fetch an instance service account for the compute instance the program is running on. If Vector is not running on a GCE instance, you must define a credentials file as above."
			required:    false
			type: string: {
				default: null
				examples: ["/path/to/credentials.json"]
			}
		}
		filename_append_uuid: {
			category:    "File Naming"
			common:      false
			description: "Whether or not to append a UUID v4 token to the end of the file. This ensures there are no name collisions high volume use cases."
			required:    false
			type: bool: default: true
		}
		filename_extension: {
			category:    "File Naming"
			common:      false
			description: "The filename extension to use in the object name."
			required:    false
			type: string: {
				default: "log"
			}
		}
		filename_time_format: {
			category:    "File Naming"
			common:      false
			description: "The format of the resulting object file name. [`strftime` specifiers](\(urls.strptime_specifiers)) are supported."
			required:    false
			type: string: {
				default: "%s"
			}
		}
		key_prefix: {
			category:    "File Naming"
			common:      true
			description: "A prefix to apply to all object key names. This should be used to partition your objects, and it's important to end this value with a `/` if you want this to be the root GCS \"folder\"."
			required:    false
			type: string: {
				default: "date=%F/"
				examples: ["date=%F/", "date=%F/hour=%H/", "year=%Y/month=%m/day=%d/", "application_id={{ application_id }}/date=%F/"]
				syntax: "template"
			}
		}
		metadata: {
			common:      false
			description: "The set of metadata `key:value` pairs for the created objects. See the [GCS custom metadata](\(urls.gcs_custom_metadata)) documentation for more details."
			required:    false
			type: string: {
				default: null
				examples: []
			}
		}
		storage_class: {
			category:    "Storage"
			common:      false
			description: "The storage class for the created objects. See [the GCP storage classes](\(urls.gcs_storage_classes)) for more details."
			required:    false
			type: string: {
				default: null
				enum: {
					STANDARD: "Standard Storage is best for data that is frequently accessed and/or stored for only brief periods of time. This is the default."
					NEARLINE: "Nearline Storage is a low-cost, highly durable storage service for storing infrequently accessed data."
					COLDLINE: "Coldline Storage is a very-low-cost, highly durable storage service for storing infrequently accessed data."
					ARCHIVE:  "Archive Storage is the lowest-cost, highly durable storage service for data archiving, online backup, and disaster recovery."
				}
			}
		}
	}

	input: {
		logs:    true
		metrics: null
		traces:  false
	}

	how_it_works: {
		object_access_control_list: {
			title: "Object access control list (ACL)"
			body:  """
					GCP Cloud Storage supports access control lists (ACL) for buckets and
					objects. In the context of Vector, only object ACLs are relevant (Vector
					does not create or modify buckets). You can set the object level ACL by
					using the `acl` option, which allows you to set one of the [predefined
					ACLs](\(urls.gcs_predefined_acl)) on each created object.
					"""
		}
		object_naming: {
			title: "Object naming"
			body:  """
				By default, Vector names your GCS objects in accordance with one of two formats.

				If compression *is* enabled, this format is used:

				```text
				key_prefix><timestamp>-<uuidv4>.log.gz
				```

				Here's an example name in the compression-enabled format:

				```text
				date=2019-06-18/1560886634-fddd7a0e-fad9-4f7e-9bce-00ae5debc563.log.gz
				```

				If compression is *not* enabled, this format is used:

				```text
				<key_prefix><timestamp>-<uuidv4>.log
				```

				Here's an example name in the compression-disabled format:

				```text
				date=2019-06-18/1560886634-fddd7a0e-fad9-4f7e-9bce-00ae5debc563.log
				```

				Vector appends a [UUIDV4](\(urls.uuidv4)) token to ensure there are no name
				conflicts in the unlikely event that two Vector instances are writing data at the same
				time.

				You can control the resulting name via the [`key_prefix`](#key_prefix),
				[`filename_time_format`](#filename_time_format),
				and [`filename_append_uuid`](#filename_append_uuid) options.
				"""
		}

		storage_class: {
			title: "Storage Class"
			body:  """
					GCS offers [storage classes](\(urls.gcs_storage_classes)). You can apply
					defaults, and rules, at the bucket level or set the storage class at the
					object level. In the context of Vector only the object level is relevant
					(Vector does not create or modify buckets). You can set the storage
					class via the `storage_class` option.
					"""
		}

		tags_and_metadata: {
			title: "Tags & Metadata"
			body:  """
					Vector supports adding [custom metadata](\(urls.gcs_custom_metadata)) to
					created objects. These metadata items are a way of associating extra
					data items with the object that are not part of the uploaded data.
					"""
		}
	}

	permissions: iam: [
		{
			platform: "gcp"
			_service: "storage"

			policies: [
				{
					_action: "objects.create"
					required_for: ["operation"]
				},
				{
					_action: "objects.get"
					required_for: ["healthcheck"]
				},
			]
		},
	]

	telemetry: metrics: {
		component_sent_events_total:      components.sources.internal_metrics.output.metrics.component_sent_events_total
		component_sent_event_bytes_total: components.sources.internal_metrics.output.metrics.component_sent_event_bytes_total
		events_discarded_total:           components.sources.internal_metrics.output.metrics.events_discarded_total
		processing_errors_total:          components.sources.internal_metrics.output.metrics.processing_errors_total
	}
}
