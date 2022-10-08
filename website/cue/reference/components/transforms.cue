package metadata

components: transforms: [Name=string]: {
	_remap_deprecation_notice: """
		This transform has been deprecated in favor of the [`remap`](\(urls.vector_remap_transform))
		transform, which enables you to use [Vector Remap Language](\(urls.vrl_reference)) (VRL for short) to
		create transform logic of any degree of complexity. The examples below show how you can use VRL to
		replace this transform's functionality.
		"""

	kind: "transform"

	telemetry: metrics: {
		events_in_total:                      components.sources.internal_metrics.output.metrics.events_in_total
		events_out_total:                     components.sources.internal_metrics.output.metrics.events_out_total
		component_received_events_count:      components.sources.internal_metrics.output.metrics.component_received_events_count
		component_received_events_total:      components.sources.internal_metrics.output.metrics.component_received_events_total
		component_received_event_bytes_total: components.sources.internal_metrics.output.metrics.component_received_event_bytes_total
		utilization:                          components.sources.internal_metrics.output.metrics.utilization
		component_sent_events_total:          components.sources.internal_metrics.output.metrics.component_sent_events_total
		component_sent_event_bytes_total:     components.sources.internal_metrics.output.metrics.component_sent_event_bytes_total
	}
}
