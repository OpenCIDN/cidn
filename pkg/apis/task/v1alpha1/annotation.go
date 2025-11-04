package v1alpha1

const (
	// WebuiDisplayNameAnnotation is the annotation key for the display name.
	WebuiDisplayNameAnnotation = "webui.task.opencidn.daocloud.io/display-name"

	// WebuiGroupAnnotation is the annotation key for the group name.
	WebuiGroupAnnotation = "webui.task.opencidn.daocloud.io/group"

	// WebuiGroupIgnoreSizeAnnotation is the annotation key to ignore size when grouping.
	WebuiGroupIgnoreSizeAnnotation = "webui.task.opencidn.daocloud.io/group-ignore-size"

	// WebuiTagAnnotation is the annotation key for tags.
	// Multiple tags can be specified as a comma-separated list.
	WebuiTagAnnotation = "webui.task.opencidn.daocloud.io/tags"

	// ReleaseTTLAnnotation is the annotation key for the time-to-live duration.
	// The value should be a valid duration string (e.g., "1h", "30m", "3600s").
	ReleaseTTLAnnotation = "release.task.opencidn.daocloud.io/ttl"
)
