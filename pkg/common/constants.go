package common

import "github.com/beam-cloud/beam/pkg/types"

const (
	BeamAppLabel                  string = "beam/app_id"
	BeamAppDeploymentLabel        string = "beam/app_deployment_id"
	BeamAppRequestBucketNameLabel string = "beam/app_request_bucket_name"
)

const (
	TriggerTypeRestApi string = "rest_api"
	TriggerTypeWebhook string = "webhook"
	TriggerTypeCronJob string = "cron_job"
	TriggerTypeASGI    string = "asgi"
	DefaultAWSRegion   string = "us-east-1"
)

var (
	BucketMissedHealthChecksThreshold int    = 5
	BucketHealthCheckIntervalS        int64  = 10
	SchedulerHost                     string = "scheduler.%s.svc.cluster.local:2003"
)

var DefaultTaskPolicy = types.TaskPolicy{
	MaxRetries: 3,
	Timeout:    3600,
}
