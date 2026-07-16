{{/*
Validate CronJob values
*/}}
{{- define "bjw-s.common.lib.cronjob.validate" -}}
  {{- $rootContext := .rootContext -}}
  {{- $cronjobValues := .object -}}

  {{- if and (ne $cronjobValues.pod.restartPolicy "Never") (ne $cronjobValues.pod.restartPolicy "OnFailure") -}}
    {{- fail (printf "Not a valid restartPolicy type for CronJob. (controller: %s, restartPolicy: %s)" $cronjobValues.identifier $cronjobValues.pod.restartPolicy) }}
  {{- end -}}
{{- end -}}
