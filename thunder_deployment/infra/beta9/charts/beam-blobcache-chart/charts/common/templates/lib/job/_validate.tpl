{{/*
Validate job values
*/}}
{{- define "bjw-s.common.lib.job.validate" -}}
  {{- $rootContext := .rootContext -}}
  {{- $jobValues := .object -}}

  {{- $allowedRestartPolicy := list "Never" "OnFailure" -}}
  {{- if not (has $jobValues.pod.restartPolicy $allowedRestartPolicy) -}}
    {{- fail (printf "Not a valid restart policy for Job (controller: %s, strategy: %s)" $jobValues.identifier $jobValues.pod.restartPolicy) -}}
  {{- end -}}
{{- end -}}
