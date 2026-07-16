{{/*
Validate Deployment values
*/}}
{{- define "bjw-s.common.lib.deployment.validate" -}}
  {{- $rootContext := .rootContext -}}
  {{- $deploymentValues := .object -}}

  {{- if and (ne $deploymentValues.strategy "Recreate") (ne $deploymentValues.strategy "RollingUpdate") -}}
    {{- fail (printf "Not a valid strategy type for Deployment. (controller: %s, strategy: %s)" $deploymentValues.identifier $deploymentValues.strategy) }}
  {{- end -}}
{{- end -}}
