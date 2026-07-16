{{/*
Validate serviceMonitor values
*/}}
{{- define "bjw-s.common.lib.serviceMonitor.validate" -}}
  {{- $rootContext := .rootContext -}}
  {{- $serviceMonitorObject := .object -}}

  {{- if not $serviceMonitorObject.endpoints -}}
    {{- fail (printf "endpoints are required for serviceMonitor with key \"%v\"" $serviceMonitorObject.identifier) -}}
  {{- end -}}
{{- end -}}
