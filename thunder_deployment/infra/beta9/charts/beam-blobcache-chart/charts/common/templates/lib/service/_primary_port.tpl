{{/*
Return the primary port for a given Service object.
*/}}
{{- define "bjw-s.common.lib.service.primaryPort" -}}
  {{- $rootContext := .rootContext -}}
  {{- $serviceObject := .serviceObject -}}
  {{- $result := "" -}}

  {{- /* Loop over all enabled ports */ -}}
  {{- $enabledPorts := include "bjw-s.common.lib.service.enabledPorts" (dict "rootContext" $rootContext "serviceObject" $serviceObject) | fromYaml }}
  {{- range $name, $port := $enabledPorts -}}
    {{- /* Determine the port that has been marked as primary */ -}}
    {{- if and (hasKey $port "primary") $port.primary -}}
      {{- $result = $port -}}
    {{- end -}}
  {{- end -}}

  {{- /* Return the first port if none has been explicitly marked as primary */ -}}
  {{- if not $result -}}
    {{- $firstPortKey := keys $enabledPorts | first -}}
    {{- $result = get $enabledPorts $firstPortKey -}}
  {{- end -}}

  {{- $result | toYaml -}}
{{- end -}}
