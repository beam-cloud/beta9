{{/*
Return a service port number by name for a Service object
*/}}
{{- define "bjw-s.common.lib.service.getPortNumberByName" -}}
  {{- $rootContext := .rootContext -}}
  {{- $identifier := .serviceID -}}
  {{- $portName := .portName -}}

  {{- $service := include "bjw-s.common.lib.service.getByIdentifier" (dict "rootContext" $rootContext "id" $identifier) | fromYaml -}}

  {{- if $service -}}
    {{ $servicePort := dig "ports" $portName "port" nil $service -}}
    {{- if not (eq $servicePort nil) -}}
      {{- $servicePort -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
