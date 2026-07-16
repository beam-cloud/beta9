{{/*
Return a service Object by its Identifier.
*/}}
{{- define "bjw-s.common.lib.service.getByIdentifier" -}}
  {{- $rootContext := .rootContext -}}
  {{- $identifier := .id -}}

  {{- range $name, $serviceValues := $rootContext.Values.service -}}
    {{- if eq $name $identifier -}}
      {{- include "bjw-s.common.lib.service.valuesToObject" (dict "rootContext" $rootContext "id" $identifier "values" $serviceValues) -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
