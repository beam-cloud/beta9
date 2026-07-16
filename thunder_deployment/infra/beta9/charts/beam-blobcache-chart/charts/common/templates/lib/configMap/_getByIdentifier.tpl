{{/*
Return a configMap Object by its Identifier.
*/}}
{{- define "bjw-s.common.lib.configMap.getByIdentifier" -}}
  {{- $rootContext := .rootContext -}}
  {{- $identifier := .id -}}

  {{- $configMapValues := dig $identifier nil $rootContext.Values.configMaps -}}
  {{- if not (empty $configMapValues) -}}
    {{- include "bjw-s.common.lib.configMap.valuesToObject" (dict "rootContext" $rootContext "id" $identifier "values" $configMapValues) -}}
  {{- end -}}
{{- end -}}
