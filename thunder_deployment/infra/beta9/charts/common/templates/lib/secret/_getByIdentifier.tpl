{{/*
Return a secret Object by its Identifier.
*/}}
{{- define "bjw-s.common.lib.secret.getByIdentifier" -}}
  {{- $rootContext := .rootContext -}}
  {{- $identifier := .id -}}

  {{- $secretValues := dig $identifier nil $rootContext.Values.secrets -}}
  {{- if not (empty $secretValues) -}}
    {{- include "bjw-s.common.lib.secret.valuesToObject" (dict "rootContext" $rootContext "id" $identifier "values" $secretValues) -}}
  {{- end -}}
{{- end -}}
