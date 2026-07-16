{{/*
Renders the Secret objects required by the chart.
*/}}
{{- define "bjw-s.common.render.secrets" -}}
  {{- /* Generate named Secrets as required */ -}}
  {{- range $key, $secret := .Values.secrets }}
    {{- /* Enable Secret by default, but allow override */ -}}
    {{- $secretEnabled := true -}}
    {{- if hasKey $secret "enabled" -}}
      {{- $secretEnabled = $secret.enabled -}}
    {{- end -}}

    {{- if $secretEnabled -}}
      {{- $secretValues := (mustDeepCopy $secret) -}}

      {{- /* Create object from the raw Secret values */ -}}
      {{- $secretObject := (include "bjw-s.common.lib.secret.valuesToObject" (dict "rootContext" $ "id" $key "values" $secretValues)) | fromYaml -}}

      {{- /* Perform validations on the Secret before rendering */ -}}
      {{- include "bjw-s.common.lib.secret.validate" (dict "rootContext" $ "object" $secretObject) -}}

      {{/* Include the Secret class */}}
      {{- include "bjw-s.common.class.secret" (dict "rootContext" $ "object" $secretObject) | nindent 0 -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
