{{/*
Renders the Ingress objects required by the chart.
*/}}
{{- define "bjw-s.common.render.ingresses" -}}
  {{- /* Generate named Ingresses as required */ -}}
  {{- range $key, $ingress := .Values.ingress }}
    {{- /* Enable Ingress by default, but allow override */ -}}
    {{- $ingressEnabled := true -}}
    {{- if hasKey $ingress "enabled" -}}
      {{- $ingressEnabled = $ingress.enabled -}}
    {{- end -}}

    {{- if $ingressEnabled -}}
      {{- $ingressValues := (mustDeepCopy $ingress) -}}

      {{- /* Create object from the raw ingress values */ -}}
      {{- $ingressObject := (include "bjw-s.common.lib.ingress.valuesToObject" (dict "rootContext" $ "id" $key "values" $ingressValues)) | fromYaml -}}

      {{- /* Perform validations on the ingress before rendering */ -}}
      {{- include "bjw-s.common.lib.ingress.validate" (dict "rootContext" $ "object" $ingressObject) -}}

      {{/* Include the ingress class */}}
      {{- include "bjw-s.common.class.ingress" (dict "rootContext" $ "object" $ingressObject) | nindent 0 -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
