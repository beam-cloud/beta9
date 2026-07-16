{{/*
Renders the networkPolicy objects required by the chart.
*/}}
{{- define "bjw-s.common.render.networkpolicies" -}}
  {{- /* Generate named networkPolicy as required */ -}}
  {{- range $key, $networkPolicy := .Values.networkpolicies }}
    {{- /* Enable networkPolicy by default, but allow override */ -}}
    {{- $networkPolicyEnabled := true -}}
    {{- if hasKey $networkPolicy "enabled" -}}
      {{- $networkPolicyEnabled = $networkPolicy.enabled -}}
    {{- end -}}

    {{- if $networkPolicyEnabled -}}
      {{- $networkPolicyValues := (mustDeepCopy $networkPolicy) -}}

      {{- /* Create object from the raw networkPolicy values */ -}}
      {{- $networkPolicyObject := (include "bjw-s.common.lib.networkpolicy.valuesToObject" (dict "rootContext" $ "id" $key "values" $networkPolicyValues)) | fromYaml -}}

      {{- /* Perform validations on the networkPolicy before rendering */ -}}
      {{- include "bjw-s.common.lib.networkpolicy.validate" (dict "rootContext" $ "object" $networkPolicyObject) -}}

      {{/* Include the networkPolicy class */}}
      {{- include "bjw-s.common.class.networkpolicy" (dict "rootContext" $ "object" $networkPolicyObject) | nindent 0 -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
