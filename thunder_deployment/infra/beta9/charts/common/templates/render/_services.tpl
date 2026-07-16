{{/*
Renders the Service objects required by the chart.
*/}}
{{- define "bjw-s.common.render.services" -}}
  {{- /* Generate named Services as required */ -}}
  {{- $enabledServices := (include "bjw-s.common.lib.service.enabledServices" (dict "rootContext" $) | fromYaml ) -}}
  {{- range $key, $svc := $enabledServices -}}
    {{- $serviceValues := (mustDeepCopy $svc) -}}

    {{- /* Create object from the raw Service values */ -}}
    {{- $serviceObject := (include "bjw-s.common.lib.service.valuesToObject" (dict "rootContext" $ "id" $key "values" $serviceValues)) | fromYaml -}}

    {{- /* Perform validations on the Service before rendering */ -}}
    {{- include "bjw-s.common.lib.service.validate" (dict "rootContext" $ "object" $serviceObject) -}}

    {{- /* Include the Service class */ -}}
    {{- include "bjw-s.common.class.service" (dict "rootContext" $ "object" $serviceObject) | nindent 0 -}}
  {{- end -}}
{{- end -}}
