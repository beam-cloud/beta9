{{/*
Renders the configMap objects required by the chart.
*/}}
{{- define "bjw-s.common.render.configMaps" -}}
  {{- /* Generate named configMaps as required */ -}}
  {{- range $key, $configMap := .Values.configMaps }}
    {{- /* Enable configMap by default, but allow override */ -}}
    {{- $configMapEnabled := true -}}
    {{- if hasKey $configMap "enabled" -}}
      {{- $configMapEnabled = $configMap.enabled -}}
    {{- end -}}

    {{- if $configMapEnabled -}}
      {{- $configMapValues := (mustDeepCopy $configMap) -}}

      {{- /* Create object from the raw configMap values */ -}}
      {{- $configMapObject := (include "bjw-s.common.lib.configMap.valuesToObject" (dict "rootContext" $ "id" $key "values" $configMapValues)) | fromYaml -}}

      {{- /* Perform validations on the configMap before rendering */ -}}
      {{- include "bjw-s.common.lib.configMap.validate" (dict "rootContext" $ "object" $configMapObject) -}}

      {{/* Include the configMap class */}}
      {{- include "bjw-s.common.class.configMap" (dict "rootContext" $ "object" $configMapObject) | nindent 0 -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
