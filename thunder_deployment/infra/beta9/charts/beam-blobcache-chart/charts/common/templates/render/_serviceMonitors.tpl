{{/*
Renders the serviceMonitor objects required by the chart.
*/}}
{{- define "bjw-s.common.render.serviceMonitors" -}}
  {{- /* Generate named serviceMonitors as required */ -}}
  {{- range $key, $serviceMonitor := .Values.serviceMonitor -}}
    {{- /* Enable ServiceMonitor by default, but allow override */ -}}
    {{- $serviceMonitorEnabled := true -}}
    {{- if hasKey $serviceMonitor "enabled" -}}
      {{- $serviceMonitorEnabled = $serviceMonitor.enabled -}}
    {{- end -}}

    {{- if $serviceMonitorEnabled -}}
      {{- $serviceMonitorValues := (mustDeepCopy $serviceMonitor) -}}

      {{- /* Create object from the raw ServiceMonitor values */ -}}
      {{- $serviceMonitorObject := (include "bjw-s.common.lib.serviceMonitor.valuesToObject" (dict "rootContext" $ "id" $key "values" $serviceMonitorValues)) | fromYaml -}}

      {{- /* Perform validations on the serviceMonitor before rendering */ -}}
      {{- include "bjw-s.common.lib.serviceMonitor.validate" (dict "rootContext" $ "object" $serviceMonitorObject) -}}

      {{/* Include the serviceMonitor class */}}
      {{- include "bjw-s.common.class.serviceMonitor" (dict "rootContext" $ "object" $serviceMonitorObject) | nindent 0 -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
