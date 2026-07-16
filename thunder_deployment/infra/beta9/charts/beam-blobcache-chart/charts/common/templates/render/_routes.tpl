{{/* Renders the Route objects required by the chart */}}
{{- define "bjw-s.common.render.routes" -}}
  {{- /* Generate named routes as required */ -}}
  {{- range $key, $route := .Values.route }}
    {{- /* Enable Route by default, but allow override */ -}}
    {{- $routeEnabled := true -}}
    {{- if hasKey $route "enabled" -}}
      {{- $routeEnabled = $route.enabled -}}
    {{- end -}}

    {{- if $routeEnabled -}}
      {{- $routeValues := (mustDeepCopy $route) -}}

      {{- /* Create object from the raw Route values */ -}}
      {{- $routeObject := (include "bjw-s.common.lib.route.valuesToObject" (dict "rootContext" $ "id" $key "values" $routeValues)) | fromYaml -}}

      {{- /* Perform validations on the Route before rendering */ -}}
      {{- include "bjw-s.common.lib.route.validate" (dict "rootContext" $ "object" $routeObject) -}}

      {{- /* Include the Route class */ -}}
      {{- include "bjw-s.common.class.route" (dict "rootContext" $ "object" $routeObject) | nindent 0 -}}
    {{- end }}
  {{- end }}
{{- end }}
