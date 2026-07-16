{{/*
Validate Ingress values
*/}}
{{- define "bjw-s.common.lib.ingress.validate" -}}
  {{- $rootContext := .rootContext -}}
  {{- $ingressValues := .object -}}

  {{- range $ingressValues.hosts -}}
    {{- range .paths -}}
      {{- if empty (dig "service" "name" nil .) -}}
        {{- fail (printf "No service name configured. (ingress: %s, path: %s)" $ingressValues.identifier .path) -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
