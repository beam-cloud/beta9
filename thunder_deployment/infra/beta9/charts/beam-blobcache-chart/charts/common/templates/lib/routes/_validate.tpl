{{/*
Validate Route values
*/}}
{{- define "bjw-s.common.lib.route.validate" -}}
  {{- $rootContext := .rootContext -}}
  {{- $routeObject := .object -}}

  {{/* Route Types */}}
  {{- $routeKind := $routeObject.kind | default "HTTPRoute"}}
  {{- if and (ne $routeKind "GRPCRoute") (ne $routeKind "HTTPRoute") (ne $routeKind "TCPRoute") (ne $routeKind "TLSRoute") (ne $routeKind "UDPRoute") }}
    {{- fail (printf "Not a valid route kind (%s)" $routeKind) }}
  {{- end }}

  {{/* Route Rules */}}

  {{- range $routeObject.rules }}
  {{- if and (.filters) (.backendRefs) }}
    {{- range .filters }}
      {{- if eq .type "RequestRedirect" }}
        {{- fail (printf "backend refs and request redirect filters cannot co-exist.")}}
      {{- end }}
    {{- end }}
  {{- end }}
  {{- end }}
{{- end -}}
