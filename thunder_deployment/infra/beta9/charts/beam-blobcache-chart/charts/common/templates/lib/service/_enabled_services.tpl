{{/*
Return the enabled services.
*/}}
{{- define "bjw-s.common.lib.service.enabledServices" -}}
  {{- $rootContext := .rootContext -}}
  {{- $enabledServices := dict -}}

  {{- range $name, $service := $rootContext.Values.service -}}
    {{- if kindIs "map" $service -}}
      {{- /* Enable Service by default, but allow override */ -}}
      {{- $serviceEnabled := true -}}
      {{- if hasKey $service "enabled" -}}
        {{- $serviceEnabled = $service.enabled -}}
      {{- end -}}

      {{- if $serviceEnabled -}}
        {{- $_ := set $enabledServices $name . -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}

  {{- $enabledServices | toYaml -}}
{{- end -}}
