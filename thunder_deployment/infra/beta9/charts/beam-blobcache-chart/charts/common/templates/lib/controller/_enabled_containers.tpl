{{/*
Return the enabled containers for a controller.
*/}}
{{- define "bjw-s.common.lib.controller.enabledContainers" -}}
  {{- $rootContext := .rootContext -}}
  {{- $controllerObject := .controllerObject -}}

  {{- $enabledContainers := dict -}}
  {{- range $name, $container := $controllerObject.containers -}}
    {{- if kindIs "map" $container -}}
      {{- /* Enable container by default, but allow override */ -}}
      {{- $containerEnabled := true -}}
      {{- if hasKey $container "enabled" -}}
        {{- $containerEnabled = $container.enabled -}}
      {{- end -}}

      {{- if $containerEnabled -}}
        {{- $_ := set $enabledContainers $name $container -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}

  {{- $enabledContainers | toYaml -}}
{{- end -}}
