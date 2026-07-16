{{/*
Image used by the container.
*/}}
{{- define "bjw-s.common.lib.container.field.image" -}}
  {{- $ctx := .ctx -}}
  {{- $rootContext := $ctx.rootContext -}}
  {{- $containerObject := $ctx.containerObject -}}

  {{- $imageRepo := $containerObject.image.repository -}}
  {{- $imageTag := $containerObject.image.tag -}}

  {{- if and $imageRepo $imageTag -}}
    {{- printf "%s:%s" $imageRepo $imageTag -}}
  {{- end -}}
{{- end -}}
