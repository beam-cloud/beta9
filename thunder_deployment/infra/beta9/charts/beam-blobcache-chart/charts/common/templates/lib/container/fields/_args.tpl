{{/*
Args used by the container.
*/}}
{{- define "bjw-s.common.lib.container.field.args" -}}
  {{- $ctx := .ctx -}}
  {{- $containerObject := $ctx.containerObject -}}

  {{- /* Default to empty list */ -}}
  {{- $args := list -}}

  {{- /* See if an override is desired */ -}}
  {{- if not (empty (get $containerObject "args")) -}}
    {{- $option := get $containerObject "args" -}}
    {{- if not (empty $option) -}}
      {{- if kindIs "string" $option -}}
        {{- $args = append $args $option -}}
      {{- else -}}
        {{- $args = $option -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}

  {{- if not (empty $args) -}}
    {{- $args | toYaml -}}
  {{- end -}}
{{- end -}}
