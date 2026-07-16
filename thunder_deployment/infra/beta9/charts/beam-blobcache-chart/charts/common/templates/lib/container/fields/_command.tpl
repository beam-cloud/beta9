{{/*
Command used by the container.
*/}}
{{- define "bjw-s.common.lib.container.field.command" -}}
  {{- $ctx := .ctx -}}
  {{- $containerObject := $ctx.containerObject -}}

  {{- /* Default to empty list */ -}}
  {{- $command := list -}}

  {{- /* See if an override is desired */ -}}
  {{- if not (empty (get $containerObject "command")) -}}
    {{- $option := get $containerObject "command" -}}
    {{- if not (empty $option) -}}
      {{- if kindIs "string" $option -}}
        {{- $command = append $command $option -}}
      {{- else -}}
        {{- $command = $option -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}

  {{- if not (empty $command) -}}
    {{- $command | toYaml -}}
  {{- end -}}
{{- end -}}
