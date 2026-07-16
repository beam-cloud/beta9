{{/*
Returns the items in a map that have a certain key
*/}}
{{- define "bjw-s.common.lib.getMapItemsWithKey" -}}
  {{- $map := .map -}}
  {{- $keyToFind := .key -}}
  {{- $output := dict -}}

  {{- if not (empty $keyToFind) -}}
    {{- range $key, $item := $map -}}
      {{- if not (empty (dig $keyToFind nil $item)) -}}
        {{- $_ := set $output $key $item -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}
  {{- $output | toYaml -}}
{{- end }}
