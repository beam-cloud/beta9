{{/*
Implementation of Kahn's algorithm based on
https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm

source: https://github.com/dastrobu/helm-charts/blob/main/environment-variables/templates/_kahn.tpl
*/}}
{{- define "bjw-s.common.lib.kahn" -}}
  {{- $graph := .graph -}}
  {{- if empty $graph -}}
    {{- $_ := set . "out" list -}}
  {{- else -}}
    {{- $S := list -}}

    {{- range $node, $edges := $graph -}}
      {{- if empty $edges -}}
        {{- $S = append $S $node -}}
      {{- end -}}
    {{- end -}}

    {{- if empty $S -}}
      {{- fail (printf "graph is cyclic or has bad edge definitions. Remaining graph is:\n%s" ( .graph | toYaml ) ) }}
    {{- end -}}

    {{- $n := first $S -}}
    {{- $_ := unset $graph $n -}}

    {{- range $node, $edges := $graph -}}
      {{- $_ := set $graph $node ( without $edges $n ) -}}
    {{- end -}}

    {{- $args := dict "graph" $graph "out" list -}}
    {{- include "bjw-s.common.lib.kahn" $args -}}
    {{- $_ = set . "out" ( concat ( list $n ) $args.out ) -}}
  {{- end -}}
{{- end }}
