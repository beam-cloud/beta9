{{/*
Validate StatefulSet values
*/}}
{{- define "bjw-s.common.lib.statefulset.validate" -}}
  {{- $rootContext := .rootContext -}}
  {{- $statefulsetValues := .object -}}

  {{- if and (ne $statefulsetValues.strategy "OnDelete") (ne $statefulsetValues.strategy "RollingUpdate") -}}
    {{- fail (printf "Not a valid strategy type for StatefulSet. (controller: %s, strategy: %s)" $statefulsetValues.identifier $statefulsetValues.strategy) -}}
  {{- end -}}

  {{- if not (empty (dig "statefulset" "volumeClaimTemplates" "" $statefulsetValues)) -}}
    {{- range $index, $volumeClaimTemplate := $statefulsetValues.statefulset.volumeClaimTemplates -}}
      {{- if empty (get . "size") -}}
        {{- fail (printf "size is required for volumeClaimTemplate. (controller: %s, volumeClaimTemplate: %s)" $statefulsetValues.identifier $volumeClaimTemplate.name) -}}
      {{- end -}}

      {{- if empty (get . "accessMode") -}}
        {{- fail (printf "accessMode is required for volumeClaimTemplate. (controller: %s, volumeClaimTemplate: %s)" $statefulsetValues.identifier $volumeClaimTemplate.name) -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
