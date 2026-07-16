{{/*
Convert ingress values to an object
*/}}
{{- define "bjw-s.common.lib.ingress.valuesToObject" -}}
  {{- $rootContext := .rootContext -}}
  {{- $identifier := .id -}}
  {{- $objectValues := .values -}}

  {{- /* Determine and inject the ingress name */ -}}
  {{- $objectName := (include "bjw-s.common.lib.chart.names.fullname" $rootContext) -}}

  {{- if $objectValues.nameOverride -}}
    {{- $objectName = printf "%s-%s" $objectName $objectValues.nameOverride -}}
  {{- else -}}
    {{- if ne $identifier "main" -}}
      {{- $objectName = printf "%s-%s" $objectName $identifier -}}
    {{- end -}}
  {{- end -}}
  {{- $_ := set $objectValues "name" $objectName -}}
  {{- $_ := set $objectValues "identifier" $identifier -}}

  {{- /* Return the ingress object */ -}}
  {{- $objectValues | toYaml -}}
{{- end -}}
