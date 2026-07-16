{{/*
Convert PVC values to an object
*/}}
{{- define "bjw-s.common.lib.pvc.valuesToObject" -}}
  {{- $rootContext := .rootContext -}}
  {{- $identifier := .id -}}
  {{- $objectValues := .values -}}

  {{- /* Determine and inject the PVC name */ -}}
  {{- $objectName := (include "bjw-s.common.lib.chart.names.fullname" $rootContext) -}}

  {{- if $objectValues.nameOverride -}}
    {{- if ne $objectValues.nameOverride "-" -}}
      {{- $objectName = printf "%s-%s" $objectName $objectValues.nameOverride -}}
    {{- end -}}
  {{- else -}}
    {{- $objectName = printf "%s-%s" $objectName $identifier -}}
  {{- end -}}
  {{- $_ := set $objectValues "name" $objectName -}}
  {{- $_ := set $objectValues "identifier" $identifier -}}

  {{- /* Return the PVC object */ -}}
  {{- $objectValues | toYaml -}}
{{- end -}}
