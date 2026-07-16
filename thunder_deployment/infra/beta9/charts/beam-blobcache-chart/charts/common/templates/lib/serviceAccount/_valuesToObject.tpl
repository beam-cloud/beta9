{{/*
Convert ServiceAccount values to an object
*/}}
{{- define "bjw-s.common.lib.serviceAccount.valuesToObject" -}}
  {{- $rootContext := .rootContext -}}
  {{- $identifier := .id -}}
  {{- $objectValues := .values -}}

  {{- /* Determine and inject the serviceAccount name */ -}}
  {{- $serviceAccountName := "" -}}
  {{- $defaultServiceAccountName := "default" -}}
  {{- if $objectValues.create -}}
    {{- $defaultServiceAccountName = (include "bjw-s.common.lib.chart.names.fullname" $rootContext) -}}
  {{- end -}}

  {{- $serviceAccountName = default $defaultServiceAccountName $objectValues.name -}}

  {{- $_ := set $objectValues "name" $serviceAccountName -}}
  {{- $_ := set $objectValues "identifier" $identifier -}}

  {{- /* Return the serviceAccount object */ -}}
  {{- $objectValues | toYaml -}}
{{- end -}}
