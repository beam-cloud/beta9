{{/*
Convert job values to an object
*/}}
{{- define "bjw-s.common.lib.job.valuesToObject" -}}
  {{- $rootContext := .rootContext -}}
  {{- $identifier := .id -}}
  {{- $objectValues := .values -}}

  {{- $restartPolicy := default "Never" $objectValues.pod.restartPolicy -}}
  {{- $_ := set $objectValues.pod "restartPolicy" $restartPolicy -}}

  {{- /* Return the Job object */ -}}
  {{- $objectValues | toYaml -}}
{{- end -}}
