{{/*
Convert Cronjob values to an object
*/}}
{{- define "bjw-s.common.lib.cronjob.valuesToObject" -}}
  {{- $rootContext := .rootContext -}}
  {{- $identifier := .id -}}
  {{- $objectValues := .values -}}

  {{- if not (hasKey $objectValues "pod") -}}
    {{- $_ := set $objectValues "pod" dict -}}
  {{- end -}}

  {{- $restartPolicy := default "Never" $objectValues.pod.restartPolicy -}}
  {{- $_ := set $objectValues.pod "restartPolicy" $restartPolicy -}}

  {{- /* Return the CronJob object */ -}}
  {{- $objectValues | toYaml -}}
{{- end -}}
