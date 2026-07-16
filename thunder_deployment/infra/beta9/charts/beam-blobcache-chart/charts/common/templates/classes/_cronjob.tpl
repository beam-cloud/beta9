{{/*
This template serves as a blueprint for Cronjob objects that are created
using the common library.
*/}}
{{- define "bjw-s.common.class.cronjob" -}}
  {{- $rootContext := .rootContext -}}
  {{- $cronjobObject := .object -}}

  {{- $timeZone := "" -}}
  {{- if ge (int $rootContext.Capabilities.KubeVersion.Minor) 27 }}
    {{- $timeZone = dig "cronjob" "timeZone" "" $cronjobObject -}}
  {{- end -}}

  {{- $labels := merge
    (dict "app.kubernetes.io/component" $cronjobObject.identifier)
    ($cronjobObject.labels | default dict)
    (include "bjw-s.common.lib.metadata.allLabels" $rootContext | fromYaml)
  -}}
  {{- $annotations := merge
    ($cronjobObject.annotations | default dict)
    (include "bjw-s.common.lib.metadata.globalAnnotations" $rootContext | fromYaml)
  -}}
---
apiVersion: batch/v1
kind: CronJob
metadata:
  name: {{ $cronjobObject.name }}
  {{- with $labels }}
  labels: {{- toYaml . | nindent 4 -}}
  {{- end }}
  {{- with $annotations }}
  annotations: {{- toYaml . | nindent 4 -}}
  {{- end }}
spec:
  {{- with $cronjobObject.cronjob.suspend }}
  suspend: {{ ternary "true" "false" . }}
  {{- end }}
  concurrencyPolicy: "{{ $cronjobObject.cronjob.concurrencyPolicy }}"
  startingDeadlineSeconds: {{ $cronjobObject.cronjob.startingDeadlineSeconds }}
  {{- with $timeZone }}
  timeZone: "{{ . }}"
  {{- end }}
  schedule: "{{ $cronjobObject.cronjob.schedule }}"
  successfulJobsHistoryLimit: {{ $cronjobObject.cronjob.successfulJobsHistory }}
  failedJobsHistoryLimit: {{ $cronjobObject.cronjob.failedJobsHistory }}
  jobTemplate:
    spec:
      {{- with $cronjobObject.cronjob.ttlSecondsAfterFinished }}
      ttlSecondsAfterFinished: {{ . }}
      {{- end }}
      backoffLimit: {{ $cronjobObject.cronjob.backoffLimit }}
      template:
        metadata:
          {{- with (include "bjw-s.common.lib.pod.metadata.annotations" (dict "rootContext" $rootContext "controllerObject" $cronjobObject)) }}
          annotations: {{ . | nindent 12 }}
          {{- end -}}
          {{- with (include "bjw-s.common.lib.pod.metadata.labels" (dict "rootContext" $rootContext "controllerObject" $cronjobObject)) }}
          labels: {{ . | nindent 12 }}
          {{- end }}
        spec: {{ include "bjw-s.common.lib.pod.spec" (dict "rootContext" $rootContext "controllerObject" $cronjobObject) | nindent 10 }}
{{- end -}}
