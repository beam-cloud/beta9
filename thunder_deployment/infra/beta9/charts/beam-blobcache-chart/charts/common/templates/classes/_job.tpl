{{/*
This template serves as the blueprint for the job objects that are created
within the common library.
*/}}
{{- define "bjw-s.common.class.job" -}}
  {{- $rootContext := .rootContext -}}
  {{- $jobObject := .object -}}

  {{- $labels := merge
    (dict "app.kubernetes.io/component" $jobObject.identifier)
    ($jobObject.labels | default dict)
    (include "bjw-s.common.lib.metadata.allLabels" $rootContext | fromYaml)
  -}}
  {{- $annotations := merge
    ($jobObject.annotations | default dict)
    (include "bjw-s.common.lib.metadata.globalAnnotations" $rootContext | fromYaml)
  -}}
---
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ $jobObject.name }}
  {{- with $labels }}
  labels: {{- toYaml . | nindent 4 -}}
  {{- end }}
  {{- with $annotations }}
  annotations: {{- toYaml . | nindent 4 -}}
  {{- end }}
spec:
  {{- with $jobObject.job.suspend }}
  suspend: {{ ternary "true" "false" . }}
  {{- end }}
  {{- with $jobObject.job.ttlSecondsAfterFinished }}
  ttlSecondsAfterFinished: {{ . }}
  {{- end }}
  backoffLimit: {{ $jobObject.job.backoffLimit }}
  template:
    metadata:
      {{- with (include "bjw-s.common.lib.pod.metadata.annotations" (dict "rootContext" $rootContext "controllerObject" $jobObject)) }}
      annotations: {{ . | nindent 8 }}
      {{- end -}}
      {{- with (include "bjw-s.common.lib.pod.metadata.labels" (dict "rootContext" $rootContext "controllerObject" $jobObject)) }}
      labels: {{ . | nindent 8 }}
      {{- end }}
    spec: {{ include "bjw-s.common.lib.pod.spec" (dict "rootContext" $rootContext "controllerObject" $jobObject) | nindent 6 }}
{{- end -}}
