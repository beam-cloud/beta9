{{/*
This template serves as the blueprint for the DaemonSet objects that are created
within the common library.
*/}}
{{- define "bjw-s.common.class.daemonset" -}}
  {{- $rootContext := .rootContext -}}
  {{- $daemonsetObject := .object -}}

  {{- $labels := merge
    (dict "app.kubernetes.io/component" $daemonsetObject.identifier)
    ($daemonsetObject.labels | default dict)
    (include "bjw-s.common.lib.metadata.allLabels" $rootContext | fromYaml)
  -}}
  {{- $annotations := merge
    ($daemonsetObject.annotations | default dict)
    (include "bjw-s.common.lib.metadata.globalAnnotations" $rootContext | fromYaml)
  -}}
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: {{ $daemonsetObject.name }}
  {{- with $labels }}
  labels: {{- toYaml . | nindent 4 -}}
  {{- end }}
  {{- with $annotations }}
  annotations: {{- toYaml . | nindent 4 -}}
  {{- end }}
spec:
  revisionHistoryLimit: {{ $daemonsetObject.revisionHistoryLimit }}
  selector:
    matchLabels:
      app.kubernetes.io/component: {{ $daemonsetObject.identifier }}
      {{- include "bjw-s.common.lib.metadata.selectorLabels" $rootContext | nindent 6 }}
  template:
    metadata:
      annotations: {{ include "bjw-s.common.lib.pod.metadata.annotations" (dict "rootContext" $rootContext "controllerObject" $daemonsetObject) | nindent 8 }}
      labels: {{ include "bjw-s.common.lib.pod.metadata.labels" (dict "rootContext" $rootContext "controllerObject" $daemonsetObject) | nindent 8 }}
    spec: {{ include "bjw-s.common.lib.pod.spec" (dict "rootContext" $rootContext "controllerObject" $daemonsetObject) | nindent 6 }}
{{- end }}
