{{/*
This template serves as a blueprint for Deployment objects that are created
using the common library.
*/}}
{{- define "bjw-s.common.class.deployment" -}}
  {{- $rootContext := .rootContext -}}
  {{- $deploymentObject := .object -}}

  {{- $labels := merge
    (dict "app.kubernetes.io/component" $deploymentObject.identifier)
    ($deploymentObject.labels | default dict)
    (include "bjw-s.common.lib.metadata.allLabels" $rootContext | fromYaml)
  -}}
  {{- $annotations := merge
    ($deploymentObject.annotations | default dict)
    (include "bjw-s.common.lib.metadata.globalAnnotations" $rootContext | fromYaml)
  -}}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ $deploymentObject.name }}
  {{- with $labels }}
  labels: {{- toYaml . | nindent 4 -}}
  {{- end }}
  {{- with $annotations }}
  annotations: {{- toYaml . | nindent 4 -}}
  {{- end }}
spec:
  revisionHistoryLimit: {{ $deploymentObject.revisionHistoryLimit }}
  {{- if not (eq $deploymentObject.replicas nil) }}
  replicas: {{ $deploymentObject.replicas }}
  {{- end }}
  strategy:
    type: {{ $deploymentObject.strategy }}
    {{- with $deploymentObject.rollingUpdate }}
      {{- if and (eq $deploymentObject.strategy "RollingUpdate") (or .surge .unavailable) }}
    rollingUpdate:
        {{- with .unavailable }}
      maxUnavailable: {{ . }}
        {{- end }}
        {{- with .surge }}
      maxSurge: {{ . }}
        {{- end }}
      {{- end }}
    {{- end }}
  selector:
    matchLabels:
      app.kubernetes.io/component: {{ $deploymentObject.identifier }}
      {{- include "bjw-s.common.lib.metadata.selectorLabels" $rootContext | nindent 6 }}
  template:
    metadata:
      {{- with (include "bjw-s.common.lib.pod.metadata.annotations" (dict "rootContext" $rootContext "controllerObject" $deploymentObject)) }}
      annotations: {{ . | nindent 8 }}
      {{- end -}}
      {{- with (include "bjw-s.common.lib.pod.metadata.labels" (dict "rootContext" $rootContext "controllerObject" $deploymentObject)) }}
      labels: {{ . | nindent 8 }}
      {{- end }}
    spec: {{ include "bjw-s.common.lib.pod.spec" (dict "rootContext" $rootContext "controllerObject" $deploymentObject) | nindent 6 }}
{{- end -}}
