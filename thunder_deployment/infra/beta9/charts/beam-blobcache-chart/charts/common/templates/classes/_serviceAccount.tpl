{{/*
This template serves as a blueprint for ServiceAccount objects that are created
using the common library.
*/}}
{{- define "bjw-s.common.class.serviceAccount" -}}
  {{- $rootContext := .rootContext -}}
  {{- $serviceAccountObject := .object -}}

  {{- $labels := merge
    ($serviceAccountObject.labels | default dict)
    (include "bjw-s.common.lib.metadata.allLabels" $rootContext | fromYaml)
  -}}
  {{- $annotations := merge
    ($serviceAccountObject.annotations | default dict)
    (include "bjw-s.common.lib.metadata.globalAnnotations" $rootContext | fromYaml)
  -}}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ $serviceAccountObject.name }}
  {{- with $labels }}
  labels: {{- toYaml . | nindent 4 -}}
  {{- end }}
  {{- with $annotations }}
  annotations: {{- toYaml . | nindent 4 -}}
  {{- end }}
secrets:
  - name: {{ include "bjw-s.common.lib.chart.names.fullname" $rootContext }}-sa-token
{{- end -}}
