{{/*
This template serves as a blueprint for all configMap objects that are created
within the common library.
*/}}
{{- define "bjw-s.common.class.configMap" -}}
  {{- $rootContext := .rootContext -}}
  {{- $configMapObject := .object -}}

  {{- $labels := merge
    ($configMapObject.labels | default dict)
    (include "bjw-s.common.lib.metadata.allLabels" $rootContext | fromYaml)
  -}}
  {{- $annotations := merge
    ($configMapObject.annotations | default dict)
    (include "bjw-s.common.lib.metadata.globalAnnotations" $rootContext | fromYaml)
  -}}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ $configMapObject.name }}
  {{- with $labels }}
  labels: {{- toYaml . | nindent 4 -}}
  {{- end }}
  {{- with $annotations }}
  annotations: {{- toYaml . | nindent 4 -}}
  {{- end }}
data:
  {{- with $configMapObject.data }}
    {{- tpl (toYaml .) $rootContext | nindent 2 }}
  {{- end }}
{{- end -}}
