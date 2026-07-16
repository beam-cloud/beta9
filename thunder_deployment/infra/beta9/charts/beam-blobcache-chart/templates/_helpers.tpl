{{- define "blobcache.init" -}}
  {{/* Make sure all variables are set properly */}}
  {{- include "bjw-s.common.loader.init" . }}

  {{/* Apply enforced values */}}
  {{- $_ := include "blobcache.image.tag" . | fromYaml | merge .Values -}}
{{- end -}}

{{- define "blobcache.image.tag" -}}
global:
  fullnameOverride: {{ .Values.global.fullnameOverride | default "blobcache" }}
controllers:
  main:
    enabled: true
    type: deployment
    containers:
      main:
        command:
        - blobcache
        image:
          repository: {{ .Values.image.repository }}
          tag: "{{ .Values.image.tag | default .Chart.AppVersion }}"
{{- end -}}
