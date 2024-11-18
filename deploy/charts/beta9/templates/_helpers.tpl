{{- define "beta9.init" -}}
  {{/* Hack to disable main controller */}}
  {{- $_ := include "beta9.disable-main-controller" . | fromYaml | merge .Values -}}

  {{/* Make sure all variables are set properly */}}
  {{- include "bjw-s.common.loader.init" . }}

  {{/* Enforce default values */}}
  {{- $_ := include "beta9" . | fromYaml | merge .Values -}}
{{- end -}}


{{/* Disable main controller */}}
{{- define "beta9.disable-main-controller" -}}
controllers:
  main:
    enabled: false
service:
  main:
    enabled: false
ingress:
  main:
    enabled: false
route:
  main:
    enabled: false
serviceMonitor:
  main:
    enabled: false
networkpolicies:
  main:
    enabled: false
{{- end -}}


{{/* Define hard coded defaults */}}
{{- define "beta9" -}}
controllers:
  gateway:
    type: deployment
    annotations:
      secret-hash: {{ include "sha256sum" (toYaml .Values.config) }}
    containers:
      main:
        command:
        - /usr/local/bin/gateway
        image:
          repository: {{ .Values.images.gateway.repository }}
          tag: "{{ .Values.images.gateway.tag | default .Chart.AppVersion }}"
          pullPolicy: {{ .Values.images.gateway.pullPolicy | default "IfNotPresent" }}
        probes:
          readiness:
            enabled: true
            custom: true
            spec:
              successThreshold: 1
              failureThreshold: 3
              initialDelaySeconds: 5
              periodSeconds: 5
              timeoutSeconds: 1
              httpGet:
                path: /api/v1/health
                port: 1994
        securityContext:
          privileged: true
    hostNetwork: true
  proxy:
    type: deployment
    containers:
      main:
        command:
        - /usr/local/bin/proxy
        image:
          repository: {{ .Values.images.proxy.repository }}
          tag: "{{ .Values.images.proxy.tag | default .Chart.AppVersion }}"
          pullPolicy: {{ .Values.images.proxy.pullPolicy | default "IfNotPresent" }}
        securityContext:
          privileged: true
    hostNetwork: true
service:
  gateway:
    controller: gateway
ingress:
  gateway:
    controller: gateway
serviceAccount:
  create: true
persistence:
  config-helm:
    enabled: {{ if .Values.config }}true{{ else }}false{{ end }}
    type: secret
    name: beta9-config-helm
    globalMounts:
    - path: /etc/beta9.d/config.yaml
      subPath: config.yaml
      readOnly: true
{{- end -}}


{{/*Adds labels to custom manifests.*/}}
{{- define "manifests.metadata" -}}
metadata:
  labels:
    {{ include "bjw-s.common.lib.metadata.allLabels" . | nindent 4 }}
{{- end -}}


{{- define "sha256sum" -}}
{{- printf "%s" (. | sha256sum) | quote -}}
{{- end -}}
