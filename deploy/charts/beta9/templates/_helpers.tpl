{{- define "beta9.init" -}}
  {{/* Hack to disable main controller */}}
  {{- $_ := include "beta9.disable-main-controller" . | fromYaml | merge .Values -}}

  {{/* Make sure all variables are set properly */}}
  {{- include "bjw-s.common.loader.init" . }}

  {{/* Enforce default values */}}
  {{- $_ := include "beta9" . | fromYaml | merge .Values -}}

  {{- include "beta9.gatewayIdentityEnv" . -}}
{{- end -}}

{{- define "beta9.gatewayIdentityEnv" -}}
  {{- $main := .Values.controllers.gateway.containers.main -}}
  {{- $env := $main.env | default dict -}}
  {{- $fields := dict "BETA9_GATEWAY_SERVICE_ACCOUNT" "spec.serviceAccountName" "BETA9_GATEWAY_NAMESPACE" "metadata.namespace" -}}
  {{- if kindIs "slice" $env -}}
    {{- $merged := list -}}
    {{- range $entry := $env -}}
      {{- if not (hasKey $fields ($entry.name | default "")) -}}
        {{- $merged = append $merged $entry -}}
      {{- end -}}
    {{- end -}}
    {{- range $name, $field := $fields -}}
      {{- $merged = append $merged (dict "name" $name "valueFrom" (dict "fieldRef" (dict "fieldPath" $field))) -}}
    {{- end -}}
    {{- $_ := set $main "env" $merged -}}
  {{- else -}}
    {{- range $name, $field := $fields -}}
      {{- $_ := set $env $name (dict "valueFrom" (dict "fieldRef" (dict "fieldPath" $field))) -}}
    {{- end -}}
    {{- $_ := set $main "env" $env -}}
  {{- end -}}
{{- end -}}

{{/* The stamped config and pod checksum must describe the same release. */}}
{{- define "beta9.config" -}}
{{- $config := deepCopy (default dict .Values.config) -}}
{{- $gateway := get $config "gatewayService" | default dict -}}
{{- $_ := set $gateway "deploymentRevision" .Release.Revision -}}
{{- $_ := set $config "gatewayService" $gateway -}}
{{- toYaml $config -}}
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
    pod:
      annotations:
        secret-hash: {{ include "sha256sum" (include "beta9.config" .) }}
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
              initialDelaySeconds: 5
              successThreshold: 2
              failureThreshold: 2
              periodSeconds: 3
              timeoutSeconds: 1
              grpc:
                port: 1993
                service: readiness
          liveness:
            enabled: true
            custom: true
            spec:
              initialDelaySeconds: 10
              successThreshold: 1
              failureThreshold: 10
              periodSeconds: 3
              timeoutSeconds: 1
              grpc:
                port: 1993
                service: liveness
          startup:
            enabled: true
            custom: true
            spec:
              periodSeconds: 3
              timeoutSeconds: 1
              failureThreshold: 100
              grpc:
                port: 1993
                service: liveness
        securityContext:
          privileged: true
    hostNetwork: true
service:
  gateway:
    controller: gateway
ingress:
  gateway:
    controller: gateway
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
