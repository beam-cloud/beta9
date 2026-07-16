{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "victoria-metrics.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "victoria-metrics.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "victoria-metrics.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create the name of the service account
*/}}
{{- define "victoria-metrics.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
    {{ default (include "victoria-metrics.fullname" .) .Values.serviceAccount.name }}
{{- else -}}
    {{ default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}

{{/*
Create unified labels for victoria-metrics components
*/}}
{{- define "victoria-metrics.common.matchLabels" -}}
app.kubernetes.io/name: {{ include "victoria-metrics.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "victoria-metrics.common.metaLabels" -}}
helm.sh/chart: {{ include "victoria-metrics.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "victoria-metrics.server.labels" -}}
{{ include "victoria-metrics.server.matchLabels" . }}
{{ include "victoria-metrics.common.metaLabels" . }}
{{- end -}}

{{- define "victoria-metrics.server.matchLabels" -}}
app: {{ .Values.server.name }}
{{ include "victoria-metrics.common.matchLabels" . }}
{{- end -}}

{{/*
Create a fully qualified server name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "victoria-metrics.server.fullname" -}}
{{- if .Values.server.fullnameOverride -}}
{{- .Values.server.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- printf "%s-%s" .Release.Name .Values.server.name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s-%s" .Release.Name $name .Values.server.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}


{{- define "split-host-port" -}}
{{- $hp := split ":" . -}}
{{- printf "%s" $hp._1 -}}
{{- end -}}

{{/*
Defines the name of scrape configuration map
*/}}
{{- define "victoria-metrics.server.scrape.configname" -}}
{{- if .Values.server.scrape.configMap -}}
{{- .Values.server.scrape.configMap -}}
{{- else -}}
{{- include "victoria-metrics.server.fullname" . -}}-scrapeconfig
{{- end -}}
{{- end -}}

{{/*
Return the appropriate apiVersion for ingress.
*/}}
{{- define "victoria-metrics.ingress.apiVersion" -}}
  {{- if and (.Capabilities.APIVersions.Has "networking.k8s.io/v1") -}}
      {{- print "networking.k8s.io/v1" -}}
  {{- else if .Capabilities.APIVersions.Has "networking.k8s.io/v1beta1" -}}
    {{- print "networking.k8s.io/v1beta1" -}}
  {{- else -}}
    {{- print "extensions/v1beta1" -}}
  {{- end -}}
{{- end -}}

{{/*
Return if ingress is stable.
*/}}
{{- define "victoria-metrics.ingress.isStable" -}}
  {{- eq (include "victoria-metrics.ingress.apiVersion" .) "networking.k8s.io/v1" -}}
{{- end -}}

{{/*
Return if ingress supports ingressClassName.
*/}}
{{- define "victoria-metrics.ingress.supportsIngressClassName" -}}
  {{- or (eq (include "victoria-metrics.ingress.isStable" .) "true") (and (eq (include "victoria-metrics.ingress.apiVersion" .) "networking.k8s.io/v1beta1")) -}}
{{- end -}}

{{/*
Return if ingress supports pathType.
*/}}
{{- define "victoria-metrics.ingress.supportsPathType" -}}
  {{- or (eq (include "victoria-metrics.ingress.isStable" .) "true") (and (eq (include "victoria-metrics.ingress.apiVersion" .) "networking.k8s.io/v1beta1")) -}}
{{- end -}}

{{- define "victoria-metrics.hasInitContainer" -}}
    {{- or (gt (len .Values.server.initContainers) 0)  .Values.server.vmbackupmanager.restore.onStart.enabled -}}
{{- end -}}

{{- define "victoria-metrics.initContiners" -}}
{{- if eq (include "victoria-metrics.hasInitContainer" . ) "true" -}}
{{- with .Values.server.initContainers -}}
{{ toYaml . }}
{{- end -}}
{{- if .Values.server.vmbackupmanager.restore.onStart.enabled }}
- name: {{ template "victoria-metrics.name" . }}-vmbackupmanager-restore
  image: "{{ .Values.server.vmbackupmanager.image.repository }}:{{ .Values.server.vmbackupmanager.image.tag }}"
  imagePullPolicy: "{{ .Values.server.image.pullPolicy }}"
  args:
    - restore
    - {{ printf "%s=%t" "--eula" .Values.server.vmbackupmanager.eula | quote}}
    - {{ printf "%s=%s" "--storageDataPath" .Values.server.persistentVolume.mountPath | quote}}
    {{- range $key, $value := .Values.server.vmbackupmanager.extraArgs }}
    - --{{ $key }}={{ $value }}
    {{- end }}
  {{- with  .Values.server.securityContext }}
  securityContext: {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .Values.server.vmbackupmanager.resources }}
  resources: {{ toYaml . | nindent 4 }}
  {{- end }}
  {{- with .Values.server.vmbackupmanager.env }}
  env: {{ toYaml . | nindent 4 }}
  {{- end }}
  ports:
    - name: manager-http
      containerPort: 8300
  volumeMounts:
    - name: server-volume
      mountPath: {{ .Values.server.persistentVolume.mountPath }}
      subPath: {{ .Values.server.persistentVolume.subPath }}
  {{- with .Values.server.vmbackupmanager.extraVolumeMounts }}
    {{- toYaml . | nindent 4 }}
  {{- end }}
{{- end -}}
{{- else -}}
[]
{{- end -}}
{{- end -}}

{{/*
Return license flag if necessary.
*/}}
{{- define "victoria-metrics.license.flag" -}}
{{- if .Values.license.key -}}
-license={{ .Values.license.key }}
{{- end }}
{{- if and .Values.license.secret.name .Values.license.secret.key -}}
-licenseFile=/etc/vm-license-key/{{ .Values.license.secret.key }}
{{- end -}}
{{- end -}}


{{/*
Return license volume mount
*/}}
{{- define "victoria-metrics.license.volume" -}}
{{- if and .Values.license.secret.name .Values.license.secret.key -}}
- name: license-key
  secret:
    secretName: {{ .Values.license.secret.name }}
{{- end -}}
{{- end -}}

{{/*
Return license volume mount for container
*/}}
{{- define "victoria-metrics.license.mount" -}}
{{- if and .Values.license.secret.name .Values.license.secret.key -}}
- name: license-key
  mountPath: /etc/vm-license-key
  readOnly: true
{{- end -}}
{{- end -}}

{{/*
Enforce license for vmbackupmanager
*/}}
{{- define "victoria-metrics.vmbackupmanager.enforce_license" -}}
{{ if and .Values.server.vmbackupmanager.enable (not (or .Values.server.vmbackupmanager.eula .Values.license.key .Values.license.secret.name)) }}
{{ fail `Pass -eula command-line flag or valid license at .Values.license if you have an enterprise license for running this software.
  See https://victoriametrics.com/legal/esa/ for details.
  Documentation - https://docs.victoriametrics.com/enterprise.html
  for more information, visit https://victoriametrics.com/products/enterprise/
  To request a trial license, go to https://victoriametrics.com/products/enterprise/trial/`}}
{{- end -}}
{{- end -}}
