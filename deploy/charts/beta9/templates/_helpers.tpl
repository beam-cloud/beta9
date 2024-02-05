{{/*
Expand the name of the chart.
*/}}
{{- define "manifests.name" -}}
{{- default .Chart.Name "beta9" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "manifests.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Adds labels to resources.
*/}}
{{- define "manifests.resource" -}}
metadata:
  labels:
    helm.sh/chart: {{ template "manifests.chart" . }}
    app.kubernetes.io/name: {{ template "manifests.name" }}
    app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
    app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}
