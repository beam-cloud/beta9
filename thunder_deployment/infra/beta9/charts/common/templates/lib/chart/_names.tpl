{{/* Expand the name of the chart */}}
{{- define "bjw-s.common.lib.chart.names.name" -}}
  {{- $globalNameOverride := get .Values.global "nameOverride" -}}
  {{- $nameOverride := get .Values "nameOverride" -}}
  {{- $name := $globalNameOverride | default $nameOverride | default .Chart.Name -}}
  {{- $name | toString | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "bjw-s.common.lib.chart.names.fullname" -}}
  {{- $name := include "bjw-s.common.lib.chart.names.name" . -}}
  {{- $globalFullNameOverride := get .Values.global "fullnameOverride" -}}
  {{- $fullNameOverride := get .Values "fullnameOverride" -}}

  {{- if or $fullNameOverride $globalFullNameOverride -}}
    {{- $name = ($globalFullNameOverride | default $fullNameOverride) -}}
  {{- else -}}
    {{- if contains $name .Release.Name -}}
      {{- $name = .Release.Name -}}
    {{- else -}}
      {{- $name = printf "%s-%s" .Release.Name $name -}}
    {{- end -}}
  {{- end -}}

  {{- $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Create chart name and version as used by the chart label */}}
{{- define "bjw-s.common.lib.chart.names.chart" -}}
  {{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}
