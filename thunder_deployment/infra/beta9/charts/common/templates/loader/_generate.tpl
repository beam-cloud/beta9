{{/*
Secondary entrypoint and primary loader for the common chart
*/}}
{{- define "bjw-s.common.loader.generate" -}}
  {{- /* Build the templates */ -}}
  {{- include "bjw-s.common.render.pvcs" . | nindent 0 -}}
  {{- include "bjw-s.common.render.serviceAccount" . | nindent 0 -}}
  {{- include "bjw-s.common.render.controllers" . | nindent 0 -}}
  {{- include "bjw-s.common.render.services" . | nindent 0 -}}
  {{- include "bjw-s.common.render.ingresses" . | nindent 0 -}}
  {{- include "bjw-s.common.render.serviceMonitors" . | nindent 0 -}}
  {{- include "bjw-s.common.render.routes" . | nindent 0 -}}
  {{- include "bjw-s.common.render.configMaps" . | nindent 0 -}}
  {{- include "bjw-s.common.render.secrets" . | nindent 0 -}}
  {{- include "bjw-s.common.render.networkpolicies" . | nindent 0 -}}
{{- end -}}
