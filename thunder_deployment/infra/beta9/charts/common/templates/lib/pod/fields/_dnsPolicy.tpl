{{- /*
Returns the value for dnsPolicy
*/ -}}
{{- define "bjw-s.common.lib.pod.field.dnsPolicy" -}}
  {{- $ctx := .ctx -}}
  {{- $controllerObject := $ctx.controllerObject -}}

  {{- /* Default to "ClusterFirst" */ -}}
  {{- $dnsPolicy := "ClusterFirst" -}}

  {{- /* Get hostNetwork value "" */ -}}
  {{- $hostNetwork:= get $controllerObject.pod "hostNetwork" -}}
  {{- if $hostNetwork -}}
    {{- $dnsPolicy = "ClusterFirstWithHostNet" -}}
  {{- end -}}

  {{- /* See if an override is desired */ -}}
  {{- $override := get $controllerObject.pod "dnsPolicy" -}}

  {{- if not (empty $override) -}}
    {{- $dnsPolicy = $override -}}
  {{- end -}}

  {{- $dnsPolicy -}}
{{- end -}}
