{{/*
Validate networkPolicy values
*/}}
{{- define "bjw-s.common.lib.networkpolicy.validate" -}}
  {{- $rootContext := .rootContext -}}
  {{- $networkpolicyObject := .object -}}

  {{- if and (not (hasKey $networkpolicyObject "podSelector")) (empty (get $networkpolicyObject "controller")) -}}
    {{- fail (printf "controller reference or podSelector is required for NetworkPolicy. (NetworkPolicy %s)" $networkpolicyObject.identifier) -}}
  {{- end -}}

  {{- if empty (get $networkpolicyObject "policyTypes") -}}
    {{- fail (printf "policyTypes is required for NetworkPolicy. (NetworkPolicy %s)" $networkpolicyObject.identifier) -}}
  {{- end -}}

  {{- $allowedpolicyTypes := list "Ingress" "Egress" -}}
  {{- range $networkpolicyObject.policyTypes -}}
    {{- if not (has . $allowedpolicyTypes) -}}
      {{- fail (printf "Not a valid policyType for NetworkPolicy. (NetworkPolicy %s, value %s)" $networkpolicyObject.identifier .) -}}
    {{- end -}}
  {{- end -}}
{{- end -}}
