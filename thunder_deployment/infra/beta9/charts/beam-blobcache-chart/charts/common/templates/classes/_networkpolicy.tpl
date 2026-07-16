{{/*
This template serves as a blueprint for all networkPolicy objects that are created
within the common library.
*/}}
{{- define "bjw-s.common.class.networkpolicy" -}}
  {{- $rootContext := .rootContext -}}
  {{- $networkPolicyObject := .object -}}

  {{- $labels := merge
    ($networkPolicyObject.labels | default dict)
    (include "bjw-s.common.lib.metadata.allLabels" $rootContext | fromYaml)
  -}}
  {{- $annotations := merge
    ($networkPolicyObject.annotations | default dict)
    (include "bjw-s.common.lib.metadata.globalAnnotations" $rootContext | fromYaml)
  -}}
  {{- $podSelector := dict -}}
  {{- if (hasKey $networkPolicyObject "podSelector") -}}
    {{- $podSelector = $networkPolicyObject.podSelector -}}
  {{- else -}}
    {{- $podSelector = dict "matchLabels" (merge
      ($networkPolicyObject.extraSelectorLabels | default dict)
      (dict "app.kubernetes.io/component" $networkPolicyObject.controller)
      (include "bjw-s.common.lib.metadata.selectorLabels" $rootContext | fromYaml)
    ) -}}
  {{- end -}}
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ $networkPolicyObject.name }}
  {{- with $labels }}
  labels: {{- toYaml . | nindent 4 -}}
  {{- end }}
  {{- with $annotations }}
  annotations: {{- toYaml . | nindent 4 -}}
  {{- end }}
spec:
  podSelector: {{- toYaml $podSelector | nindent 4 }}
  {{- with $networkPolicyObject.policyTypes }}
  policyTypes: {{- toYaml . | nindent 4 -}}
  {{- end }}
  {{- with $networkPolicyObject.rules.ingress }}
  ingress: {{- tpl (toYaml .) $rootContext | nindent 4 -}}
  {{- end }}
  {{- with $networkPolicyObject.rules.egress }}
  egress: {{- tpl (toYaml .) $rootContext | nindent 4 -}}
  {{- end }}
{{- end -}}
