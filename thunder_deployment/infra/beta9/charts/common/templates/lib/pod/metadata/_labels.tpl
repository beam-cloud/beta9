{{- /*
Returns the value for labels
*/ -}}
{{- define "bjw-s.common.lib.pod.metadata.labels" -}}
  {{- $rootContext := .rootContext -}}
  {{- $controllerObject := .controllerObject -}}

  {{- /* Default labels */ -}}
  {{- $labels := merge
    (dict "app.kubernetes.io/component" $controllerObject.identifier)
  -}}

  {{- /* Fetch the Pod selectorLabels */ -}}
  {{- $selectorLabels := include "bjw-s.common.lib.metadata.selectorLabels" $rootContext | fromYaml -}}
  {{- if not (empty $selectorLabels) -}}
    {{- $labels = merge $selectorLabels $labels -}}
  {{- end -}}

  {{- /* Set to the default if it is set */ -}}
  {{- $defaultOption := get $rootContext.Values.defaultPodOptions "labels" -}}
  {{- if not (empty $defaultOption) -}}
    {{- $labels = merge $defaultOption $labels -}}
  {{- end -}}

  {{- /* See if a pod-specific override is set */ -}}
  {{- if hasKey $controllerObject "pod" -}}
    {{- $podOption := get $controllerObject.pod "labels" -}}
    {{- if not (empty $podOption) -}}
      {{- $labels = merge $podOption $labels -}}
    {{- end -}}
  {{- end -}}

  {{- if not (empty $labels) -}}
    {{- $labels | toYaml -}}
  {{- end -}}
{{- end -}}
