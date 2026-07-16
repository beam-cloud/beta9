{{- /*
Returns the value for the specified field
*/ -}}
{{- define "bjw-s.common.lib.pod.getOption" -}}
  {{- $rootContext := .ctx.rootContext -}}
  {{- $controllerObject := .ctx.controllerObject -}}
  {{- $option := .option -}}

  {{- $value := "" -}}

  {{- /* Set to the default if it is set */ -}}
  {{- $defaultOption := get $rootContext.Values.defaultPodOptions $option -}}
  {{- if kindIs "bool" $defaultOption -}}
    {{- $value = $defaultOption -}}
  {{- else if not (empty $defaultOption) -}}
    {{- $value = $defaultOption -}}
  {{- end -}}

  {{- /* See if a pod-specific override is needed */ -}}
  {{- if hasKey $controllerObject "pod" -}}
    {{- $podOption := get $controllerObject.pod $option -}}
    {{- if kindIs "bool" $podOption -}}
      {{- $value = $podOption -}}
    {{- else if not (empty $podOption) -}}
      {{- $value = $podOption -}}
    {{- end -}}
  {{- end -}}

  {{- if kindIs "bool" $value -}}
    {{- $value | toYaml -}}
  {{- else if not (empty $value) -}}
    {{- $value | toYaml -}}
  {{- end -}}
{{- end -}}
