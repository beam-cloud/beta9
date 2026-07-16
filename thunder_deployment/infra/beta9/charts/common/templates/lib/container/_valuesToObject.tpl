{{/*
Convert container values to an object
*/}}
{{- define "bjw-s.common.lib.container.valuesToObject" -}}
  {{- $rootContext := .rootContext -}}
  {{- $identifier := .id -}}
  {{- $objectValues := .values -}}

  {{- $_ := set $objectValues "identifier" $identifier -}}

  {{- /* Process image tags */ -}}
  {{- if kindIs "map" $objectValues.image -}}
    {{- $imageTag := dig "image" "tag" "" $objectValues -}}
    {{- /* Convert float64 image tags to string */ -}}
    {{- if kindIs "float64" $imageTag -}}
      {{- $imageTag = $imageTag | toString -}}
    {{- end -}}

    {{- /* Process any templates in the tag */ -}}
    {{- $imageTag = tpl $imageTag $rootContext -}}

    {{- $_ := set $objectValues.image "tag" $imageTag -}}
  {{- end -}}

  {{- /* Return the container object */ -}}
  {{- $objectValues | toYaml -}}
{{- end -}}
