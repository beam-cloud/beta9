{{- /*
Returns the value for annotations
*/ -}}
{{- define "bjw-s.common.lib.pod.metadata.annotations" -}}
  {{- $rootContext := .rootContext -}}
  {{- $controllerObject := .controllerObject -}}

  {{- /* Default annotations */ -}}
  {{- $annotations := dict -}}

  {{- /* Set to the default if it is set */ -}}
  {{- $defaultOption := get $rootContext.Values.defaultPodOptions "annotations" -}}
  {{- if not (empty $defaultOption) -}}
    {{- $annotations = merge $defaultOption $annotations -}}
  {{- end -}}

  {{- /* See if a pod-specific override is set */ -}}
  {{- if hasKey $controllerObject "pod" -}}
    {{- $podOption := get $controllerObject.pod "annotations" -}}
    {{- if not (empty $podOption) -}}
      {{- $annotations = merge $podOption $annotations -}}
    {{- end -}}
  {{- end -}}

  {{- /* Add configMaps checksum */ -}}
  {{- $configMapsFound := dict -}}
  {{- range $name, $configmap := $rootContext.Values.configMaps -}}
    {{- if $configmap.enabled -}}
      {{- $_ := set $configMapsFound $name (toYaml $configmap.data | sha256sum) -}}
    {{- end -}}
  {{- end -}}
  {{- if $configMapsFound -}}
    {{- $annotations = merge
      (dict "checksum/configMaps" (toYaml $configMapsFound | sha256sum))
      $annotations
    -}}
  {{- end -}}

  {{- /* Add Secrets checksum */ -}}
  {{- $secretsFound := dict -}}
  {{- range $name, $secret := $rootContext.Values.secrets -}}
    {{- if $secret.enabled -}}
      {{- $_ := set $secretsFound $name (toYaml $secret.stringData | sha256sum) -}}
    {{- end -}}
  {{- end -}}
  {{- if $secretsFound -}}
    {{- $annotations = merge
      (dict "checksum/secrets" (toYaml $secretsFound | sha256sum))
      $annotations
    -}}
  {{- end -}}

  {{- if not (empty $annotations) -}}
    {{- $annotations | toYaml -}}
  {{- end -}}
{{- end -}}
