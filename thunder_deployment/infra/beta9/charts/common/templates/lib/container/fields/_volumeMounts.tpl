{{/*
volumeMounts used by the container.
*/}}
{{- define "bjw-s.common.lib.container.field.volumeMounts" -}}
  {{- $ctx := .ctx -}}
  {{- $rootContext := $ctx.rootContext -}}
  {{- $controllerObject := $ctx.controllerObject -}}
  {{- $containerObject := $ctx.containerObject -}}

  {{- /* Default to empty dict */ -}}
  {{- $persistenceItemsToProcess := dict -}}
  {{- $enabledVolumeMounts := list -}}

  {{- /* Collect regular persistence items */ -}}
  {{- range $identifier, $persistenceValues := $rootContext.Values.persistence -}}
    {{- /* Enable persistence item by default, but allow override */ -}}
    {{- $persistenceEnabled := true -}}
    {{- if hasKey $persistenceValues "enabled" -}}
      {{- $persistenceEnabled = $persistenceValues.enabled -}}
    {{- end -}}

    {{- if $persistenceEnabled -}}
      {{- $_ := set $persistenceItemsToProcess $identifier $persistenceValues -}}
    {{- end -}}
  {{- end -}}

  {{- /* Collect volumeClaimTemplates */ -}}
  {{- if not (empty (dig "statefulset" "volumeClaimTemplates" nil $controllerObject)) -}}
    {{- range $persistenceValues := $controllerObject.statefulset.volumeClaimTemplates -}}
      {{- /* Enable persistence item by default, but allow override */ -}}
      {{- $persistenceEnabled := true -}}
      {{- if hasKey $persistenceValues "enabled" -}}
        {{- $persistenceEnabled = $persistenceValues.enabled -}}
      {{- end -}}

      {{- if $persistenceEnabled -}}
        {{- $mountValues := dict -}}
        {{- if not (empty (dig "globalMounts" nil $persistenceValues)) -}}
          {{- $_ := set $mountValues "globalMounts" $persistenceValues.globalMounts -}}
        {{- end -}}
        {{- if not (empty (dig "advancedMounts" nil $persistenceValues)) -}}
          {{- $_ := set $mountValues "advancedMounts" (dict $controllerObject.identifier $persistenceValues.advancedMounts) -}}
        {{- end -}}
        {{- $_ := set $persistenceItemsToProcess $persistenceValues.name $mountValues -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}

  {{- range $identifier, $persistenceValues := $persistenceItemsToProcess -}}
    {{- /* Set some default values */ -}}

    {{- /* Set the default mountPath to /<name_of_the_peristence_item> */ -}}
    {{- $mountPath := (printf "/%v" $identifier) -}}
    {{- if eq "hostPath" (default "pvc" $persistenceValues.type) -}}
      {{- $mountPath = $persistenceValues.hostPath -}}
    {{- end -}}

    {{- /* Process configured mounts */ -}}
    {{- if or .globalMounts .advancedMounts -}}
      {{- $mounts := list -}}
      {{- if hasKey . "globalMounts" -}}
        {{- $mounts = .globalMounts -}}
      {{- end -}}

      {{- if hasKey . "advancedMounts" -}}
        {{- $advancedMounts := dig $controllerObject.identifier $containerObject.identifier list .advancedMounts -}}
        {{- range $advancedMounts -}}
          {{- $mounts = append $mounts . -}}
        {{- end -}}
      {{- end -}}

      {{- range $mounts -}}
        {{- $volumeMount := dict -}}
        {{- $_ := set $volumeMount "name" $identifier -}}

        {{- /* Use the specified mountPath if provided */ -}}
        {{- with .path -}}
          {{- $mountPath = . -}}
        {{- end -}}
        {{- $_ := set $volumeMount "mountPath" $mountPath -}}

        {{- /* Use the specified subPath if provided */ -}}
        {{- with .subPath -}}
          {{- $subPath := . -}}
          {{- $_ := set $volumeMount "subPath" $subPath -}}
        {{- end -}}

        {{- /* Use the specified readOnly setting if provided */ -}}
        {{- with .readOnly -}}
          {{- $readOnly := . -}}
          {{- $_ := set $volumeMount "readOnly" $readOnly -}}
        {{- end -}}

        {{- /* Use the specified mountPropagation setting if provided */ -}}
        {{- with .mountPropagation -}}
          {{- $mountPropagation := . -}}
          {{- $_ := set $volumeMount "mountPropagation" $mountPropagation -}}
        {{- end -}}

        {{- $enabledVolumeMounts = append $enabledVolumeMounts $volumeMount -}}
      {{- end -}}

    {{- /* Mount to default path if no mounts are configured */ -}}
    {{- else -}}
      {{- $volumeMount := dict -}}
      {{- $_ := set $volumeMount "name" $identifier -}}
      {{- $_ := set $volumeMount "mountPath" $mountPath -}}
      {{- $enabledVolumeMounts = append $enabledVolumeMounts $volumeMount -}}
    {{- end -}}
  {{- end -}}

  {{- with $enabledVolumeMounts -}}
    {{- . | toYaml -}}
  {{- end -}}
{{- end -}}
