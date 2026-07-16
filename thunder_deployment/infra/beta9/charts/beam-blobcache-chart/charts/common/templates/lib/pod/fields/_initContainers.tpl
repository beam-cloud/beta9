{{- /*
Returns the value for initContainers
*/ -}}
{{- define "bjw-s.common.lib.pod.field.initContainers" -}}
  {{- $rootContext := .ctx.rootContext -}}
  {{- $controllerObject := .ctx.controllerObject -}}

  {{- /* Default to empty list */ -}}
  {{- $graph := dict -}}
  {{- $containers := list -}}

  {{- /* Fetch configured containers for this controller */ -}}
  {{- $renderedContainers := dict -}}

  {{- /* TODO: Remove this logic after "order" removal in v3 */ -}}
  {{- $containersWithDependsOn := include "bjw-s.common.lib.getMapItemsWithKey" (dict "map" $controllerObject.initContainers "key" "dependsOn") | fromYaml | keys -}}
  {{- $useDependsOn := gt (len $containersWithDependsOn) 0 -}}

  {{- range $key, $containerValues := $controllerObject.initContainers -}}
    {{- /* Enable container by default, but allow override */ -}}
    {{- $containerEnabled := true -}}
    {{- if hasKey $containerValues "enabled" -}}
      {{- $containerEnabled = $containerValues.enabled -}}
    {{- end -}}

    {{- if $containerEnabled -}}
      {{- /* Create object from the container values */ -}}
      {{- $containerObject := (include "bjw-s.common.lib.container.valuesToObject" (dict "rootContext" $rootContext "id" $key "values" $containerValues)) | fromYaml -}}

      {{- /* Perform validations on the Container before rendering */ -}}
      {{- include "bjw-s.common.lib.container.validate" (dict "rootContext" $ "controllerObject" $controllerObject "containerObject" $containerObject) -}}

      {{- /* Generate the Container spec */ -}}
      {{- $renderedContainer := include "bjw-s.common.lib.container.spec" (dict "rootContext" $rootContext "controllerObject" $controllerObject "containerObject" $containerObject) | fromYaml -}}
      {{- $_ := set $renderedContainers $key $renderedContainer -}}

      {{- /* Determine the Container order */ -}}
      {{- if $useDependsOn -}}
        {{- if empty (dig "dependsOn" nil $containerValues) -}}
          {{- $_ := set $graph $key ( list ) -}}
        {{- else if kindIs "string" $containerValues.dependsOn -}}
          {{- $_ := set $graph $key ( list $containerValues.dependsOn ) -}}
        {{- else if kindIs "slice" $containerValues.dependsOn -}}
          {{- $_ := set $graph $key $containerValues.dependsOn -}}
        {{- end -}}
      {{- else -}}
        {{- /* TODO: Remove this logic after "order" removal in v3 */ -}}
        {{- $containerOrder := (dig "order" 99 $containerValues) -}}
        {{- $_ := set $graph $key $containerOrder -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}

  {{- /* Process graph */ -}}
  {{- if $useDependsOn -}}
    {{- $args := dict "graph" $graph "out" list -}}
    {{- include "bjw-s.common.lib.kahn" $args -}}

    {{- range $name := $args.out -}}
      {{- $containerItem := get $renderedContainers $name -}}
      {{- $containers = append $containers $containerItem -}}
    {{- end -}}
  {{- else -}}
    {{- /* TODO: Remove this logic after "order" removal in v3 */ -}}
    {{- $orderedContainers := dict -}}
    {{- range $key, $order := $graph -}}
      {{- $containerItem := get $renderedContainers $key -}}
      {{- $_ := set $orderedContainers (printf "%v-%s" $order $key) $containerItem -}}
    {{- end -}}
    {{- range $key, $containerValues := $orderedContainers -}}
      {{- $containers = append $containers $containerValues -}}
    {{- end -}}
  {{- end -}}

  {{- if not (empty $containers) -}}
    {{- $containers | toYaml -}}
  {{- end -}}
{{- end -}}
