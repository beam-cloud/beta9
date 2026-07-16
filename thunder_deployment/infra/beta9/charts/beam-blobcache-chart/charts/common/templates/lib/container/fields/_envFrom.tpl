{{/*
Env field used by the container.
*/}}
{{- define "bjw-s.common.lib.container.field.envFrom" -}}
  {{- $ctx := .ctx -}}
  {{- $rootContext := $ctx.rootContext -}}
  {{- $containerObject := $ctx.containerObject -}}

  {{- if not (empty (get $containerObject "envFrom")) -}}
    {{- $envFrom := list -}}
    {{- range $containerObject.envFrom -}}
      {{- $item := dict -}}

      {{- if hasKey . "configMap" -}}
        {{- $configMap := include "bjw-s.common.lib.configMap.getByIdentifier" (dict "rootContext" $rootContext "id" .configMap) | fromYaml -}}
        {{- $configMapName := default (tpl .configMap $rootContext) $configMap.name -}}
        {{- $_ := set $item "configMapRef" (dict "name" $configMapName) -}}
      {{- else if hasKey . "configMapRef" -}}
        {{- if not (empty (dig "identifier" nil .configMapRef)) -}}
          {{- $configMap := include "bjw-s.common.lib.configMap.getByIdentifier" (dict "rootContext" $rootContext "id" .configMapRef.identifier) | fromYaml -}}
          {{- if empty $configMap -}}
            {{- fail (printf "No configMap configured with identifier '%s'" .configMapRef.identifier) -}}
          {{- end -}}

          {{- $_ := set $item "configMapRef" (dict "name" $configMap.name) -}}
        {{- else -}}
          {{- $_ := set $item "configMapRef" (dict "name" (tpl .configMapRef.name $rootContext)) -}}
        {{- end -}}

      {{- else if hasKey . "secret" -}}
        {{- $secret := include "bjw-s.common.lib.secret.getByIdentifier" (dict "rootContext" $rootContext "id" .secret) | fromYaml -}}
        {{- $secretName := default (tpl .secret $rootContext) $secret.name -}}
        {{- $_ := set $item "secretRef" (dict "name" $secretName) -}}
      {{- else if hasKey . "secretRef" -}}
        {{- if not (empty (dig "identifier" nil .secretRef)) -}}
          {{- $secret := include "bjw-s.common.lib.secret.getByIdentifier" (dict "rootContext" $rootContext "id" .secretRef.identifier) | fromYaml -}}
          {{- if empty $secret -}}
            {{- fail (printf "No secret configured with identifier '%s'" .secretRef.identifier) -}}
          {{- end -}}

          {{- $_ := set $item "secretRef" (dict "name" $secret.name) -}}
        {{- else -}}
          {{- $_ := set $item "secretRef" (dict "name" (tpl .secretRef.name $rootContext)) -}}
        {{- end -}}
      {{- end -}}

      {{- if not (empty (dig "prefix" nil .)) -}}
        {{- $_ := set $item "prefix" .prefix -}}
      {{- end -}}

      {{- if not (empty $item) -}}
        {{- $envFrom = append $envFrom $item -}}
      {{- end -}}
    {{- end -}}

    {{- $envFrom | toYaml -}}
  {{- end -}}
{{- end -}}
