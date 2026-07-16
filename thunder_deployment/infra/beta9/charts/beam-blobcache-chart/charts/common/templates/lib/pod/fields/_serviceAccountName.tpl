{{- /*
Returns the value for serviceAccountName
*/ -}}
{{- define "bjw-s.common.lib.pod.field.serviceAccountName" -}}
  {{- $rootContext := .ctx.rootContext -}}

  {{- $serviceAccountValues := (mustDeepCopy $rootContext.Values.serviceAccount) -}}
  {{- $serviceAccountObject := (include "bjw-s.common.lib.serviceAccount.valuesToObject" (dict "rootContext" $rootContext "id" "default" "values" $serviceAccountValues)) | fromYaml -}}
  {{- $serviceAccountObject.name -}}

{{- end -}}
