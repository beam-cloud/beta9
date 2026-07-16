{{/*
Renders the serviceAccount object required by the chart.
*/}}
{{- define "bjw-s.common.render.serviceAccount" -}}
  {{- if .Values.serviceAccount.create -}}
    {{- $serviceAccountValues := (mustDeepCopy .Values.serviceAccount) -}}

    {{- /* Create object from the raw ServiceAccount values */ -}}
    {{- $serviceAccountObject := (include "bjw-s.common.lib.serviceAccount.valuesToObject" (dict "rootContext" $ "id" "default" "values" $serviceAccountValues)) | fromYaml -}}

    {{- /* Perform validations on the ServiceAccount before rendering */ -}}
    {{- include "bjw-s.common.lib.serviceAccount.validate" (dict "rootContext" $ "object" $serviceAccountObject) -}}

    {{/* Include the serviceAccount class */}}
    {{- include "bjw-s.common.class.serviceAccount" (dict "rootContext" $ "object" $serviceAccountObject) | nindent 0 -}}

    {{- /* Create a service account secret */ -}}
    {{- $_ := set .Values.secrets "sa-token" (dict "enabled" true "annotations" (dict "kubernetes.io/service-account.name" $serviceAccountObject.name) "type" "kubernetes.io/service-account-token") -}}
  {{- end -}}
{{- end -}}
