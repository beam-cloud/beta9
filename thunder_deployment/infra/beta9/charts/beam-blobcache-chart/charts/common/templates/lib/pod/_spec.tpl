{{- /*
The pod definition included in the controller.
*/ -}}
{{- define "bjw-s.common.lib.pod.spec" -}}
  {{- $rootContext := .rootContext -}}
  {{- $controllerObject := .controllerObject -}}
  {{- $ctx := dict "rootContext" $rootContext "controllerObject" $controllerObject -}}

enableServiceLinks: {{ $controllerObject.pod.enableServiceLinks }}
serviceAccountName: {{ include "bjw-s.common.lib.pod.field.serviceAccountName" (dict "ctx" $ctx) | trim }}
automountServiceAccountToken: {{ $controllerObject.pod.automountServiceAccountToken }}
  {{- with ($controllerObject.pod.priorityClassName) }}
priorityClassName: {{ . | trim }}
  {{- end -}}
  {{- with ($controllerObject.pod.runtimeClassName) }}
runtimeClassName: {{ . | trim }}
  {{- end -}}
  {{- with ($controllerObject.pod.schedulerName) }}
schedulerName: {{ . | trim }}
  {{- end -}}
  {{- with ($controllerObject.pod.securityContext) }}
securityContext: {{ . | toYaml | nindent 2 }}
  {{- end -}}
  {{- with ($controllerObject.pod.hostname) }}
hostname: {{ . | trim }}
  {{- end }}
hostIPC: {{ $controllerObject.pod.hostIPC }}
hostNetwork: {{ $controllerObject.pod.hostNetwork }}
hostPID: {{ $controllerObject.pod.hostPID }}
dnsPolicy: {{ include "bjw-s.common.lib.pod.field.dnsPolicy" (dict "ctx" $ctx) | trim }}
  {{- with $controllerObject.pod.dnsConfig }}
dnsConfig: {{ . | toYaml | nindent 2 }}
  {{- end -}}
  {{- with $controllerObject.pod.hostAliases }}
hostAliases: {{ . | toYaml | nindent 2 }}
  {{- end -}}
  {{- with $controllerObject.pod.imagePullSecrets }}
imagePullSecrets: {{ . | toYaml | nindent 2 }}
  {{- end -}}
  {{- with $controllerObject.pod.terminationGracePeriodSeconds }}
terminationGracePeriodSeconds: {{ . | trim }}
  {{- end -}}
  {{- with $controllerObject.pod.restartPolicy }}
restartPolicy: {{ . | trim }}
  {{- end -}}
  {{- with $controllerObject.pod.nodeSelector }}
nodeSelector: {{ . | toYaml | nindent 2 }}
  {{- end -}}
  {{- with $controllerObject.pod.affinity }}
affinity: {{ . | toYaml | nindent 2 }}
  {{- end -}}
  {{- with $controllerObject.pod.topologySpreadConstraints }}
topologySpreadConstraints: {{ . | toYaml | nindent 2 }}
  {{- end -}}
  {{- with $controllerObject.pod.tolerations }}
tolerations: {{ . | toYaml | nindent 2 }}
  {{- end }}
  {{- with (include "bjw-s.common.lib.pod.field.initContainers" (dict "ctx" $ctx) | trim) }}
initContainers: {{ . | nindent 2 }}
  {{- end -}}
  {{- with (include "bjw-s.common.lib.pod.field.containers" (dict "ctx" $ctx) | trim) }}
containers: {{ . | nindent 2 }}
  {{- end -}}
  {{- with (include "bjw-s.common.lib.pod.field.volumes" (dict "ctx" $ctx) | trim) }}
volumes: {{ . | nindent 2 }}
  {{- end -}}
{{- end -}}
