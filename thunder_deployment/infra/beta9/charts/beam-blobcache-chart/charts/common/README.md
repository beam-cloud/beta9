# common

![Version: 2.5.0](https://img.shields.io/badge/Version-2.5.0-informational?style=flat-square) ![Type: library](https://img.shields.io/badge/Type-library-informational?style=flat-square)

Function library for Helm charts

## Requirements

Kubernetes: `>=1.22.0-0`

## Dependencies

| Repository | Name | Version |
|------------|------|---------|

## Installing the Chart

This is a [Helm Library Chart](https://helm.sh/docs/topics/library_charts/#helm).

**🚨 WARNING: THIS CHART IS NOT MEANT TO BE INSTALLED DIRECTLY**

## Using this library

Include this chart as a dependency in your `Chart.yaml` e.g.

```yaml
# Chart.yaml
dependencies:
  - name: common
    version: 2.5.0
    repository: https://bjw-s.github.io/helm-charts/
```

For more information, take a look at the [Docs](http://bjw-s.github.io/helm-charts/docs/common-library/introduction/).

## Configuration

Read through the [values.yaml](./values.yaml) file. It has several commented out suggested values.

## Values

**Important**: When deploying an application Helm chart you can add more values from the common library chart [here](https://github.com/bjw-s/helm-charts/tree/main/charts/library/common)

The following table contains an overview of available values and their descriptions / default values.

<details>
<summary>Expand</summary>

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| configMaps | object | See below | Configure configMaps for the chart here. Additional configMaps can be added by adding a dictionary key similar to the 'config' object. |
| configMaps.config.annotations | object | `{}` | Annotations to add to the configMap |
| configMaps.config.data | object | `{}` | configMap data content. Helm template enabled. |
| configMaps.config.enabled | bool | `false` | Enables or disables the configMap |
| configMaps.config.labels | object | `{}` | Labels to add to the configMap |
| controllers.main.annotations | object | `{}` | Set annotations on the deployment/statefulset/daemonset/cronjob/job |
| controllers.main.containers.main.args | list | `[]` | Override the args for the default container |
| controllers.main.containers.main.command | list | `[]` | Override the command(s) for the default container |
| controllers.main.containers.main.dependsOn | list | `[]` | Specify if this container depends on any other containers This is used to determine the order in which the containers are rendered. The use of "dependsOn" completely disables the "order" field within the controller. |
| controllers.main.containers.main.env | string | `nil` | Environment variables. Template enabled. Syntax options: A) TZ: UTC B) PASSWD: '{{ .Release.Name }}' B) TZ:      value: UTC      dependsOn: otherVar D) PASSWD:      configMapKeyRef:        name: config-map-name        key: key-name E) PASSWD:      dependsOn:        - otherVar1        - otherVar2      valueFrom:        secretKeyRef:          name: secret-name          key: key-name      ... F) - name: TZ      value: UTC G) - name: TZ      value: '{{ .Release.Name }}' |
| controllers.main.containers.main.envFrom | list | `[]` | Secrets and/or ConfigMaps that will be loaded as environment variables. Syntax options: A) Pass an app-template configMap identifier:    - config: config B) Pass any configMap name that is not also an identifier (Template enabled):    - config: random-configmap-name C) Pass an app-template configMap identifier, explicit syntax:    - configMapRef:        identifier: config D) Pass any configMap name, explicit syntax (Template enabled):    - configMapRef:        name: "{{ .Release.Name }}-config" E) Pass an app-template secret identifier:    - secret: secret F) Pass any secret name that is not also an identifier (Template enabled):    - secret: random-secret-name G) Pass an app-template secret identifier, explicit syntax:    - secretRef:        identifier: secret H) Pass any secret name, explicit syntax (Template enabled):    - secretRef:        name: "{{ .Release.Name }}-secret" |
| controllers.main.containers.main.image.pullPolicy | string | `nil` | image pull policy |
| controllers.main.containers.main.image.repository | string | `nil` | image repository |
| controllers.main.containers.main.image.tag | string | `nil` | image tag |
| controllers.main.containers.main.lifecycle | object | `{}` | [[ref](https://kubernetes.io/docs/tasks/configure-pod-container/attach-handler-lifecycle-event/)] |
| controllers.main.containers.main.nameOverride | string | `nil` | Override the container name |
| controllers.main.containers.main.order | int | 99 | Override the default container order Containers get sorted alphanumerically by the `<order>-<identifier>` combination. |
| controllers.main.containers.main.probes | object | See below | [[ref]](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/) |
| controllers.main.containers.main.probes.liveness | object | See below | Liveness probe configuration |
| controllers.main.containers.main.probes.liveness.custom | bool | `false` | Set this to `true` if you wish to specify your own livenessProbe |
| controllers.main.containers.main.probes.liveness.enabled | bool | `true` | Enable the liveness probe |
| controllers.main.containers.main.probes.liveness.spec | object | See below | The spec field contains the values for the default livenessProbe. If you selected `custom: true`, this field holds the definition of the livenessProbe. |
| controllers.main.containers.main.probes.liveness.type | string | "TCP" | sets the probe type when not using a custom probe |
| controllers.main.containers.main.probes.readiness | object | See below | Redainess probe configuration |
| controllers.main.containers.main.probes.readiness.custom | bool | `false` | Set this to `true` if you wish to specify your own readinessProbe |
| controllers.main.containers.main.probes.readiness.enabled | bool | `true` | Enable the readiness probe |
| controllers.main.containers.main.probes.readiness.spec | object | See below | The spec field contains the values for the default readinessProbe. If you selected `custom: true`, this field holds the definition of the readinessProbe. |
| controllers.main.containers.main.probes.readiness.type | string | "TCP" | sets the probe type when not using a custom probe |
| controllers.main.containers.main.probes.startup | object | See below | Startup probe configuration |
| controllers.main.containers.main.probes.startup.custom | bool | `false` | Set this to `true` if you wish to specify your own startupProbe |
| controllers.main.containers.main.probes.startup.enabled | bool | `true` | Enable the startup probe |
| controllers.main.containers.main.probes.startup.spec | object | See below | The spec field contains the values for the default startupProbe. If you selected `custom: true`, this field holds the definition of the startupProbe. |
| controllers.main.containers.main.probes.startup.type | string | "TCP" | sets the probe type when not using a custom probe |
| controllers.main.containers.main.resources | object | `{}` | Set the resource requests / limits for the container. |
| controllers.main.containers.main.securityContext | object | `{}` | Configure the Security Context for the container |
| controllers.main.containers.main.terminationMessagePath | string | `nil` | [[ref](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#lifecycle-1)] |
| controllers.main.containers.main.terminationMessagePolicy | string | `nil` | [[ref](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#lifecycle-1)] |
| controllers.main.containers.main.workingDir | string | `nil` | Override the working directory for the default container |
| controllers.main.cronjob | object | See below | CronJob configuration. Required only when using `controller.type: cronjob`. |
| controllers.main.cronjob.backoffLimit | int | `6` | Limits the number of times a failed job will be retried |
| controllers.main.cronjob.concurrencyPolicy | string | `"Forbid"` | Specifies how to treat concurrent executions of a job that is created by this cron job valid values are Allow, Forbid or Replace |
| controllers.main.cronjob.failedJobsHistory | int | `1` | The number of failed Jobs to keep |
| controllers.main.cronjob.schedule | string | `"*/20 * * * *"` | Sets the CronJob time when to execute your jobs |
| controllers.main.cronjob.startingDeadlineSeconds | int | `30` | The deadline in seconds for starting the job if it misses its scheduled time for any reason |
| controllers.main.cronjob.successfulJobsHistory | int | `1` | The number of succesful Jobs to keep |
| controllers.main.cronjob.suspend | string | false | Suspends the CronJob [[ref]](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/#schedule-suspension) |
| controllers.main.cronjob.timeZone | string | `nil` | Sets the CronJob timezone (only works in Kubernetes >= 1.27) |
| controllers.main.cronjob.ttlSecondsAfterFinished | string | `nil` | If this field is set, ttlSecondsAfterFinished after the Job finishes, it is eligible to be automatically deleted. |
| controllers.main.enabled | bool | `true` | enable the controller. |
| controllers.main.initContainers | object | `{}` | Specify any initContainers here as dictionary items. Each initContainer should have its own key initContainers get sorted alphanumerically by the `<order>-<identifier>` combination if no order or dependsOn has been configured for them. |
| controllers.main.job | object | See below | Job configuration. Required only when using `controller.type: job`. |
| controllers.main.job.backoffLimit | int | `6` | Limits the number of times a failed job will be retried |
| controllers.main.job.suspend | string | false | Suspends the Job [[ref]](https://kubernetes.io/docs/concepts/workloads/controllers/job/#suspending-a-job) |
| controllers.main.job.ttlSecondsAfterFinished | string | `nil` | If this field is set, ttlSecondsAfterFinished after the Job finishes, it is eligible to be automatically deleted. |
| controllers.main.labels | object | `{}` | Set labels on the deployment/statefulset/daemonset/cronjob/job |
| controllers.main.pod | object | `{}` |  |
| controllers.main.replicas | int | `1` | Number of desired pods. When using a HorizontalPodAutoscaler, set this to `null`. |
| controllers.main.revisionHistoryLimit | int | `3` | ReplicaSet revision history limit |
| controllers.main.rollingUpdate.partition | string | `nil` | Set statefulset RollingUpdate partition |
| controllers.main.rollingUpdate.surge | string | `nil` | Set deployment RollingUpdate max surge |
| controllers.main.rollingUpdate.unavailable | string | `nil` | Set deployment RollingUpdate max unavailable |
| controllers.main.statefulset | object | `{"podManagementPolicy":null,"volumeClaimTemplates":[]}` | StatefulSet configuration. Required only when using `controller.type: statefulset`. |
| controllers.main.statefulset.podManagementPolicy | string | `nil` | Set podManagementPolicy, valid values are Parallel and OrderedReady (default). |
| controllers.main.statefulset.volumeClaimTemplates | list | `[]` | Used to create individual disks for each instance. |
| controllers.main.strategy | string | `nil` | Set the controller upgrade strategy For Deployments, valid values are Recreate (default) and RollingUpdate. For StatefulSets, valid values are OnDelete and RollingUpdate (default). DaemonSets/CronJobs/Jobs ignore this. |
| controllers.main.type | string | `"deployment"` | Set the controller type. Valid options are deployment, daemonset, statefulset, cronjob or job |
| defaultPodOptions | object | `{"affinity":{},"annotations":{},"automountServiceAccountToken":true,"dnsConfig":{},"dnsPolicy":null,"enableServiceLinks":true,"hostAliases":[],"hostIPC":false,"hostNetwork":false,"hostPID":false,"hostname":null,"imagePullSecrets":[],"labels":{},"nodeSelector":{},"priorityClassName":null,"restartPolicy":null,"runtimeClassName":null,"schedulerName":null,"securityContext":{},"terminationGracePeriodSeconds":null,"tolerations":[],"topologySpreadConstraints":[]}` | Set default options for all controllers / pods here Each of these options can be overridden on a Controller level |
| defaultPodOptions.affinity | object | `{}` | Defines affinity constraint rules. [[ref]](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity) |
| defaultPodOptions.annotations | object | `{}` | Set annotations on the Pod. Pod-specific values will be merged with this. |
| defaultPodOptions.automountServiceAccountToken | bool | `true` | Specifies whether a service account token should be automatically mounted. |
| defaultPodOptions.dnsConfig | object | `{}` | Configuring the ndots option may resolve nslookup issues on some Kubernetes setups. |
| defaultPodOptions.dnsPolicy | string | `nil` | Defaults to "ClusterFirst" if hostNetwork is false and "ClusterFirstWithHostNet" if hostNetwork is true. |
| defaultPodOptions.enableServiceLinks | bool | `true` | Enable/disable the generation of environment variables for services. [[ref]](https://kubernetes.io/docs/concepts/services-networking/connect-applications-service/#accessing-the-service) |
| defaultPodOptions.hostAliases | list | `[]` | Use hostAliases to add custom entries to /etc/hosts - mapping IP addresses to hostnames. [[ref]](https://kubernetes.io/docs/concepts/services-networking/add-entries-to-pod-etc-hosts-with-host-aliases/) |
| defaultPodOptions.hostIPC | bool | `false` | Use the host's ipc namespace |
| defaultPodOptions.hostNetwork | bool | `false` | When using hostNetwork make sure you set dnsPolicy to `ClusterFirstWithHostNet` |
| defaultPodOptions.hostPID | bool | `false` | Use the host's pid namespace |
| defaultPodOptions.hostname | string | `nil` | Allows specifying explicit hostname setting |
| defaultPodOptions.imagePullSecrets | list | `[]` | Set image pull secrets |
| defaultPodOptions.labels | object | `{}` | Set labels on the Pod. Pod-specific values will be merged with this. |
| defaultPodOptions.nodeSelector | object | `{}` | Node selection constraint [[ref]](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodeselector) |
| defaultPodOptions.priorityClassName | string | `nil` | Custom priority class for different treatment by the scheduler |
| defaultPodOptions.restartPolicy | string | `Always`. When `controller.type` is `cronjob` it defaults to `Never`. | Set Container restart policy. |
| defaultPodOptions.runtimeClassName | string | `nil` | Allow specifying a runtimeClassName other than the default one (ie: nvidia) |
| defaultPodOptions.schedulerName | string | `nil` | Allows specifying a custom scheduler name |
| defaultPodOptions.securityContext | object | `{}` | Configure the Security Context for the Pod |
| defaultPodOptions.terminationGracePeriodSeconds | string | `nil` | [[ref](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#lifecycle)] |
| defaultPodOptions.tolerations | list | `[]` | Specify taint tolerations [[ref]](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/) |
| defaultPodOptions.topologySpreadConstraints | list | `[]` | Defines topologySpreadConstraint rules. [[ref]](https://kubernetes.io/docs/concepts/workloads/pods/pod-topology-spread-constraints/) |
| global.annotations | object | `{}` | Set additional global annotations. Helm templates can be used. |
| global.fullnameOverride | string | `nil` | Set the entire name definition |
| global.labels | object | `{}` | Set additional global labels. Helm templates can be used. |
| global.nameOverride | string | `nil` | Set an override for the prefix of the fullname |
| ingress | object | See below | Configure the ingresses for the chart here. Additional ingresses can be added by adding a dictionary key similar to the 'main' ingress. |
| ingress.main.annotations | object | `{}` | Provide additional annotations which may be required. |
| ingress.main.className | string | `nil` | Set the ingressClass that is used for this ingress. |
| ingress.main.defaultBackend | string | `nil` | Configure the defaultBackend for this ingress. This will disable any other rules for the ingress. |
| ingress.main.enabled | bool | `false` | Enables or disables the ingress |
| ingress.main.hosts[0].host | string | `"chart-example.local"` | Host address. Helm template can be passed. |
| ingress.main.hosts[0].paths[0].path | string | `"/"` | Path.  Helm template can be passed. |
| ingress.main.hosts[0].paths[0].service.name | string | `"main"` | Overrides the service name reference for this path This can be an actual service name, or reference a service identifier from this values.yaml |
| ingress.main.hosts[0].paths[0].service.port | string | `nil` | Overrides the service port number reference for this path |
| ingress.main.labels | object | `{}` | Provide additional labels which may be required. |
| ingress.main.nameOverride | string | `nil` | Override the name suffix that is used for this ingress. |
| ingress.main.primary | bool | `true` | Make this the primary ingress (used in probes, notes, etc...). If there is more than 1 ingress, make sure that only 1 ingress is marked as primary. |
| ingress.main.tls | list | `[]` | Configure TLS for the ingress. Both secretName and hosts can process a Helm template. |
| networkpolicies | object | See below | Configure the networkPolicies for the chart here. Additional networkPolicies can be added by adding a dictionary key similar to the 'main' networkPolicy. |
| networkpolicies.main.controller | string | `"main"` | Configure which controller this networkPolicy should target |
| networkpolicies.main.enabled | bool | `false` | Enables or disables the networkPolicy item. Defaults to true |
| networkpolicies.main.policyTypes | list | `["Ingress","Egress"]` | The policyTypes for this networkPolicy |
| networkpolicies.main.rules | object | `{"egress":[{}],"ingress":[{}]}` | The rulesets for this networkPolicy [[ref]](https://kubernetes.io/docs/concepts/services-networking/network-policies/#networkpolicy-resource) |
| networkpolicies.main.rules.egress | list | `[{}]` | The egress rules for this networkPolicy. Allows all egress traffic by default. |
| networkpolicies.main.rules.ingress | list | `[{}]` | The ingress rules for this networkPolicy. Allows all ingress traffic by default. |
| persistence | object | See below | Configure persistence for the chart here. Additional items can be added by adding a dictionary key similar to the 'config' key. [[ref]](https://bjw-s.github.io/helm-charts/docs/common-library/common-library-storage) |
| persistence.config.accessMode | string | `"ReadWriteOnce"` | AccessMode for the persistent volume. Make sure to select an access mode that is supported by your storage provider! [[ref]](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes) |
| persistence.config.advancedMounts | object | `{}` | Explicitly configure mounts for specific controllers and containers. Example: advancedMounts:   main: # the controller with the "main" identifier     main: # the container with the "main" identifier       - path: /data/config.yaml         readOnly: true         mountPropagation: None         subPath: config.yaml     second-container: # the container with the "second-container" identifier       - path: /appdata/config         readOnly: true   second-controller: # the controller with the "second-controller" identifier     main: # the container with the "main" identifier       - path: /data/config.yaml         readOnly: false         subPath: config.yaml |
| persistence.config.dataSource | object | `{}` | The optional data source for the persistentVolumeClaim. [[ref]](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#volume-populators-and-data-sources) |
| persistence.config.dataSourceRef | object | `{}` | The optional volume populator for the persistentVolumeClaim. [[ref]](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#volume-populators-and-data-sources) |
| persistence.config.enabled | bool | `false` | Enables or disables the persistence item. Defaults to true |
| persistence.config.existingClaim | string | `nil` | If you want to reuse an existing claim, the name of the existing PVC can be passed here. |
| persistence.config.globalMounts | list | `[]` | Configure mounts to all controllers and containers. By default the persistence item will be mounted to `/<name_of_the_peristence_item>`. Example: globalMounts:   - path: /config     readOnly: false |
| persistence.config.retain | bool | `false` | Set to true to retain the PVC upon `helm uninstall` |
| persistence.config.size | string | `"1Gi"` | The amount of storage that is requested for the persistent volume. |
| persistence.config.storageClass | string | `nil` | Storage Class for the config volume. If set to `-`, dynamic provisioning is disabled. If set to something else, the given storageClass is used. If undefined (the default) or set to null, no storageClassName spec is set, choosing the default provisioner. |
| persistence.config.type | string | `"persistentVolumeClaim"` | Sets the persistence type Valid options are persistentVolumeClaim, emptyDir, nfs, hostPath, secret, configMap or custom |
| route | object | See below | Configure the gateway routes for the chart here. Additional routes can be added by adding a dictionary key similar to the 'main' route. [[ref]](https://gateway-api.sigs.k8s.io/references/spec/) |
| route.main.annotations | object | `{}` | Provide additional annotations which may be required. |
| route.main.enabled | bool | `false` | Enables or disables the route |
| route.main.hostnames | list | `[]` | Host addresses. Helm template can be passed. |
| route.main.kind | string | `"HTTPRoute"` | Set the route kind Valid options are GRPCRoute, HTTPRoute, TCPRoute, TLSRoute, UDPRoute |
| route.main.labels | object | `{}` | Provide additional labels which may be required. |
| route.main.nameOverride | string | `nil` | Override the name suffix that is used for this route. |
| route.main.parentRefs | list | `[{"group":"gateway.networking.k8s.io","kind":"Gateway","name":null,"namespace":null,"sectionName":null}]` | Configure the resource the route attaches to. |
| route.main.rules | list | `[{"backendRefs":[],"filters":[],"matches":[{"path":{"type":"PathPrefix","value":"/"}}],"timeouts":{}}]` | Configure rules for routing. Defaults to the primary service. |
| route.main.rules[0].backendRefs | list | `[]` | Configure backends where matching requests should be sent. |
| secrets | object | See below | Use this to populate secrets with the values you specify. Be aware that these values are not encrypted by default, and could therefore visible to anybody with access to the values.yaml file. Additional Secrets can be added by adding a dictionary key similar to the 'secret' object. |
| secrets.secret.annotations | object | `{}` | Annotations to add to the Secret |
| secrets.secret.enabled | bool | `false` | Enables or disables the Secret |
| secrets.secret.labels | object | `{}` | Labels to add to the Secret |
| secrets.secret.stringData | object | `{}` | Secret stringData content. Helm template enabled. |
| service | object | See below | Configure the services for the chart here. Additional services can be added by adding a dictionary key similar to the 'main' service. |
| service.main.annotations | object | `{}` | Provide additional annotations which may be required. |
| service.main.controller | string | `"main"` | Configure which controller this service should target |
| service.main.enabled | bool | `true` | Enables or disables the service |
| service.main.externalTrafficPolicy | string | `nil` | [[ref](https://kubernetes.io/docs/tutorials/services/source-ip/)] |
| service.main.extraSelectorLabels | object | `{}` | Allow adding additional match labels |
| service.main.ipFamilies | list | `[]` | The ip families that should be used. Options: IPv4, IPv6 |
| service.main.ipFamilyPolicy | string | `nil` | Specify the ip policy. Options: SingleStack, PreferDualStack, RequireDualStack |
| service.main.labels | object | `{}` | Provide additional labels which may be required. |
| service.main.nameOverride | string | `nil` | Override the name suffix that is used for this service |
| service.main.ports | object | See below | Configure the Service port information here. Additional ports can be added by adding a dictionary key similar to the 'http' service. |
| service.main.ports.http.appProtocol | string | `nil` | Specify the appProtocol value for the Service. [[ref]](https://kubernetes.io/docs/concepts/services-networking/service/#application-protocol) |
| service.main.ports.http.enabled | bool | `true` | Enables or disables the port |
| service.main.ports.http.nodePort | string | `nil` | Specify the nodePort value for the LoadBalancer and NodePort service types. [[ref]](https://kubernetes.io/docs/concepts/services-networking/service/#type-nodeport) |
| service.main.ports.http.port | string | `nil` | The port number |
| service.main.ports.http.primary | bool | `true` | Make this the primary port (used in probes, notes, etc...) If there is more than 1 service, make sure that only 1 port is marked as primary. |
| service.main.ports.http.protocol | string | `"HTTP"` | Port protocol. Support values are `HTTP`, `HTTPS`, `TCP` and `UDP`. HTTP and HTTPS spawn a TCP service and get used for internal URL and name generation |
| service.main.ports.http.targetPort | string | `nil` | Specify a service targetPort if you wish to differ the service port from the application port. If `targetPort` is specified, this port number is used in the container definition instead of the `port` value. Therefore named ports are not supported for this field. |
| service.main.primary | bool | `true` | Make this the primary service for this controller (used in probes, notes, etc...). If there is more than 1 service targeting the controller, make sure that only 1 service is marked as primary. |
| service.main.type | string | `"ClusterIP"` | Set the service type |
| serviceAccount.annotations | object | `{}` | Annotations to add to the service account |
| serviceAccount.create | bool | `false` | Specifies whether a service account should be created |
| serviceAccount.labels | object | `{}` | Labels to add to the service account |
| serviceAccount.name | string | `""` | The name of the service account to use. If not set and create is true, a name is generated using the fullname template |
| serviceMonitor | object | See below | Configure the ServiceMonitors for the chart here. Additional ServiceMonitors can be added by adding a dictionary key similar to the 'main' ServiceMonitors. |
| serviceMonitor.main.annotations | object | `{}` | Provide additional annotations which may be required. |
| serviceMonitor.main.enabled | bool | `false` | Enables or disables the serviceMonitor. |
| serviceMonitor.main.endpoints | list | See values.yaml | Configures the endpoints for the serviceMonitor. |
| serviceMonitor.main.labels | object | `{}` | Provide additional labels which may be required. |
| serviceMonitor.main.nameOverride | string | `nil` | Override the name suffix that is used for this serviceMonitor. |
| serviceMonitor.main.selector | object | `{}` | Configures a custom selector for the serviceMonitor, this takes precedence over specifying a service name. Helm templates can be used. |
| serviceMonitor.main.serviceName | string | `"{{ include \"bjw-s.common.lib.chart.names.fullname\" $ }}"` | Configures the target Service for the serviceMonitor. Helm templates can be used. |
| serviceMonitor.main.targetLabels | list | `[]` | Configures custom targetLabels for the serviceMonitor. (All collected meterics will have these labels, taking the value from the target service) [[ref]](https://github.com/prometheus-operator/prometheus-operator/blob/main/Documentation/api.md#servicemonitorspec/) |

</details>

## Support

- See the [Docs](http://bjw-s.github.io/helm-charts/docs/)
- Open an [issue](https://github.com/bjw-s/helm-charts/issues/new/choose)
- Join the k8s-at-home [Discord](https://discord.gg/k8s-at-home) community

----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.11.3](https://github.com/norwoodj/helm-docs/releases/v1.11.3)
