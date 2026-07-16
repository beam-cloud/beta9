# Victoria Metrics Helm Chart for Single Version

 ![Version: 0.9.15](https://img.shields.io/badge/Version-0.9.15-informational?style=flat-square)
[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/victoriametrics)](https://artifacthub.io/packages/helm/victoriametrics/victoria-metrics-single)

Victoria Metrics Single version - high-performance, cost-effective and scalable TSDB, long-term remote storage for Prometheus

## Prerequisites

* Install the follow packages: ``git``, ``kubectl``, ``helm``, ``helm-docs``. See this [tutorial](../../REQUIREMENTS.md).
* PV support on underlying infrastructure.

## Chart Details

This chart will do the following:

* Rollout Victoria Metrics Single.

## How to install

Access a Kubernetes cluster.

Add a chart helm repository with follow commands:

```console
helm repo add vm https://victoriametrics.github.io/helm-charts/

helm repo update
```

List versions of ``vm/victoria-metrics-single`` chart available to installation:

```console
helm search repo vm/victoria-metrics-single -l
```

Export default values of ``victoria-metrics-single`` chart to file ``values.yaml``:

```console
helm show values vm/victoria-metrics-single > values.yaml
```

Change the values according to the need of the environment in ``values.yaml`` file.

Test the installation with command:

```console
helm install vmsingle vm/victoria-metrics-single -f values.yaml -n NAMESPACE --debug --dry-run
```

Install chart with command:

```console
helm install vmsingle vm/victoria-metrics-single -f values.yaml -n NAMESPACE
```

Get the pods lists by running this commands:

```console
kubectl get pods -A | grep 'single'
```

Get the application by running this command:

```console
helm list -f vmsingle -n NAMESPACE
```

See the history of versions of ``vmsingle`` application with command.

```console
helm history vmsingle -n NAMESPACE
```

## How to uninstall

Remove application with command.

```console
helm uninstall vmsingle -n NAMESPACE
```

## Documentation of Helm Chart

Install ``helm-docs`` following the instructions on this [tutorial](../../REQUIREMENTS.md).

Generate docs with ``helm-docs`` command.

```bash
cd charts/victoria-metrics-single

helm-docs
```

The markdown generation is entirely go template driven. The tool parses metadata from charts and generates a number of sub-templates that can be referenced in a template file (by default ``README.md.gotmpl``). If no template file is provided, the tool has a default internal template that will generate a reasonably formatted README.

## Parameters

The following tables lists the configurable parameters of the chart and their default values.

Change the values according to the need of the environment in ``victoria-metrics-single/values.yaml`` file.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| automountServiceAccountToken | bool | `true` |  |
| extraObjects | list | `[]` | Add extra specs dynamically to this chart |
| license | object | `{"key":"","secret":{"key":"","name":""}}` | Enterprise license key configuration for VictoriaMetrics enterprise. Required only for VictoriaMetrics enterprise. Documentation - https://docs.victoriametrics.com/enterprise.html, for more information, visit https://victoriametrics.com/products/enterprise/ . To request a trial license, go to https://victoriametrics.com/products/enterprise/trial/ Supported starting from VictoriaMetrics v1.94.0 |
| license.key | string | `""` | License key |
| license.secret | object | `{"key":"","name":""}` | Use existing secret with license key |
| license.secret.key | string | `""` | Key in secret with license key |
| license.secret.name | string | `""` | Existing secret name |
| podDisruptionBudget.enabled | bool | `false` | See `kubectl explain poddisruptionbudget.spec` for more. Ref: [https://kubernetes.io/docs/tasks/run-application/configure-pdb/](https://kubernetes.io/docs/tasks/run-application/configure-pdb/) |
| podDisruptionBudget.extraLabels | object | `{}` |  |
| printNotes | bool | `true` | Print chart notes |
| rbac.create | bool | `true` |  |
| rbac.extraLabels | object | `{}` |  |
| rbac.namespaced | bool | `false` |  |
| rbac.pspEnabled | bool | `true` |  |
| server.affinity | object | `{}` | Pod affinity |
| server.containerWorkingDir | string | `""` | Container workdir |
| server.enabled | bool | `true` | Enable deployment of server component. Deployed as StatefulSet |
| server.env | list | `[]` | Additional environment variables (ex.: secret tokens, flags) https://github.com/VictoriaMetrics/VictoriaMetrics#environment-variables |
| server.envFrom | list | `[]` |  |
| server.extraArgs."envflag.enable" | string | `"true"` |  |
| server.extraArgs."envflag.prefix" | string | `"VM_"` |  |
| server.extraArgs.loggerFormat | string | `"json"` |  |
| server.extraContainers | list | `[]` |  |
| server.extraHostPathMounts | list | `[]` |  |
| server.extraLabels | object | `{}` | Sts/Deploy additional labels |
| server.extraVolumeMounts | list | `[]` |  |
| server.extraVolumes | list | `[]` |  |
| server.fullnameOverride | string | `nil` | Overrides the full name of server component |
| server.image.pullPolicy | string | `"IfNotPresent"` | Image pull policy |
| server.image.repository | string | `"victoriametrics/victoria-metrics"` | Image repository |
| server.image.tag | string | `"v1.97.1"` | Image tag |
| server.ingress.annotations | object | `{}` | Ingress annotations |
| server.ingress.enabled | bool | `false` | Enable deployment of ingress for server component |
| server.ingress.extraLabels | object | `{}` | Ingress extra labels |
| server.ingress.hosts | list | `[]` | Array of host objects |
| server.ingress.pathType | string | `"Prefix"` | pathType is only for k8s >= 1.1= |
| server.ingress.tls | list | `[]` | Array of TLS objects |
| server.initContainers | list | `[]` |  |
| server.livenessProbe.failureThreshold | int | `10` |  |
| server.livenessProbe.httpGet.path | string | `"/health"` |  |
| server.livenessProbe.httpGet.port | int | `8428` |  |
| server.livenessProbe.httpGet.scheme | string | `"HTTP"` |  |
| server.livenessProbe.initialDelaySeconds | int | `30` |  |
| server.livenessProbe.periodSeconds | int | `30` |  |
| server.livenessProbe.timeoutSeconds | int | `5` |  |
| server.name | string | `"server"` | Server container name |
| server.nodeSelector | object | `{}` | Pod's node selector. Ref: [https://kubernetes.io/docs/user-guide/node-selection/](https://kubernetes.io/docs/user-guide/node-selection/) |
| server.persistentVolume.accessModes | list | `["ReadWriteOnce"]` | Array of access modes. Must match those of existing PV or dynamic provisioner. Ref: [http://kubernetes.io/docs/user-guide/persistent-volumes/](http://kubernetes.io/docs/user-guide/persistent-volumes/) |
| server.persistentVolume.annotations | object | `{}` | Persistant volume annotations |
| server.persistentVolume.enabled | bool | `true` | Create/use Persistent Volume Claim for server component. Empty dir if false |
| server.persistentVolume.existingClaim | string | `""` | Existing Claim name. If defined, PVC must be created manually before volume will be bound |
| server.persistentVolume.matchLabels | object | `{}` | Bind Persistent Volume by labels. Must match all labels of targeted PV. |
| server.persistentVolume.mountPath | string | `"/storage"` | Mount path. Server data Persistent Volume mount root path. |
| server.persistentVolume.size | string | `"16Gi"` | Size of the volume. Should be calculated based on the metrics you send and retention policy you set. |
| server.persistentVolume.storageClass | string | `""` | StorageClass to use for persistent volume. Requires server.persistentVolume.enabled: true. If defined, PVC created automatically |
| server.persistentVolume.subPath | string | `""` | Mount subpath |
| server.podAnnotations | object | `{}` | Pod's annotations |
| server.podLabels | object | `{}` | Pod's additional labels |
| server.podManagementPolicy | string | `"OrderedReady"` | Pod's management policy |
| server.podSecurityContext | object | `{}` | Pod's security context. Ref: [https://kubernetes.io/docs/tasks/configure-pod-container/security-context/](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/) |
| server.priorityClassName | string | `""` | Name of Priority Class |
| server.readinessProbe.failureThreshold | int | `3` |  |
| server.readinessProbe.httpGet.path | string | `"/health"` |  |
| server.readinessProbe.httpGet.port | string | `"http"` |  |
| server.readinessProbe.initialDelaySeconds | int | `5` |  |
| server.readinessProbe.periodSeconds | int | `15` |  |
| server.readinessProbe.timeoutSeconds | int | `5` |  |
| server.resources | object | `{}` | Resource object. Ref: [http://kubernetes.io/docs/user-guide/compute-resources/](http://kubernetes.io/docs/user-guide/compute-resources/ |
| server.retentionPeriod | int | `1` | Data retention period in month |
| server.scrape | object | `{"config":{"global":{"scrape_interval":"15s"},"scrape_configs":[{"job_name":"victoriametrics","static_configs":[{"targets":["localhost:8428"]}]},{"bearer_token_file":"/var/run/secrets/kubernetes.io/serviceaccount/token","job_name":"kubernetes-apiservers","kubernetes_sd_configs":[{"role":"endpoints"}],"relabel_configs":[{"action":"keep","regex":"default;kubernetes;https","source_labels":["__meta_kubernetes_namespace","__meta_kubernetes_service_name","__meta_kubernetes_endpoint_port_name"]}],"scheme":"https","tls_config":{"ca_file":"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt","insecure_skip_verify":true}},{"bearer_token_file":"/var/run/secrets/kubernetes.io/serviceaccount/token","job_name":"kubernetes-nodes","kubernetes_sd_configs":[{"role":"node"}],"relabel_configs":[{"action":"labelmap","regex":"__meta_kubernetes_node_label_(.+)"},{"replacement":"kubernetes.default.svc:443","target_label":"__address__"},{"regex":"(.+)","replacement":"/api/v1/nodes/$1/proxy/metrics","source_labels":["__meta_kubernetes_node_name"],"target_label":"__metrics_path__"}],"scheme":"https","tls_config":{"ca_file":"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt","insecure_skip_verify":true}},{"bearer_token_file":"/var/run/secrets/kubernetes.io/serviceaccount/token","honor_timestamps":false,"job_name":"kubernetes-nodes-cadvisor","kubernetes_sd_configs":[{"role":"node"}],"relabel_configs":[{"action":"labelmap","regex":"__meta_kubernetes_node_label_(.+)"},{"replacement":"kubernetes.default.svc:443","target_label":"__address__"},{"regex":"(.+)","replacement":"/api/v1/nodes/$1/proxy/metrics/cadvisor","source_labels":["__meta_kubernetes_node_name"],"target_label":"__metrics_path__"}],"scheme":"https","tls_config":{"ca_file":"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt","insecure_skip_verify":true}},{"job_name":"kubernetes-service-endpoints","kubernetes_sd_configs":[{"role":"endpoints"}],"relabel_configs":[{"action":"drop","regex":true,"source_labels":["__meta_kubernetes_pod_container_init"]},{"action":"keep_if_equal","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_port","__meta_kubernetes_pod_container_port_number"]},{"action":"keep","regex":true,"source_labels":["__meta_kubernetes_service_annotation_prometheus_io_scrape"]},{"action":"replace","regex":"(https?)","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_scheme"],"target_label":"__scheme__"},{"action":"replace","regex":"(.+)","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_path"],"target_label":"__metrics_path__"},{"action":"replace","regex":"([^:]+)(?::\\d+)?;(\\d+)","replacement":"$1:$2","source_labels":["__address__","__meta_kubernetes_service_annotation_prometheus_io_port"],"target_label":"__address__"},{"action":"labelmap","regex":"__meta_kubernetes_service_label_(.+)"},{"action":"replace","source_labels":["__meta_kubernetes_namespace"],"target_label":"kubernetes_namespace"},{"action":"replace","source_labels":["__meta_kubernetes_service_name"],"target_label":"kubernetes_name"},{"action":"replace","source_labels":["__meta_kubernetes_pod_node_name"],"target_label":"kubernetes_node"}]},{"job_name":"kubernetes-service-endpoints-slow","kubernetes_sd_configs":[{"role":"endpoints"}],"relabel_configs":[{"action":"drop","regex":true,"source_labels":["__meta_kubernetes_pod_container_init"]},{"action":"keep_if_equal","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_port","__meta_kubernetes_pod_container_port_number"]},{"action":"keep","regex":true,"source_labels":["__meta_kubernetes_service_annotation_prometheus_io_scrape_slow"]},{"action":"replace","regex":"(https?)","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_scheme"],"target_label":"__scheme__"},{"action":"replace","regex":"(.+)","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_path"],"target_label":"__metrics_path__"},{"action":"replace","regex":"([^:]+)(?::\\d+)?;(\\d+)","replacement":"$1:$2","source_labels":["__address__","__meta_kubernetes_service_annotation_prometheus_io_port"],"target_label":"__address__"},{"action":"labelmap","regex":"__meta_kubernetes_service_label_(.+)"},{"action":"replace","source_labels":["__meta_kubernetes_namespace"],"target_label":"kubernetes_namespace"},{"action":"replace","source_labels":["__meta_kubernetes_service_name"],"target_label":"kubernetes_name"},{"action":"replace","source_labels":["__meta_kubernetes_pod_node_name"],"target_label":"kubernetes_node"}],"scrape_interval":"5m","scrape_timeout":"30s"},{"job_name":"kubernetes-services","kubernetes_sd_configs":[{"role":"service"}],"metrics_path":"/probe","params":{"module":["http_2xx"]},"relabel_configs":[{"action":"keep","regex":true,"source_labels":["__meta_kubernetes_service_annotation_prometheus_io_probe"]},{"source_labels":["__address__"],"target_label":"__param_target"},{"replacement":"blackbox","target_label":"__address__"},{"source_labels":["__param_target"],"target_label":"instance"},{"action":"labelmap","regex":"__meta_kubernetes_service_label_(.+)"},{"source_labels":["__meta_kubernetes_namespace"],"target_label":"kubernetes_namespace"},{"source_labels":["__meta_kubernetes_service_name"],"target_label":"kubernetes_name"}]},{"job_name":"kubernetes-pods","kubernetes_sd_configs":[{"role":"pod"}],"relabel_configs":[{"action":"drop","regex":true,"source_labels":["__meta_kubernetes_pod_container_init"]},{"action":"keep_if_equal","source_labels":["__meta_kubernetes_pod_annotation_prometheus_io_port","__meta_kubernetes_pod_container_port_number"]},{"action":"keep","regex":true,"source_labels":["__meta_kubernetes_pod_annotation_prometheus_io_scrape"]},{"action":"replace","regex":"(.+)","source_labels":["__meta_kubernetes_pod_annotation_prometheus_io_path"],"target_label":"__metrics_path__"},{"action":"replace","regex":"([^:]+)(?::\\d+)?;(\\d+)","replacement":"$1:$2","source_labels":["__address__","__meta_kubernetes_pod_annotation_prometheus_io_port"],"target_label":"__address__"},{"action":"labelmap","regex":"__meta_kubernetes_pod_label_(.+)"},{"action":"replace","source_labels":["__meta_kubernetes_namespace"],"target_label":"kubernetes_namespace"},{"action":"replace","source_labels":["__meta_kubernetes_pod_name"],"target_label":"kubernetes_pod_name"}]}]},"configMap":"","enabled":false,"extraScrapeConfigs":[]}` | Scrape configuration for victoriametrics |
| server.scrape.config | object | `{"global":{"scrape_interval":"15s"},"scrape_configs":[{"job_name":"victoriametrics","static_configs":[{"targets":["localhost:8428"]}]},{"bearer_token_file":"/var/run/secrets/kubernetes.io/serviceaccount/token","job_name":"kubernetes-apiservers","kubernetes_sd_configs":[{"role":"endpoints"}],"relabel_configs":[{"action":"keep","regex":"default;kubernetes;https","source_labels":["__meta_kubernetes_namespace","__meta_kubernetes_service_name","__meta_kubernetes_endpoint_port_name"]}],"scheme":"https","tls_config":{"ca_file":"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt","insecure_skip_verify":true}},{"bearer_token_file":"/var/run/secrets/kubernetes.io/serviceaccount/token","job_name":"kubernetes-nodes","kubernetes_sd_configs":[{"role":"node"}],"relabel_configs":[{"action":"labelmap","regex":"__meta_kubernetes_node_label_(.+)"},{"replacement":"kubernetes.default.svc:443","target_label":"__address__"},{"regex":"(.+)","replacement":"/api/v1/nodes/$1/proxy/metrics","source_labels":["__meta_kubernetes_node_name"],"target_label":"__metrics_path__"}],"scheme":"https","tls_config":{"ca_file":"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt","insecure_skip_verify":true}},{"bearer_token_file":"/var/run/secrets/kubernetes.io/serviceaccount/token","honor_timestamps":false,"job_name":"kubernetes-nodes-cadvisor","kubernetes_sd_configs":[{"role":"node"}],"relabel_configs":[{"action":"labelmap","regex":"__meta_kubernetes_node_label_(.+)"},{"replacement":"kubernetes.default.svc:443","target_label":"__address__"},{"regex":"(.+)","replacement":"/api/v1/nodes/$1/proxy/metrics/cadvisor","source_labels":["__meta_kubernetes_node_name"],"target_label":"__metrics_path__"}],"scheme":"https","tls_config":{"ca_file":"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt","insecure_skip_verify":true}},{"job_name":"kubernetes-service-endpoints","kubernetes_sd_configs":[{"role":"endpoints"}],"relabel_configs":[{"action":"drop","regex":true,"source_labels":["__meta_kubernetes_pod_container_init"]},{"action":"keep_if_equal","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_port","__meta_kubernetes_pod_container_port_number"]},{"action":"keep","regex":true,"source_labels":["__meta_kubernetes_service_annotation_prometheus_io_scrape"]},{"action":"replace","regex":"(https?)","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_scheme"],"target_label":"__scheme__"},{"action":"replace","regex":"(.+)","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_path"],"target_label":"__metrics_path__"},{"action":"replace","regex":"([^:]+)(?::\\d+)?;(\\d+)","replacement":"$1:$2","source_labels":["__address__","__meta_kubernetes_service_annotation_prometheus_io_port"],"target_label":"__address__"},{"action":"labelmap","regex":"__meta_kubernetes_service_label_(.+)"},{"action":"replace","source_labels":["__meta_kubernetes_namespace"],"target_label":"kubernetes_namespace"},{"action":"replace","source_labels":["__meta_kubernetes_service_name"],"target_label":"kubernetes_name"},{"action":"replace","source_labels":["__meta_kubernetes_pod_node_name"],"target_label":"kubernetes_node"}]},{"job_name":"kubernetes-service-endpoints-slow","kubernetes_sd_configs":[{"role":"endpoints"}],"relabel_configs":[{"action":"drop","regex":true,"source_labels":["__meta_kubernetes_pod_container_init"]},{"action":"keep_if_equal","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_port","__meta_kubernetes_pod_container_port_number"]},{"action":"keep","regex":true,"source_labels":["__meta_kubernetes_service_annotation_prometheus_io_scrape_slow"]},{"action":"replace","regex":"(https?)","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_scheme"],"target_label":"__scheme__"},{"action":"replace","regex":"(.+)","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_path"],"target_label":"__metrics_path__"},{"action":"replace","regex":"([^:]+)(?::\\d+)?;(\\d+)","replacement":"$1:$2","source_labels":["__address__","__meta_kubernetes_service_annotation_prometheus_io_port"],"target_label":"__address__"},{"action":"labelmap","regex":"__meta_kubernetes_service_label_(.+)"},{"action":"replace","source_labels":["__meta_kubernetes_namespace"],"target_label":"kubernetes_namespace"},{"action":"replace","source_labels":["__meta_kubernetes_service_name"],"target_label":"kubernetes_name"},{"action":"replace","source_labels":["__meta_kubernetes_pod_node_name"],"target_label":"kubernetes_node"}],"scrape_interval":"5m","scrape_timeout":"30s"},{"job_name":"kubernetes-services","kubernetes_sd_configs":[{"role":"service"}],"metrics_path":"/probe","params":{"module":["http_2xx"]},"relabel_configs":[{"action":"keep","regex":true,"source_labels":["__meta_kubernetes_service_annotation_prometheus_io_probe"]},{"source_labels":["__address__"],"target_label":"__param_target"},{"replacement":"blackbox","target_label":"__address__"},{"source_labels":["__param_target"],"target_label":"instance"},{"action":"labelmap","regex":"__meta_kubernetes_service_label_(.+)"},{"source_labels":["__meta_kubernetes_namespace"],"target_label":"kubernetes_namespace"},{"source_labels":["__meta_kubernetes_service_name"],"target_label":"kubernetes_name"}]},{"job_name":"kubernetes-pods","kubernetes_sd_configs":[{"role":"pod"}],"relabel_configs":[{"action":"drop","regex":true,"source_labels":["__meta_kubernetes_pod_container_init"]},{"action":"keep_if_equal","source_labels":["__meta_kubernetes_pod_annotation_prometheus_io_port","__meta_kubernetes_pod_container_port_number"]},{"action":"keep","regex":true,"source_labels":["__meta_kubernetes_pod_annotation_prometheus_io_scrape"]},{"action":"replace","regex":"(.+)","source_labels":["__meta_kubernetes_pod_annotation_prometheus_io_path"],"target_label":"__metrics_path__"},{"action":"replace","regex":"([^:]+)(?::\\d+)?;(\\d+)","replacement":"$1:$2","source_labels":["__address__","__meta_kubernetes_pod_annotation_prometheus_io_port"],"target_label":"__address__"},{"action":"labelmap","regex":"__meta_kubernetes_pod_label_(.+)"},{"action":"replace","source_labels":["__meta_kubernetes_namespace"],"target_label":"kubernetes_namespace"},{"action":"replace","source_labels":["__meta_kubernetes_pod_name"],"target_label":"kubernetes_pod_name"}]}]}` | Scrape config |
| server.scrape.config.scrape_configs | list | `[{"job_name":"victoriametrics","static_configs":[{"targets":["localhost:8428"]}]},{"bearer_token_file":"/var/run/secrets/kubernetes.io/serviceaccount/token","job_name":"kubernetes-apiservers","kubernetes_sd_configs":[{"role":"endpoints"}],"relabel_configs":[{"action":"keep","regex":"default;kubernetes;https","source_labels":["__meta_kubernetes_namespace","__meta_kubernetes_service_name","__meta_kubernetes_endpoint_port_name"]}],"scheme":"https","tls_config":{"ca_file":"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt","insecure_skip_verify":true}},{"bearer_token_file":"/var/run/secrets/kubernetes.io/serviceaccount/token","job_name":"kubernetes-nodes","kubernetes_sd_configs":[{"role":"node"}],"relabel_configs":[{"action":"labelmap","regex":"__meta_kubernetes_node_label_(.+)"},{"replacement":"kubernetes.default.svc:443","target_label":"__address__"},{"regex":"(.+)","replacement":"/api/v1/nodes/$1/proxy/metrics","source_labels":["__meta_kubernetes_node_name"],"target_label":"__metrics_path__"}],"scheme":"https","tls_config":{"ca_file":"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt","insecure_skip_verify":true}},{"bearer_token_file":"/var/run/secrets/kubernetes.io/serviceaccount/token","honor_timestamps":false,"job_name":"kubernetes-nodes-cadvisor","kubernetes_sd_configs":[{"role":"node"}],"relabel_configs":[{"action":"labelmap","regex":"__meta_kubernetes_node_label_(.+)"},{"replacement":"kubernetes.default.svc:443","target_label":"__address__"},{"regex":"(.+)","replacement":"/api/v1/nodes/$1/proxy/metrics/cadvisor","source_labels":["__meta_kubernetes_node_name"],"target_label":"__metrics_path__"}],"scheme":"https","tls_config":{"ca_file":"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt","insecure_skip_verify":true}},{"job_name":"kubernetes-service-endpoints","kubernetes_sd_configs":[{"role":"endpoints"}],"relabel_configs":[{"action":"drop","regex":true,"source_labels":["__meta_kubernetes_pod_container_init"]},{"action":"keep_if_equal","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_port","__meta_kubernetes_pod_container_port_number"]},{"action":"keep","regex":true,"source_labels":["__meta_kubernetes_service_annotation_prometheus_io_scrape"]},{"action":"replace","regex":"(https?)","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_scheme"],"target_label":"__scheme__"},{"action":"replace","regex":"(.+)","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_path"],"target_label":"__metrics_path__"},{"action":"replace","regex":"([^:]+)(?::\\d+)?;(\\d+)","replacement":"$1:$2","source_labels":["__address__","__meta_kubernetes_service_annotation_prometheus_io_port"],"target_label":"__address__"},{"action":"labelmap","regex":"__meta_kubernetes_service_label_(.+)"},{"action":"replace","source_labels":["__meta_kubernetes_namespace"],"target_label":"kubernetes_namespace"},{"action":"replace","source_labels":["__meta_kubernetes_service_name"],"target_label":"kubernetes_name"},{"action":"replace","source_labels":["__meta_kubernetes_pod_node_name"],"target_label":"kubernetes_node"}]},{"job_name":"kubernetes-service-endpoints-slow","kubernetes_sd_configs":[{"role":"endpoints"}],"relabel_configs":[{"action":"drop","regex":true,"source_labels":["__meta_kubernetes_pod_container_init"]},{"action":"keep_if_equal","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_port","__meta_kubernetes_pod_container_port_number"]},{"action":"keep","regex":true,"source_labels":["__meta_kubernetes_service_annotation_prometheus_io_scrape_slow"]},{"action":"replace","regex":"(https?)","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_scheme"],"target_label":"__scheme__"},{"action":"replace","regex":"(.+)","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_path"],"target_label":"__metrics_path__"},{"action":"replace","regex":"([^:]+)(?::\\d+)?;(\\d+)","replacement":"$1:$2","source_labels":["__address__","__meta_kubernetes_service_annotation_prometheus_io_port"],"target_label":"__address__"},{"action":"labelmap","regex":"__meta_kubernetes_service_label_(.+)"},{"action":"replace","source_labels":["__meta_kubernetes_namespace"],"target_label":"kubernetes_namespace"},{"action":"replace","source_labels":["__meta_kubernetes_service_name"],"target_label":"kubernetes_name"},{"action":"replace","source_labels":["__meta_kubernetes_pod_node_name"],"target_label":"kubernetes_node"}],"scrape_interval":"5m","scrape_timeout":"30s"},{"job_name":"kubernetes-services","kubernetes_sd_configs":[{"role":"service"}],"metrics_path":"/probe","params":{"module":["http_2xx"]},"relabel_configs":[{"action":"keep","regex":true,"source_labels":["__meta_kubernetes_service_annotation_prometheus_io_probe"]},{"source_labels":["__address__"],"target_label":"__param_target"},{"replacement":"blackbox","target_label":"__address__"},{"source_labels":["__param_target"],"target_label":"instance"},{"action":"labelmap","regex":"__meta_kubernetes_service_label_(.+)"},{"source_labels":["__meta_kubernetes_namespace"],"target_label":"kubernetes_namespace"},{"source_labels":["__meta_kubernetes_service_name"],"target_label":"kubernetes_name"}]},{"job_name":"kubernetes-pods","kubernetes_sd_configs":[{"role":"pod"}],"relabel_configs":[{"action":"drop","regex":true,"source_labels":["__meta_kubernetes_pod_container_init"]},{"action":"keep_if_equal","source_labels":["__meta_kubernetes_pod_annotation_prometheus_io_port","__meta_kubernetes_pod_container_port_number"]},{"action":"keep","regex":true,"source_labels":["__meta_kubernetes_pod_annotation_prometheus_io_scrape"]},{"action":"replace","regex":"(.+)","source_labels":["__meta_kubernetes_pod_annotation_prometheus_io_path"],"target_label":"__metrics_path__"},{"action":"replace","regex":"([^:]+)(?::\\d+)?;(\\d+)","replacement":"$1:$2","source_labels":["__address__","__meta_kubernetes_pod_annotation_prometheus_io_port"],"target_label":"__address__"},{"action":"labelmap","regex":"__meta_kubernetes_pod_label_(.+)"},{"action":"replace","source_labels":["__meta_kubernetes_namespace"],"target_label":"kubernetes_namespace"},{"action":"replace","source_labels":["__meta_kubernetes_pod_name"],"target_label":"kubernetes_pod_name"}]}]` | Scrape targets |
| server.scrape.config.scrape_configs[0] | object | `{"job_name":"victoriametrics","static_configs":[{"targets":["localhost:8428"]}]}` | Scrape rule for scrape victoriametrics |
| server.scrape.config.scrape_configs[4] | object | `{"job_name":"kubernetes-service-endpoints","kubernetes_sd_configs":[{"role":"endpoints"}],"relabel_configs":[{"action":"drop","regex":true,"source_labels":["__meta_kubernetes_pod_container_init"]},{"action":"keep_if_equal","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_port","__meta_kubernetes_pod_container_port_number"]},{"action":"keep","regex":true,"source_labels":["__meta_kubernetes_service_annotation_prometheus_io_scrape"]},{"action":"replace","regex":"(https?)","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_scheme"],"target_label":"__scheme__"},{"action":"replace","regex":"(.+)","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_path"],"target_label":"__metrics_path__"},{"action":"replace","regex":"([^:]+)(?::\\d+)?;(\\d+)","replacement":"$1:$2","source_labels":["__address__","__meta_kubernetes_service_annotation_prometheus_io_port"],"target_label":"__address__"},{"action":"labelmap","regex":"__meta_kubernetes_service_label_(.+)"},{"action":"replace","source_labels":["__meta_kubernetes_namespace"],"target_label":"kubernetes_namespace"},{"action":"replace","source_labels":["__meta_kubernetes_service_name"],"target_label":"kubernetes_name"},{"action":"replace","source_labels":["__meta_kubernetes_pod_node_name"],"target_label":"kubernetes_node"}]}` | Scrape rule using kubernetes service discovery for endpoints |
| server.scrape.config.scrape_configs[5] | object | `{"job_name":"kubernetes-service-endpoints-slow","kubernetes_sd_configs":[{"role":"endpoints"}],"relabel_configs":[{"action":"drop","regex":true,"source_labels":["__meta_kubernetes_pod_container_init"]},{"action":"keep_if_equal","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_port","__meta_kubernetes_pod_container_port_number"]},{"action":"keep","regex":true,"source_labels":["__meta_kubernetes_service_annotation_prometheus_io_scrape_slow"]},{"action":"replace","regex":"(https?)","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_scheme"],"target_label":"__scheme__"},{"action":"replace","regex":"(.+)","source_labels":["__meta_kubernetes_service_annotation_prometheus_io_path"],"target_label":"__metrics_path__"},{"action":"replace","regex":"([^:]+)(?::\\d+)?;(\\d+)","replacement":"$1:$2","source_labels":["__address__","__meta_kubernetes_service_annotation_prometheus_io_port"],"target_label":"__address__"},{"action":"labelmap","regex":"__meta_kubernetes_service_label_(.+)"},{"action":"replace","source_labels":["__meta_kubernetes_namespace"],"target_label":"kubernetes_namespace"},{"action":"replace","source_labels":["__meta_kubernetes_service_name"],"target_label":"kubernetes_name"},{"action":"replace","source_labels":["__meta_kubernetes_pod_node_name"],"target_label":"kubernetes_node"}],"scrape_interval":"5m","scrape_timeout":"30s"}` | Scrape config for slow service endpoints; same as above, but with a larger timeout and a larger interval  The relabeling allows the actual service scrape endpoint to be configured via the following annotations:  * `prometheus.io/scrape-slow`: Only scrape services that have a value of `true` * `prometheus.io/scheme`: If the metrics endpoint is secured then you will need to set this to `https` & most likely set the `tls_config` of the scrape config. * `prometheus.io/path`: If the metrics path is not `/metrics` override this. * `prometheus.io/port`: If the metrics are exposed on a different port to the service then set this appropriately. |
| server.scrape.config.scrape_configs[6] | object | `{"job_name":"kubernetes-services","kubernetes_sd_configs":[{"role":"service"}],"metrics_path":"/probe","params":{"module":["http_2xx"]},"relabel_configs":[{"action":"keep","regex":true,"source_labels":["__meta_kubernetes_service_annotation_prometheus_io_probe"]},{"source_labels":["__address__"],"target_label":"__param_target"},{"replacement":"blackbox","target_label":"__address__"},{"source_labels":["__param_target"],"target_label":"instance"},{"action":"labelmap","regex":"__meta_kubernetes_service_label_(.+)"},{"source_labels":["__meta_kubernetes_namespace"],"target_label":"kubernetes_namespace"},{"source_labels":["__meta_kubernetes_service_name"],"target_label":"kubernetes_name"}]}` | Example scrape config for probing services via the Blackbox Exporter.  The relabeling allows the actual service scrape endpoint to be configured via the following annotations:  * `prometheus.io/probe`: Only probe services that have a value of `true` |
| server.scrape.config.scrape_configs[7] | object | `{"job_name":"kubernetes-pods","kubernetes_sd_configs":[{"role":"pod"}],"relabel_configs":[{"action":"drop","regex":true,"source_labels":["__meta_kubernetes_pod_container_init"]},{"action":"keep_if_equal","source_labels":["__meta_kubernetes_pod_annotation_prometheus_io_port","__meta_kubernetes_pod_container_port_number"]},{"action":"keep","regex":true,"source_labels":["__meta_kubernetes_pod_annotation_prometheus_io_scrape"]},{"action":"replace","regex":"(.+)","source_labels":["__meta_kubernetes_pod_annotation_prometheus_io_path"],"target_label":"__metrics_path__"},{"action":"replace","regex":"([^:]+)(?::\\d+)?;(\\d+)","replacement":"$1:$2","source_labels":["__address__","__meta_kubernetes_pod_annotation_prometheus_io_port"],"target_label":"__address__"},{"action":"labelmap","regex":"__meta_kubernetes_pod_label_(.+)"},{"action":"replace","source_labels":["__meta_kubernetes_namespace"],"target_label":"kubernetes_namespace"},{"action":"replace","source_labels":["__meta_kubernetes_pod_name"],"target_label":"kubernetes_pod_name"}]}` | Example scrape config for pods  The relabeling allows the actual pod scrape endpoint to be configured via the following annotations:  * `prometheus.io/scrape`: Only scrape pods that have a value of `true` * `prometheus.io/path`: If the metrics path is not `/metrics` override this. * `prometheus.io/port`: Scrape the pod on the indicated port instead of the default of `9102`. |
| server.scrape.configMap | string | `""` | Use existing configmap if specified otherwise .config values will be used |
| server.scrape.enabled | bool | `false` | If true scrapes targets, creates config map or use specified one with scrape targets |
| server.scrape.extraScrapeConfigs | list | `[]` | Extra scrape configs that will be appended to `server.scrape.config` |
| server.securityContext | object | `{}` | Security context to be added to server pods |
| server.service.annotations | object | `{}` | Service annotations |
| server.service.clusterIP | string | `""` | Service ClusterIP |
| server.service.externalIPs | list | `[]` | Service External IPs. Ref: [https://kubernetes.io/docs/user-guide/services/#external-ips]( https://kubernetes.io/docs/user-guide/services/#external-ips) |
| server.service.labels | object | `{}` | Service labels |
| server.service.loadBalancerIP | string | `""` | Service load balacner IP |
| server.service.loadBalancerSourceRanges | list | `[]` | Load balancer source range |
| server.service.servicePort | int | `8428` | Service port |
| server.service.type | string | `"ClusterIP"` | Service type |
| server.serviceMonitor.annotations | object | `{}` | Service Monitor annotations |
| server.serviceMonitor.enabled | bool | `false` | Enable deployment of Service Monitor for server component. This is Prometheus operator object |
| server.serviceMonitor.extraLabels | object | `{}` | Service Monitor labels |
| server.serviceMonitor.relabelings | list | `[]` | Service Monitor relabelings |
| server.startupProbe | object | `{}` |  |
| server.statefulSet.enabled | bool | `true` | Creates statefulset instead of deployment, useful when you want to keep the cache |
| server.statefulSet.podManagementPolicy | string | `"OrderedReady"` | Deploy order policy for StatefulSet pods |
| server.statefulSet.service.annotations | object | `{}` | Headless service annotations |
| server.statefulSet.service.labels | object | `{}` | Headless service labels |
| server.statefulSet.service.servicePort | int | `8428` | Headless service port |
| server.terminationGracePeriodSeconds | int | `60` | Pod's termination grace period in seconds |
| server.tolerations | list | `[]` | Node tolerations for server scheduling to nodes with taints. Ref: [https://kubernetes.io/docs/concepts/configuration/assign-pod-node/](https://kubernetes.io/docs/concepts/configuration/assign-pod-node/) |
| server.vmbackupmanager.destination | string | `""` | backup destination at S3, GCS or local filesystem. Release name will be included to path! |
| server.vmbackupmanager.disableDaily | bool | `false` | disable daily backups |
| server.vmbackupmanager.disableHourly | bool | `false` | disable hourly backups |
| server.vmbackupmanager.disableMonthly | bool | `false` | disable monthly backups |
| server.vmbackupmanager.disableWeekly | bool | `false` | disable weekly backups |
| server.vmbackupmanager.enable | bool | `false` | enable automatic creation of backup via vmbackupmanager. vmbackupmanager is part of Enterprise packages |
| server.vmbackupmanager.env | list | `[]` | Additional environment variables (ex.: secret tokens, flags) https://github.com/VictoriaMetrics/VictoriaMetrics#environment-variables |
| server.vmbackupmanager.eula | bool | `false` | should be true and means that you have the legal right to run a backup manager that can either be a signed contract or an email with confirmation to run the service in a trial period # https://victoriametrics.com/legal/esa/ |
| server.vmbackupmanager.extraArgs."envflag.enable" | string | `"true"` |  |
| server.vmbackupmanager.extraArgs."envflag.prefix" | string | `"VM_"` |  |
| server.vmbackupmanager.extraArgs.loggerFormat | string | `"json"` |  |
| server.vmbackupmanager.extraVolumeMounts | list | `[]` |  |
| server.vmbackupmanager.image.repository | string | `"victoriametrics/vmbackupmanager"` | vmbackupmanager image repository |
| server.vmbackupmanager.image.tag | string | `"v1.97.1-enterprise"` | vmbackupmanager image tag |
| server.vmbackupmanager.livenessProbe.failureThreshold | int | `10` |  |
| server.vmbackupmanager.livenessProbe.initialDelaySeconds | int | `30` |  |
| server.vmbackupmanager.livenessProbe.periodSeconds | int | `30` |  |
| server.vmbackupmanager.livenessProbe.tcpSocket.port | string | `"manager-http"` |  |
| server.vmbackupmanager.livenessProbe.timeoutSeconds | int | `5` |  |
| server.vmbackupmanager.readinessProbe.failureThreshold | int | `3` |  |
| server.vmbackupmanager.readinessProbe.httpGet.path | string | `"/health"` |  |
| server.vmbackupmanager.readinessProbe.httpGet.port | string | `"manager-http"` |  |
| server.vmbackupmanager.readinessProbe.initialDelaySeconds | int | `5` |  |
| server.vmbackupmanager.readinessProbe.periodSeconds | int | `15` |  |
| server.vmbackupmanager.readinessProbe.timeoutSeconds | int | `5` |  |
| server.vmbackupmanager.resources | object | `{}` |  |
| server.vmbackupmanager.restore | object | `{"onStart":{"enabled":false}}` | Allows to enable restore options for pod. Read more: https://docs.victoriametrics.com/vmbackupmanager.html#restore-commands |
| server.vmbackupmanager.retention | object | `{"keepLastDaily":2,"keepLastHourly":2,"keepLastMonthly":2,"keepLastWeekly":2}` | backups' retention settings |
| server.vmbackupmanager.retention.keepLastDaily | int | `2` | keep last N daily backups. 0 means delete all existing daily backups. Specify -1 to turn off |
| server.vmbackupmanager.retention.keepLastHourly | int | `2` | keep last N hourly backups. 0 means delete all existing hourly backups. Specify -1 to turn off |
| server.vmbackupmanager.retention.keepLastMonthly | int | `2` | keep last N monthly backups. 0 means delete all existing monthly backups. Specify -1 to turn off |
| server.vmbackupmanager.retention.keepLastWeekly | int | `2` | keep last N weekly backups. 0 means delete all existing weekly backups. Specify -1 to turn off |
| serviceAccount.automountToken | bool | `true` | Mount API token to pod directly |
| serviceAccount.create | bool | `true` | Create service account. |
| serviceAccount.extraLabels | object | `{}` |  |