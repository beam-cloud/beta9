# Helm Chart For Victoria Metrics Agent.

 ![Version: 0.9.17](https://img.shields.io/badge/Version-0.9.17-informational?style=flat-square)
[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/victoriametrics)](https://artifacthub.io/packages/helm/victoriametrics/victoria-metrics-agent)
[![Slack](https://img.shields.io/badge/join%20slack-%23victoriametrics-brightgreen.svg)](https://slack.victoriametrics.com/)

Victoria Metrics Agent - collects metrics from various sources and stores them to VictoriaMetrics

## Prerequisites

* Install the follow packages: ``git``, ``kubectl``, ``helm``, ``helm-docs``. See this [tutorial](../../REQUIREMENTS.md).

## How to install

Access a Kubernetes cluster.

Add a chart helm repository with follow commands:

```console
helm repo add vm https://victoriametrics.github.io/helm-charts/

helm repo update
```

List versions of ``vm/victoria-metrics-agent`` chart available to installation:

```console
helm search repo vm/victoria-metrics-agent -l
```

Export default values of ``victoria-metrics-agent`` chart to file ``values.yaml``:

```console
helm show values vm/victoria-metrics-agent > values.yaml
```

Change the values according to the need of the environment in ``values.yaml`` file.

Test the installation with command:

```console
helm install vmagent vm/victoria-metrics-agent -f values.yaml -n NAMESPACE --debug --dry-run
```

Install chart with command:

```console
helm install vmagent vm/victoria-metrics-agent -f values.yaml -n NAMESPACE
```

Get the pods lists by running this commands:

```console
kubectl get pods -A | grep 'agent'
```

Get the application by running this command:

```console
helm list -f vmagent -n NAMESPACE
```

See the history of versions of ``vmagent`` application with command.

```console
helm history vmagent -n NAMESPACE
```

## How to uninstall

Remove application with command.

```console
helm uninstall vmagent -n NAMESPACE
```

## Documentation of Helm Chart

Install ``helm-docs`` following the instructions on this [tutorial](../../REQUIREMENTS.md).

Generate docs with ``helm-docs`` command.

```bash
cd charts/victoria-metrics-agent

helm-docs
```

The markdown generation is entirely go template driven. The tool parses metadata from charts and generates a number of sub-templates that can be referenced in a template file (by default ``README.md.gotmpl``). If no template file is provided, the tool has a default internal template that will generate a reasonably formatted README.

## Parameters

The following tables lists the configurable parameters of the chart and their default values.

Change the values according to the need of the environment in ``victoria-metrics-agent/values.yaml`` file.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` |  |
| annotations | object | `{}` |  |
| config.global.scrape_interval | string | `"10s"` |  |
| config.scrape_configs[0].job_name | string | `"vmagent"` |  |
| config.scrape_configs[0].static_configs[0].targets[0] | string | `"localhost:8429"` |  |
| config.scrape_configs[1].bearer_token_file | string | `"/var/run/secrets/kubernetes.io/serviceaccount/token"` |  |
| config.scrape_configs[1].job_name | string | `"kubernetes-apiservers"` |  |
| config.scrape_configs[1].kubernetes_sd_configs[0].role | string | `"endpoints"` |  |
| config.scrape_configs[1].relabel_configs[0].action | string | `"keep"` |  |
| config.scrape_configs[1].relabel_configs[0].regex | string | `"default;kubernetes;https"` |  |
| config.scrape_configs[1].relabel_configs[0].source_labels[0] | string | `"__meta_kubernetes_namespace"` |  |
| config.scrape_configs[1].relabel_configs[0].source_labels[1] | string | `"__meta_kubernetes_service_name"` |  |
| config.scrape_configs[1].relabel_configs[0].source_labels[2] | string | `"__meta_kubernetes_endpoint_port_name"` |  |
| config.scrape_configs[1].scheme | string | `"https"` |  |
| config.scrape_configs[1].tls_config.ca_file | string | `"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"` |  |
| config.scrape_configs[1].tls_config.insecure_skip_verify | bool | `true` |  |
| config.scrape_configs[2].bearer_token_file | string | `"/var/run/secrets/kubernetes.io/serviceaccount/token"` |  |
| config.scrape_configs[2].job_name | string | `"kubernetes-nodes"` |  |
| config.scrape_configs[2].kubernetes_sd_configs[0].role | string | `"node"` |  |
| config.scrape_configs[2].relabel_configs[0].action | string | `"labelmap"` |  |
| config.scrape_configs[2].relabel_configs[0].regex | string | `"__meta_kubernetes_node_label_(.+)"` |  |
| config.scrape_configs[2].relabel_configs[1].replacement | string | `"kubernetes.default.svc:443"` |  |
| config.scrape_configs[2].relabel_configs[1].target_label | string | `"__address__"` |  |
| config.scrape_configs[2].relabel_configs[2].regex | string | `"(.+)"` |  |
| config.scrape_configs[2].relabel_configs[2].replacement | string | `"/api/v1/nodes/$1/proxy/metrics"` |  |
| config.scrape_configs[2].relabel_configs[2].source_labels[0] | string | `"__meta_kubernetes_node_name"` |  |
| config.scrape_configs[2].relabel_configs[2].target_label | string | `"__metrics_path__"` |  |
| config.scrape_configs[2].scheme | string | `"https"` |  |
| config.scrape_configs[2].tls_config.ca_file | string | `"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"` |  |
| config.scrape_configs[2].tls_config.insecure_skip_verify | bool | `true` |  |
| config.scrape_configs[3].bearer_token_file | string | `"/var/run/secrets/kubernetes.io/serviceaccount/token"` |  |
| config.scrape_configs[3].honor_timestamps | bool | `false` |  |
| config.scrape_configs[3].job_name | string | `"kubernetes-nodes-cadvisor"` |  |
| config.scrape_configs[3].kubernetes_sd_configs[0].role | string | `"node"` |  |
| config.scrape_configs[3].relabel_configs[0].action | string | `"labelmap"` |  |
| config.scrape_configs[3].relabel_configs[0].regex | string | `"__meta_kubernetes_node_label_(.+)"` |  |
| config.scrape_configs[3].relabel_configs[1].replacement | string | `"kubernetes.default.svc:443"` |  |
| config.scrape_configs[3].relabel_configs[1].target_label | string | `"__address__"` |  |
| config.scrape_configs[3].relabel_configs[2].regex | string | `"(.+)"` |  |
| config.scrape_configs[3].relabel_configs[2].replacement | string | `"/api/v1/nodes/$1/proxy/metrics/cadvisor"` |  |
| config.scrape_configs[3].relabel_configs[2].source_labels[0] | string | `"__meta_kubernetes_node_name"` |  |
| config.scrape_configs[3].relabel_configs[2].target_label | string | `"__metrics_path__"` |  |
| config.scrape_configs[3].scheme | string | `"https"` |  |
| config.scrape_configs[3].tls_config.ca_file | string | `"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"` |  |
| config.scrape_configs[3].tls_config.insecure_skip_verify | bool | `true` |  |
| config.scrape_configs[4].job_name | string | `"kubernetes-service-endpoints"` |  |
| config.scrape_configs[4].kubernetes_sd_configs[0].role | string | `"endpointslices"` |  |
| config.scrape_configs[4].relabel_configs[0].action | string | `"drop"` |  |
| config.scrape_configs[4].relabel_configs[0].regex | bool | `true` |  |
| config.scrape_configs[4].relabel_configs[0].source_labels[0] | string | `"__meta_kubernetes_pod_container_init"` |  |
| config.scrape_configs[4].relabel_configs[10].source_labels[0] | string | `"__meta_kubernetes_service_name"` |  |
| config.scrape_configs[4].relabel_configs[10].target_label | string | `"service"` |  |
| config.scrape_configs[4].relabel_configs[11].replacement | string | `"${1}"` |  |
| config.scrape_configs[4].relabel_configs[11].source_labels[0] | string | `"__meta_kubernetes_service_name"` |  |
| config.scrape_configs[4].relabel_configs[11].target_label | string | `"job"` |  |
| config.scrape_configs[4].relabel_configs[12].action | string | `"replace"` |  |
| config.scrape_configs[4].relabel_configs[12].source_labels[0] | string | `"__meta_kubernetes_pod_node_name"` |  |
| config.scrape_configs[4].relabel_configs[12].target_label | string | `"node"` |  |
| config.scrape_configs[4].relabel_configs[1].action | string | `"keep_if_equal"` |  |
| config.scrape_configs[4].relabel_configs[1].source_labels[0] | string | `"__meta_kubernetes_service_annotation_prometheus_io_port"` |  |
| config.scrape_configs[4].relabel_configs[1].source_labels[1] | string | `"__meta_kubernetes_pod_container_port_number"` |  |
| config.scrape_configs[4].relabel_configs[2].action | string | `"keep"` |  |
| config.scrape_configs[4].relabel_configs[2].regex | bool | `true` |  |
| config.scrape_configs[4].relabel_configs[2].source_labels[0] | string | `"__meta_kubernetes_service_annotation_prometheus_io_scrape"` |  |
| config.scrape_configs[4].relabel_configs[3].action | string | `"replace"` |  |
| config.scrape_configs[4].relabel_configs[3].regex | string | `"(https?)"` |  |
| config.scrape_configs[4].relabel_configs[3].source_labels[0] | string | `"__meta_kubernetes_service_annotation_prometheus_io_scheme"` |  |
| config.scrape_configs[4].relabel_configs[3].target_label | string | `"__scheme__"` |  |
| config.scrape_configs[4].relabel_configs[4].action | string | `"replace"` |  |
| config.scrape_configs[4].relabel_configs[4].regex | string | `"(.+)"` |  |
| config.scrape_configs[4].relabel_configs[4].source_labels[0] | string | `"__meta_kubernetes_service_annotation_prometheus_io_path"` |  |
| config.scrape_configs[4].relabel_configs[4].target_label | string | `"__metrics_path__"` |  |
| config.scrape_configs[4].relabel_configs[5].action | string | `"replace"` |  |
| config.scrape_configs[4].relabel_configs[5].regex | string | `"([^:]+)(?::\\d+)?;(\\d+)"` |  |
| config.scrape_configs[4].relabel_configs[5].replacement | string | `"$1:$2"` |  |
| config.scrape_configs[4].relabel_configs[5].source_labels[0] | string | `"__address__"` |  |
| config.scrape_configs[4].relabel_configs[5].source_labels[1] | string | `"__meta_kubernetes_service_annotation_prometheus_io_port"` |  |
| config.scrape_configs[4].relabel_configs[5].target_label | string | `"__address__"` |  |
| config.scrape_configs[4].relabel_configs[6].action | string | `"labelmap"` |  |
| config.scrape_configs[4].relabel_configs[6].regex | string | `"__meta_kubernetes_service_label_(.+)"` |  |
| config.scrape_configs[4].relabel_configs[7].source_labels[0] | string | `"__meta_kubernetes_pod_name"` |  |
| config.scrape_configs[4].relabel_configs[7].target_label | string | `"pod"` |  |
| config.scrape_configs[4].relabel_configs[8].source_labels[0] | string | `"__meta_kubernetes_pod_container_name"` |  |
| config.scrape_configs[4].relabel_configs[8].target_label | string | `"container"` |  |
| config.scrape_configs[4].relabel_configs[9].source_labels[0] | string | `"__meta_kubernetes_namespace"` |  |
| config.scrape_configs[4].relabel_configs[9].target_label | string | `"namespace"` |  |
| config.scrape_configs[5].job_name | string | `"kubernetes-service-endpoints-slow"` |  |
| config.scrape_configs[5].kubernetes_sd_configs[0].role | string | `"endpointslices"` |  |
| config.scrape_configs[5].relabel_configs[0].action | string | `"drop"` |  |
| config.scrape_configs[5].relabel_configs[0].regex | bool | `true` |  |
| config.scrape_configs[5].relabel_configs[0].source_labels[0] | string | `"__meta_kubernetes_pod_container_init"` |  |
| config.scrape_configs[5].relabel_configs[10].source_labels[0] | string | `"__meta_kubernetes_service_name"` |  |
| config.scrape_configs[5].relabel_configs[10].target_label | string | `"service"` |  |
| config.scrape_configs[5].relabel_configs[11].replacement | string | `"${1}"` |  |
| config.scrape_configs[5].relabel_configs[11].source_labels[0] | string | `"__meta_kubernetes_service_name"` |  |
| config.scrape_configs[5].relabel_configs[11].target_label | string | `"job"` |  |
| config.scrape_configs[5].relabel_configs[12].action | string | `"replace"` |  |
| config.scrape_configs[5].relabel_configs[12].source_labels[0] | string | `"__meta_kubernetes_pod_node_name"` |  |
| config.scrape_configs[5].relabel_configs[12].target_label | string | `"node"` |  |
| config.scrape_configs[5].relabel_configs[1].action | string | `"keep_if_equal"` |  |
| config.scrape_configs[5].relabel_configs[1].source_labels[0] | string | `"__meta_kubernetes_service_annotation_prometheus_io_port"` |  |
| config.scrape_configs[5].relabel_configs[1].source_labels[1] | string | `"__meta_kubernetes_pod_container_port_number"` |  |
| config.scrape_configs[5].relabel_configs[2].action | string | `"keep"` |  |
| config.scrape_configs[5].relabel_configs[2].regex | bool | `true` |  |
| config.scrape_configs[5].relabel_configs[2].source_labels[0] | string | `"__meta_kubernetes_service_annotation_prometheus_io_scrape_slow"` |  |
| config.scrape_configs[5].relabel_configs[3].action | string | `"replace"` |  |
| config.scrape_configs[5].relabel_configs[3].regex | string | `"(https?)"` |  |
| config.scrape_configs[5].relabel_configs[3].source_labels[0] | string | `"__meta_kubernetes_service_annotation_prometheus_io_scheme"` |  |
| config.scrape_configs[5].relabel_configs[3].target_label | string | `"__scheme__"` |  |
| config.scrape_configs[5].relabel_configs[4].action | string | `"replace"` |  |
| config.scrape_configs[5].relabel_configs[4].regex | string | `"(.+)"` |  |
| config.scrape_configs[5].relabel_configs[4].source_labels[0] | string | `"__meta_kubernetes_service_annotation_prometheus_io_path"` |  |
| config.scrape_configs[5].relabel_configs[4].target_label | string | `"__metrics_path__"` |  |
| config.scrape_configs[5].relabel_configs[5].action | string | `"replace"` |  |
| config.scrape_configs[5].relabel_configs[5].regex | string | `"([^:]+)(?::\\d+)?;(\\d+)"` |  |
| config.scrape_configs[5].relabel_configs[5].replacement | string | `"$1:$2"` |  |
| config.scrape_configs[5].relabel_configs[5].source_labels[0] | string | `"__address__"` |  |
| config.scrape_configs[5].relabel_configs[5].source_labels[1] | string | `"__meta_kubernetes_service_annotation_prometheus_io_port"` |  |
| config.scrape_configs[5].relabel_configs[5].target_label | string | `"__address__"` |  |
| config.scrape_configs[5].relabel_configs[6].action | string | `"labelmap"` |  |
| config.scrape_configs[5].relabel_configs[6].regex | string | `"__meta_kubernetes_service_label_(.+)"` |  |
| config.scrape_configs[5].relabel_configs[7].source_labels[0] | string | `"__meta_kubernetes_pod_name"` |  |
| config.scrape_configs[5].relabel_configs[7].target_label | string | `"pod"` |  |
| config.scrape_configs[5].relabel_configs[8].source_labels[0] | string | `"__meta_kubernetes_pod_container_name"` |  |
| config.scrape_configs[5].relabel_configs[8].target_label | string | `"container"` |  |
| config.scrape_configs[5].relabel_configs[9].source_labels[0] | string | `"__meta_kubernetes_namespace"` |  |
| config.scrape_configs[5].relabel_configs[9].target_label | string | `"namespace"` |  |
| config.scrape_configs[5].scrape_interval | string | `"5m"` |  |
| config.scrape_configs[5].scrape_timeout | string | `"30s"` |  |
| config.scrape_configs[6].job_name | string | `"kubernetes-services"` |  |
| config.scrape_configs[6].kubernetes_sd_configs[0].role | string | `"service"` |  |
| config.scrape_configs[6].metrics_path | string | `"/probe"` |  |
| config.scrape_configs[6].params.module[0] | string | `"http_2xx"` |  |
| config.scrape_configs[6].relabel_configs[0].action | string | `"keep"` |  |
| config.scrape_configs[6].relabel_configs[0].regex | bool | `true` |  |
| config.scrape_configs[6].relabel_configs[0].source_labels[0] | string | `"__meta_kubernetes_service_annotation_prometheus_io_probe"` |  |
| config.scrape_configs[6].relabel_configs[1].source_labels[0] | string | `"__address__"` |  |
| config.scrape_configs[6].relabel_configs[1].target_label | string | `"__param_target"` |  |
| config.scrape_configs[6].relabel_configs[2].replacement | string | `"blackbox"` |  |
| config.scrape_configs[6].relabel_configs[2].target_label | string | `"__address__"` |  |
| config.scrape_configs[6].relabel_configs[3].source_labels[0] | string | `"__param_target"` |  |
| config.scrape_configs[6].relabel_configs[3].target_label | string | `"instance"` |  |
| config.scrape_configs[6].relabel_configs[4].action | string | `"labelmap"` |  |
| config.scrape_configs[6].relabel_configs[4].regex | string | `"__meta_kubernetes_service_label_(.+)"` |  |
| config.scrape_configs[6].relabel_configs[5].source_labels[0] | string | `"__meta_kubernetes_namespace"` |  |
| config.scrape_configs[6].relabel_configs[5].target_label | string | `"namespace"` |  |
| config.scrape_configs[6].relabel_configs[6].source_labels[0] | string | `"__meta_kubernetes_service_name"` |  |
| config.scrape_configs[6].relabel_configs[6].target_label | string | `"service"` |  |
| config.scrape_configs[7].job_name | string | `"kubernetes-pods"` |  |
| config.scrape_configs[7].kubernetes_sd_configs[0].role | string | `"pod"` |  |
| config.scrape_configs[7].relabel_configs[0].action | string | `"drop"` |  |
| config.scrape_configs[7].relabel_configs[0].regex | bool | `true` |  |
| config.scrape_configs[7].relabel_configs[0].source_labels[0] | string | `"__meta_kubernetes_pod_container_init"` |  |
| config.scrape_configs[7].relabel_configs[1].action | string | `"keep_if_equal"` |  |
| config.scrape_configs[7].relabel_configs[1].source_labels[0] | string | `"__meta_kubernetes_pod_annotation_prometheus_io_port"` |  |
| config.scrape_configs[7].relabel_configs[1].source_labels[1] | string | `"__meta_kubernetes_pod_container_port_number"` |  |
| config.scrape_configs[7].relabel_configs[2].action | string | `"keep"` |  |
| config.scrape_configs[7].relabel_configs[2].regex | bool | `true` |  |
| config.scrape_configs[7].relabel_configs[2].source_labels[0] | string | `"__meta_kubernetes_pod_annotation_prometheus_io_scrape"` |  |
| config.scrape_configs[7].relabel_configs[3].action | string | `"replace"` |  |
| config.scrape_configs[7].relabel_configs[3].regex | string | `"(.+)"` |  |
| config.scrape_configs[7].relabel_configs[3].source_labels[0] | string | `"__meta_kubernetes_pod_annotation_prometheus_io_path"` |  |
| config.scrape_configs[7].relabel_configs[3].target_label | string | `"__metrics_path__"` |  |
| config.scrape_configs[7].relabel_configs[4].action | string | `"replace"` |  |
| config.scrape_configs[7].relabel_configs[4].regex | string | `"([^:]+)(?::\\d+)?;(\\d+)"` |  |
| config.scrape_configs[7].relabel_configs[4].replacement | string | `"$1:$2"` |  |
| config.scrape_configs[7].relabel_configs[4].source_labels[0] | string | `"__address__"` |  |
| config.scrape_configs[7].relabel_configs[4].source_labels[1] | string | `"__meta_kubernetes_pod_annotation_prometheus_io_port"` |  |
| config.scrape_configs[7].relabel_configs[4].target_label | string | `"__address__"` |  |
| config.scrape_configs[7].relabel_configs[5].action | string | `"labelmap"` |  |
| config.scrape_configs[7].relabel_configs[5].regex | string | `"__meta_kubernetes_pod_label_(.+)"` |  |
| config.scrape_configs[7].relabel_configs[6].source_labels[0] | string | `"__meta_kubernetes_pod_name"` |  |
| config.scrape_configs[7].relabel_configs[6].target_label | string | `"pod"` |  |
| config.scrape_configs[7].relabel_configs[7].source_labels[0] | string | `"__meta_kubernetes_pod_container_name"` |  |
| config.scrape_configs[7].relabel_configs[7].target_label | string | `"container"` |  |
| config.scrape_configs[7].relabel_configs[8].source_labels[0] | string | `"__meta_kubernetes_namespace"` |  |
| config.scrape_configs[7].relabel_configs[8].target_label | string | `"namespace"` |  |
| config.scrape_configs[7].relabel_configs[9].action | string | `"replace"` |  |
| config.scrape_configs[7].relabel_configs[9].source_labels[0] | string | `"__meta_kubernetes_pod_node_name"` |  |
| config.scrape_configs[7].relabel_configs[9].target_label | string | `"node"` |  |
| configMap | string | `""` |  |
| containerWorkingDir | string | `"/"` |  |
| deployment.enabled | bool | `true` |  |
| deployment.strategy | object | `{}` |  |
| env | list | `[]` | Additional environment variables (ex.: secret tokens, flags) https://github.com/VictoriaMetrics/VictoriaMetrics#environment-variables |
| envFrom | list | `[]` |  |
| extraArgs."envflag.enable" | string | `"true"` |  |
| extraArgs."envflag.prefix" | string | `"VM_"` |  |
| extraArgs.loggerFormat | string | `"json"` |  |
| extraContainers | list | `[]` |  |
| extraHostPathMounts | list | `[]` |  |
| extraLabels | object | `{}` |  |
| extraObjects | list | `[]` |  |
| extraScrapeConfigs | list | `[]` | Extra scrape configs that will be appended to `config` |
| extraVolumeMounts | list | `[]` |  |
| extraVolumes | list | `[]` |  |
| fullnameOverride | string | `""` |  |
| image.pullPolicy | string | `"IfNotPresent"` |  |
| image.repository | string | `"victoriametrics/vmagent"` |  |
| image.tag | string | `""` |  |
| imagePullSecrets | list | `[]` |  |
| ingress.annotations | object | `{}` |  |
| ingress.enabled | bool | `false` |  |
| ingress.extraLabels | object | `{}` |  |
| ingress.hosts | list | `[]` |  |
| ingress.pathType | string | `"Prefix"` | pathType is only for k8s >= 1.1= |
| ingress.tls | list | `[]` |  |
| initContainers | list | `[]` |  |
| license | object | `{"key":"","secret":{"key":"","name":""}}` | Enterprise license key configuration for VictoriaMetrics enterprise. Required only for VictoriaMetrics enterprise. Documentation - https://docs.victoriametrics.com/enterprise.html, for more information, visit https://victoriametrics.com/products/enterprise/ . To request a trial license, go to https://victoriametrics.com/products/enterprise/trial/ Supported starting from VictoriaMetrics v1.94.0 |
| license.key | string | `""` | License key |
| license.secret | object | `{"key":"","name":""}` | Use existing secret with license key |
| license.secret.key | string | `""` | Key in secret with license key |
| license.secret.name | string | `""` | Existing secret name |
| multiTenantUrls | list | `[]` |  |
| nameOverride | string | `""` |  |
| nodeSelector | object | `{}` |  |
| persistence.accessModes[0] | string | `"ReadWriteOnce"` |  |
| persistence.annotations | object | `{}` |  |
| persistence.enabled | bool | `false` |  |
| persistence.existingClaim | string | `""` |  |
| persistence.extraLabels | object | `{}` |  |
| persistence.matchLabels | object | `{}` | Bind Persistent Volume by labels. Must match all labels of targeted PV. |
| persistence.size | string | `"10Gi"` |  |
| podAnnotations | object | `{}` |  |
| podDisruptionBudget.enabled | bool | `false` |  |
| podDisruptionBudget.labels | object | `{}` |  |
| podLabels | object | `{}` |  |
| podSecurityContext | object | `{}` |  |
| priorityClassName | string | `""` | priority class to be assigned to the pod(s) |
| rbac.annotations | object | `{}` |  |
| rbac.create | bool | `true` |  |
| rbac.extraLabels | object | `{}` |  |
| rbac.namespaced | bool | `false` | if true and `rbac.enabled`, will deploy a Role/Rolebinding instead of a ClusterRole/ClusterRoleBinding |
| rbac.pspEnabled | bool | `true` |  |
| remoteWriteUrls | list | `[]` |  |
| replicaCount | int | `1` |  |
| resources | object | `{}` |  |
| securityContext | object | `{}` |  |
| service.annotations | object | `{}` |  |
| service.clusterIP | string | `""` |  |
| service.enabled | bool | `false` |  |
| service.externalIPs | list | `[]` |  |
| service.extraLabels | object | `{}` |  |
| service.loadBalancerIP | string | `""` |  |
| service.loadBalancerSourceRanges | list | `[]` |  |
| service.servicePort | int | `8429` |  |
| service.type | string | `"ClusterIP"` |  |
| serviceAccount.annotations | object | `{}` |  |
| serviceAccount.create | bool | `true` |  |
| serviceAccount.name | string | `nil` |  |
| serviceMonitor.annotations | object | `{}` |  |
| serviceMonitor.enabled | bool | `false` |  |
| serviceMonitor.extraLabels | object | `{}` |  |
| serviceMonitor.relabelings | list | `[]` |  |
| statefulset.clusterMode | bool | `false` | create cluster of vmagents. See https://docs.victoriametrics.com/vmagent.html#scraping-big-number-of-targets available since 1.77.2 version https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.77.2 |
| statefulset.enabled | bool | `false` |  |
| statefulset.replicationFactor | int | `1` | replication factor for vmagent in cluster mode |
| statefulset.updateStrategy | object | `{}` |  |
| tolerations | list | `[]` |  |
| topologySpreadConstraints | list | `[]` |  |
