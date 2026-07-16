# CHANGELOG for `victoria-metrics-single` helm-chart

## Next release

- TODO

## 0.9.15

**Release date:** 2024-02-01

![AppVersion: v1.97.1](https://img.shields.io/static/v1?label=AppVersion&message=v1.97.1&color=success&logo=)
![Helm: v3](https://img.shields.io/static/v1?label=Helm&message=v3&color=informational&logo=helm)

- bump version of VM components to [v1.97.1](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.97.1)

## 0.9.14

**Release date:** 2023-12-13

![AppVersion: v1.96.0](https://img.shields.io/static/v1?label=AppVersion&message=v1.96.0&color=success&logo=)
![Helm: v3](https://img.shields.io/static/v1?label=Helm&message=v3&color=informational&logo=helm)

- Fix configuration of volume mount for license key referenced by using secret.

## 0.9.13

**Release date:** 2023-12-12

![AppVersion: v1.96.0](https://img.shields.io/static/v1?label=AppVersion&message=v1.96.0&color=success&logo=)
![Helm: v3](https://img.shields.io/static/v1?label=Helm&message=v3&color=informational&logo=helm)

- bump version of VM components to [v1.96.0](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.96.0)

## 0.9.12

**Release date:** 2023-11-16

![AppVersion: v1.95.1](https://img.shields.io/static/v1?label=AppVersion&message=v1.95.1&color=success&logo=)
![Helm: v3](https://img.shields.io/static/v1?label=Helm&message=v3&color=informational&logo=helm)

- bump version of VM components to [v1.95.1](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.95.1)

## 0.9.11

**Release date:** 2023-11-15

![AppVersion: v1.95.0](https://img.shields.io/static/v1?label=AppVersion&message=v1.95.0&color=success&logo=)
![Helm: v3](https://img.shields.io/static/v1?label=Helm&message=v3&color=informational&logo=helm)

- bump version of VM components to [v1.95.0](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.95.0)

## 0.9.10

**Release date:** 2023-10-12

![AppVersion: v1.94.0](https://img.shields.io/static/v1?label=AppVersion&message=v1.94.0&color=success&logo=)
![Helm: v3](https://img.shields.io/static/v1?label=Helm&message=v3&color=informational&logo=helm)

- Add license enforcement for vmbackupmanager in order to avoid running it without enterprise license key. See [these docs](https://docs.victoriametrics.com/enterprise.html) for details.
- Fix license flags not being passed to vmsingle when using StatefulSet.

## 0.9.9

**Release date:** 2023-10-04

![AppVersion: v1.94.0](https://img.shields.io/static/v1?label=AppVersion&message=v1.94.0&color=success&logo=)
![Helm: v3](https://img.shields.io/static/v1?label=Helm&message=v3&color=informational&logo=helm)

- bump version of VM components to [v1.94.0](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.94.0)
- Add support of providing enterprise license key for VictoriaMetrics enterprise. See [these docs](https://docs.victoriametrics.com/enterprise.html) for details.

## 0.9.8

**Release date:** 2023-09-21

![AppVersion: v1.93.5](https://img.shields.io/static/v1?label=AppVersion&message=v1.93.5&color=success&logo=)
![Helm: v3](https://img.shields.io/static/v1?label=Helm&message=v3&color=informational&logo=helm)

- Bump version of VM components to [v1.93.5](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.93.5)

## 0.9.7

**Release date:** 2023-09-11

![AppVersion: v1.93.4](https://img.shields.io/static/v1?label=AppVersion&message=v1.93.4&color=success&logo=)
![Helm: v3](https://img.shields.io/static/v1?label=Helm&message=v3&color=informational&logo=helm)

- Bump version of VM components to [v1.93.4](https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.93.4)

## 0.9.6

**Release date:** 2023-09-04

![AppVersion: v1.93.3](https://img.shields.io/static/v1?label=AppVersion&message=v1.93.3&color=success&logo=)
![Helm: v3](https://img.shields.io/static/v1?label=Helm&message=v3&color=informational&logo=helm)

- Bump version of Victoria Metrics to `v1.93.3`
- Fixed `kubeVersion` in `Chart.yaml`

## 0.9.5

**Release date:** 2023-08-30

![AppVersion: v1.93.1](https://img.shields.io/static/v1?label=AppVersion&message=v1.93.1&color=success&logo=)
![Helm: v3](https://img.shields.io/static/v1?label=Helm&message=v3&color=informational&logo=helm)

* Disable `honorTimestamps` for cadvisor scrape job by default (#617)

## 0.9.4

**Release date:** 2023-08-23

![AppVersion: v1.93.1](https://img.shields.io/static/v1?label=AppVersion&message=v1.93.0&color=success&logo=)
![Helm: v3](https://img.shields.io/static/v1?label=Helm&message=v3&color=informational&logo=helm)

* Update VictoriaMetrics components from v1.93.0 to v1.93.1

## 0.9.3

**Release date:** 2023-08-12

![AppVersion: v1.93.0](https://img.shields.io/static/v1?label=AppVersion&message=v1.93.0&color=success&logo=)
![Helm: v3](https://img.shields.io/static/v1?label=Helm&message=v3&color=informational&logo=helm)

* Update VictoriaMetrics components from v1.92.1 to v1.93.0

## 0.9.2

**Release date:** 2023-07-28

![AppVersion: v1.92.1](https://img.shields.io/static/v1?label=AppVersion&message=v1.92.1&color=success&logo=)
![Helm: v3](https://img.shields.io/static/v1?label=Helm&message=v3&color=informational&logo=helm)

* Update VictoriaMetrics components from v1.92.0 to v1.92.1 (#599)

## 0.9.1

**Release date:** 2023-07-27

![AppVersion: v1.92.0](https://img.shields.io/static/v1?label=AppVersion&message=v1.92.0&color=success&logo=)
![Helm: v3](https://img.shields.io/static/v1?label=Helm&message=v3&color=informational&logo=helm)

* Update VictoriaMetrics components from v1.91.3 to v1.92.0
* fix misused securityContext and podSecurityContext (#592)

## 0.9.0

**Release date:** 2023-07-13

![AppVersion: v1.91.3](https://img.shields.io/static/v1?label=AppVersion&message=v1.91.3&color=success&logo=)
![Helm: v3](https://img.shields.io/static/v1?label=Helm&message=v3&color=informational&logo=helm)

* bump version of agent, alert, auth, cluster, single
* feat(vmsingle): Add ability to deploy extra manifests (#587)
