# Gateway replacement reliability

The successful 112 ms retry demonstrated recovery after a failed request. It did
not demonstrate an uninterrupted gateway handoff. The earlier failed public
verification remains a failed availability check even though both Qwen replicas
survived.

## Verified causes and risks

Staging has one public gateway target: the single Okteto pod. The original
gateway deployment has zero replicas. The development pod has no Kubernetes
readiness/liveness/startup probes and a zero-second pod termination grace. Its
child gateway is stopped before the replacement begins listening, so a request
can arrive when no gateway process can serve it.

The source-reload helper also had a concrete shutdown bug: it allowed ten
seconds before SIGKILL, while the gateway waits ten seconds for readiness
propagation before starting its graceful shutdown. Logs confirm forced shutdown
on all three recent swaps:

| SIGTERM and drain start (UTC) | Replacement listening (UTC) | Helper forced termination |
| --- | --- | --- |
| 18:08:01 | 18:08:12 | Yes |
| 18:22:52 | 18:23:04 | Yes |
| 18:48:20 | 18:48:31 | Yes |

These timestamps identify process replacement; they do not measure the exact
client-visible outage duration. The original timed-out request does not include
a complete correlated network trace, so the precise TCP failure mechanism is
not proven.

Production has three healthy targets, normal gRPC readiness probes, rolling
updates, and a 195-second pod shutdown budget. It therefore has redundancy
absent from the Okteto setup. However, live configuration inspection found:

- Target-group unhealthy connection termination enabled with zero unhealthy
  draining interval. The gateway intentionally fails readiness during shutdown.
- Deregistration timeout 30 seconds with connection termination enabled,
  shorter than the gateway's 180-second HTTP drain.
- No AWS target-health readiness gates, allowing Kubernetes readiness to precede
  load-balancer target registration during a rolling update.

These settings are a deployment interruption risk, not proof of a production
incident. AWS documents that unhealthy connection termination closes existing
connections and recommends target-health readiness gates to coordinate rolling
updates with load-balancer registration. See [NLB target attributes](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/edit-target-group-attributes.html)
and [EKS load-balancing guidance](https://docs.aws.amazon.com/eks/latest/best-practices/load-balancing.html).

## Correction and validation boundary

The helper now defaults to a 195-second shutdown allowance (10 seconds readiness
propagation + 180 seconds HTTP drain + five seconds buffer), with a positive
`HOSTED_DEV_SHUTDOWN_SECONDS` override. Tests use actual child processes and the
helper's real shutdown functions: a 12-second graceful drain survives, a stuck
process is terminated at an explicitly shortened deadline, and an invalid
budget fails before starting a gateway. All three passed, as did Bash syntax
and diff checks.

This helper correction is prepared for the next watcher start. The current
watcher has already parsed its loop; synchronizing the script does not change
its running functions. No gateway swap was induced for this investigation, and
no production deployment or load-balancer settings were changed.

A single-gateway restart still has a listener gap after this correction.
Reliability signoff requires compatible overlapping gateways, working readiness
and load-balancer draining, and a test covering sustained new requests plus
existing streams through the entire replacement. Every timeout/reset/non-2xx
must count as a failure; a successful retry cannot erase it. Model/container
continuity and billing correctness should be checked separately.
