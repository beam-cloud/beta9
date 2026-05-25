from __future__ import annotations

from .model import Measurement


class Validator:
    def validate(self, measurements: list[Measurement]) -> list[str]:
        failures: list[str] = []
        for measurement in measurements:
            failures.extend(self._validate_measurement(measurement))
        return failures

    def _validate_measurement(self, measurement: Measurement) -> list[str]:
        failures: list[str] = []
        if measurement.status != "ok":
            failures.append(
                f"{measurement.suite}/{measurement.scenario}/{measurement.measurement}: {measurement.status}"
                + (f" ({measurement.error})" if measurement.error else "")
            )
            return failures

        tags = measurement.tags
        evidence = measurement.evidence
        if tags.get("requires_sha") and evidence.get("sha_ok") is not True:
            failures.append(
                f"{measurement.suite}/{measurement.scenario}/{measurement.measurement}: missing SHA proof"
            )
        if tags.get("requires_cache_hit") and evidence.get("cache_hit") is not True:
            failures.append(
                f"{measurement.suite}/{measurement.scenario}/{measurement.measurement}: missing embedded-cache hit proof"
            )
        if tags.get("requires_remote_worker") and evidence.get("remote_worker") is not True:
            failures.append(
                f"{measurement.suite}/{measurement.scenario}/{measurement.measurement}: missing different-worker remote proof"
            )
        if tags.get("reject_cloud_read") and evidence.get("cloud_read"):
            failures.append(
                f"{measurement.suite}/{measurement.scenario}/{measurement.measurement}: cloud read observed during hot cache scenario"
            )
        min_mbps = tags.get("min_mbps")
        if min_mbps is not None and measurement.mbps and measurement.mbps < float(min_mbps):
            failures.append(
                f"{measurement.suite}/{measurement.scenario}/{measurement.measurement}: "
                f"{measurement.mbps:.2f} MB/s below {float(min_mbps):.2f} MB/s"
            )
        return failures
