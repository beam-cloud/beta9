// Package vast contains the compatibility shim for machines that are already
// listed on Vast. It does not schedule Beam work itself. The normal agent still
// joins Beam and starts workers; this package only maps Vast default-job
// sentinel heartbeats into per-GPU agent service start/stop and preemption.
package vast
