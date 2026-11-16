package repository

import (
	"context"
	"fmt"
	"hash/fnv"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	redis "github.com/redis/go-redis/v9"
)

// renewContainerLeasesScript is the routine status heartbeat for some
// containers of one worker. For each it reads the state, checks it and renews
// it in one step, so it needs no lock and nothing can change the state in
// between.
//
// It applies the rules UpdateContainerStatus applies to a heartbeat. A
// missing state is reported and never recreated. A state assigned to another
// worker is left alone. STOPPING is terminal and its lease belongs to
// finalization, so it is reported and not renewed. A PENDING container whose
// runtime has started becomes RUNNING, which sets started_at; the next
// heartbeat finds it RUNNING and only renews it.
//
// KEYS[1] is the state index and each key after it one container's state.
// ARGV[8] onwards are the containers' flags, in the order of their keys: "s"
// when the runtime has started, "a" when the renewal announces the container.
// The reply has one entry per container, in the same order. A state that
// cannot be read is reported for that container alone.
var renewContainerLeasesScript = redis.NewScript(`
local worker_id, workspace_id = ARGV[1], ARGV[2]
local now, running_ttl, pending_ttl = tonumber(ARGV[3]), tonumber(ARGV[4]), tonumber(ARGV[5])
local pending, running = ARGV[6], ARGV[7]

local function renew(key, flags)
	local state = redis.pcall("HMGET", key, "status", "worker_id", "workspace_id", "evicting", "drain_seconds")
	if type(state) ~= "table" or state.err then
		return {"error", (type(state) == "table" and state.err) or "the state cannot be read"}
	end
	local status = state[1]
	if not status and redis.call("EXISTS", key) == 0 then
		return {"missing"}
	end
	if state[2] == "" or state[2] ~= worker_id or (workspace_id ~= "" and state[3] ~= workspace_id) then
		return {"unassigned"}
	end

	local started = "0"
	if status == pending and string.find(flags, "s", 1, true) then
		redis.call("HSET", key, "status", running, "started_at", ARGV[3])
		status = running
		started = "1"
	end
	local ttl = (status == running and running_ttl) or (status == pending and pending_ttl)
	if ttl then
		redis.call("EXPIRE", key, ttl)
		redis.call("ZADD", KEYS[1], now + ttl, key)
		if started == "0" and string.find(flags, "a", 1, true) then
			redis.call("HSET", key, "status", status)
		end
	end
	return {"found", status or "", started, state[4] or "", state[5] or ""}
end

local leases = {}
for n = 2, #KEYS do
	leases[n - 1] = renew(KEYS[n], ARGV[6 + n])
end
return leases
`)

// containerLeasesPerScript is how many containers one run of the script
// renews. A script costs Redis about as much to start as one container costs
// to renew, so renewing them one script each nearly doubled the work. Redis
// serves nothing else while a script runs: fifty containers keep it for
// under a millisecond.
const containerLeasesPerScript = 50

const (
	// containerLeaseAnnounceEvery is how many renewals apart a container's
	// status is rewritten, unchanged, and containerLeaseAnnounceSlot is the
	// interval workers renew at.
	//
	// Renewing a lease writes nothing to the state, so the gateways that
	// listen for writes to container state hear nothing of it. The heartbeat
	// this replaces wrote the status every time, and a gateway that had
	// missed a container's creation, or had let go of its stub's instance,
	// picked the container up again from that write within one interval. One
	// renewal in six keeps that, about a minute apart, for a sixth of the
	// events. Containers take turns, so the writes are spread evenly.
	containerLeaseAnnounceEvery = 6
	containerLeaseAnnounceSlot  = 10
)

// containerLeaseAnnounces reports whether a renewal at this time, in unix
// seconds, is one that rewrites the container's status.
func containerLeaseAnnounces(containerId string, now int64) bool {
	turn := fnv.New32a()
	turn.Write([]byte(containerId))
	return (int64(turn.Sum32()%containerLeaseAnnounceEvery)+now/containerLeaseAnnounceSlot)%containerLeaseAnnounceEvery == 0
}

// RenewContainerLeases is the routine status heartbeat for many containers of
// one worker. It costs one round trip to Redis whatever their number, where
// GetContainerState followed by UpdateContainerStatus takes a lock and
// several round trips for each container. workspaceId is empty for a worker
// that serves every workspace.
func (cr *ContainerRedisRepository) RenewContainerLeases(ctx context.Context, workerId, workspaceId string, renewals []types.ContainerLeaseRenewal) []types.ContainerLease {
	leases := make([]types.ContainerLease, len(renewals))
	indexKey := common.RedisKeys.SchedulerContainerStateIndex()
	now := time.Now().Unix()

	containers := make([]int, 0, len(renewals))
	for i, renewal := range renewals {
		if renewal.ContainerId == "" {
			leases[i] = types.ContainerLease{Err: &types.ErrContainerNotAssigned{}}
			continue
		}
		if strings.HasPrefix(renewal.ContainerId, "managed-") {
			// UpdateContainerStatus gives a managed endpoint a longer lease
			// after looking up its replica. Renewing it here would cut that
			// lease short.
			leases[i] = types.ContainerLease{
				ContainerId: renewal.ContainerId,
				Err:         fmt.Errorf("managed container <%s> renews its lease with UpdateContainerStatus", renewal.ContainerId),
			}
			continue
		}
		containers = append(containers, i)
	}

	var groups [][]int
	for group := range slices.Chunk(containers, containerLeasesPerScript) {
		groups = append(groups, group)
	}
	replies := make([]*redis.Cmd, len(groups))

	// run sends the script for these groups of containers in one pipeline.
	run := func(script func(context.Context, redis.Scripter, []string, ...interface{}) *redis.Cmd, which []int) {
		pipe := cr.rdb.Pipeline()
		for _, g := range which {
			keys := make([]string, 0, 1+len(groups[g]))
			keys = append(keys, indexKey)
			args := make([]interface{}, 0, 7+len(groups[g]))
			args = append(args, workerId, workspaceId, now, types.ContainerStateTtlS, types.ContainerStateTtlSWhilePending,
				string(types.ContainerStatusPending), string(types.ContainerStatusRunning))
			for _, i := range groups[g] {
				flags := ""
				if renewals[i].RuntimeStarted {
					flags += "s"
				}
				if containerLeaseAnnounces(renewals[i].ContainerId, now) {
					flags += "a"
				}
				keys = append(keys, common.RedisKeys.SchedulerContainerState(renewals[i].ContainerId))
				args = append(args, flags)
			}
			replies[g] = script(ctx, pipe, keys, args...)
		}
		// Exec reports the first command that failed. Each reply is read on
		// its own below.
		_, _ = pipe.Exec(ctx)
	}

	all := make([]int, len(groups))
	for g := range groups {
		all[g] = g
	}
	run(renewContainerLeasesScript.EvalSha, all)

	// Redis forgets its scripts when it restarts. Sending the script's text
	// runs it and caches it again for the calls that follow.
	var uncached []int
	for g := range groups {
		if redis.HasErrorPrefix(replies[g].Err(), "NOSCRIPT") {
			uncached = append(uncached, g)
		}
	}
	if len(uncached) > 0 {
		run(renewContainerLeasesScript.Eval, uncached)
	}

	for g, group := range groups {
		entries, err := replies[g].Slice()
		if err == nil && len(entries) != len(group) {
			err = fmt.Errorf("%d replies for %d containers", len(entries), len(group))
		}
		for n, i := range group {
			if err != nil {
				leases[i] = types.ContainerLease{
					ContainerId: renewals[i].ContainerId,
					Err:         fmt.Errorf("failed to renew container lease <%s>: %w", renewals[i].ContainerId, err),
				}
				continue
			}
			leases[i] = containerLease(renewals[i].ContainerId, entries[n])
		}
	}
	return leases
}

// containerLease reads what renewContainerLeasesScript returned for a container.
func containerLease(containerId string, reply interface{}) types.ContainerLease {
	lease := types.ContainerLease{ContainerId: containerId}

	values, _ := reply.([]interface{})
	fields := make([]string, len(values))
	for n, value := range values {
		fields[n], _ = value.(string)
	}
	switch {
	case len(fields) == 1 && fields[0] == "missing":
		lease.Err = &types.ErrContainerStateNotFound{ContainerId: containerId}
	case len(fields) == 1 && fields[0] == "unassigned":
		lease.Err = &types.ErrContainerNotAssigned{}
	case len(fields) == 5 && fields[0] == "found":
		lease.Status = types.ContainerStatus(fields[1])
		lease.Started = fields[2] == "1"
		// Read the way GetContainerState reads these fields: a value that
		// does not parse counts as unset.
		if evicting, err := strconv.ParseBool(fields[3]); err == nil {
			lease.Evicting = evicting
		}
		if drainSeconds, err := strconv.ParseUint(fields[4], 10, 32); err == nil {
			lease.DrainSeconds = uint32(drainSeconds)
		}

		switch lease.Status {
		case types.ContainerStatusPending, types.ContainerStatusRunning, types.ContainerStatusStopping:
		default:
			lease.Err = fmt.Errorf("invalid status: %s", lease.Status)
		}
	case len(fields) == 2 && fields[0] == "error":
		lease.Err = fmt.Errorf("failed to renew container lease <%s>: %s", containerId, fields[1])
	default:
		lease.Err = fmt.Errorf("failed to renew container lease <%s>: unexpected reply %q", containerId, fields)
	}
	return lease
}
