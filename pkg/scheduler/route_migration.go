package scheduler

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// This file reads scheduling.failover.chains, which alsoServes on each pool
// replaced, so a config written the old way keeps routing for one release.
// It is deleted together with the old key.

// chainMigration is what a config's chains turn into.
type chainMigration struct {
	// alsoServes is the GPU types each pool takes over from the chains.
	alsoServes map[string][]string
	// write is the config that says the same thing the new way.
	write []string
	// reordered describes each chain whose pools are now tried in a different
	// order.
	reordered []string
	// notes are other things that behave differently.
	notes []string
}

// migrateFailoverChains converts the chains and works out what the operator
// needs to know. It is a pure function of the config.
func migrateFailoverChains(failover types.FailoverConfig, pools map[string]types.WorkerPoolConfig) chainMigration {
	migration := chainMigration{alsoServes: failover.AlsoServes()}
	if len(failover.Chains) == 0 {
		return migration
	}
	if !failover.Enabled {
		migration.notes = append(migration.notes,
			"scheduling.failover is disabled, so its chains have no effect; remove the section")
		return migration
	}

	for _, pool := range slices.Sorted(maps.Keys(migration.alsoServes)) {
		line := fmt.Sprintf("worker.pools.%s.alsoServes: [%s]", pool, strings.Join(migration.alsoServes[pool], ", "))
		if _, configured := pools[pool]; !configured {
			line += "  (not in this config: set also_serves on it through the pools API)"
		}
		migration.write = append(migration.write, line)
	}

	for _, gpu := range slices.Sorted(maps.Keys(failover.Chains)) {
		// Chains named their pools in the order to try them. Pools that also
		// serve a GPU type are tried by priority, then name.
		var was []string
		for _, pool := range failover.Chains[gpu].Pools {
			if _, configured := pools[pool]; configured {
				was = append(was, pool)
			}
		}
		now := slices.Clone(was)
		slices.SortStableFunc(now, func(a, b string) int {
			return cmp.Or(cmp.Compare(pools[b].Priority, pools[a].Priority), strings.Compare(a, b))
		})
		if !slices.Equal(was, now) {
			migration.reordered = append(migration.reordered, fmt.Sprintf(
				"%s fallbacks were tried in the order [%s] and are now tried by priority, then name: [%s]; set worker.pools.<name>.priority to keep the old order",
				gpu, strings.Join(was, ", "), strings.Join(now, ", ")))
		}
	}

	if len(failover.Chains) > 1 {
		migration.notes = append(migration.notes,
			"a request that lists several GPU types now uses the fallback pools of all of them; it used only those of the first listed type that had a chain")
	}
	return migration
}

// warn logs what replaces the chains, once, at startup.
func (m chainMigration) warn() {
	if len(m.write) > 0 {
		log.Warn().Strs("write_instead", m.write).
			Msg("scheduling.failover.chains is deprecated and stops being read in the next release; it was converted to alsoServes on each pool")
	}
	for _, change := range m.reordered {
		log.Warn().Msg("scheduling.failover.chains: " + change)
	}
	for _, note := range m.notes {
		log.Warn().Msg("scheduling.failover: " + note)
	}
}

// apply adds the GPU types a pool takes over from the chains to its
// alsoServes.
func (m chainMigration) apply(name string, config types.WorkerPoolConfig) types.WorkerPoolConfig {
	for _, gpu := range m.alsoServes[name] {
		if !config.Serves(gpu) && !strings.EqualFold(config.GPUType, gpu) {
			config.AlsoServes = append(slices.Clone(config.AlsoServes), gpu)
		}
	}
	return config
}
