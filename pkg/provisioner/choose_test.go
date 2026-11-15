package provisioner

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// spotL40S is a pool that buys spot L40S machines from verda.
func spotL40S(change ...func(*types.WorkerPoolConfig)) types.WorkerPoolConfig {
	pool := types.WorkerPoolConfig{
		GPUType:      "L40S",
		Provisioning: types.WorkerPoolProvisioningConfig{Provider: "verda", Spot: true, MaxNodes: 4},
	}
	for _, apply := range change {
		apply(&pool)
	}
	return pool
}

// l40s is a spot machine with one L40S for a dollar an hour.
func l40s(change ...func(*providers.Offer)) providers.Offer {
	offer := providers.Offer{
		ID: "1L40S.20V", Provider: "verda", Region: "FIN-01",
		GPU: "L40S", GPUCount: 1, CPUMillicores: 20_000, MemoryMB: 60_000,
		HourlyCostMicros: 100 * microsPerCent, Spot: true,
	}
	for _, apply := range change {
		apply(&offer)
	}
	return offer
}

func costing(cents int64) func(*providers.Offer) {
	return func(offer *providers.Offer) { offer.HourlyCostMicros = cents * microsPerCent }
}

func in(region string) func(*providers.Offer) {
	return func(offer *providers.Offer) { offer.Region = region }
}

func named(id string) func(*providers.Offer) {
	return func(offer *providers.Offer) { offer.ID = id }
}

func TestSells(t *testing.T) {
	cpuPool := spotL40S(func(pool *types.WorkerPoolConfig) { pool.GPUType = "" })
	cpuOffer := l40s(func(offer *providers.Offer) { offer.GPU, offer.GPUCount = "", 0 })

	tests := []struct {
		name  string
		offer providers.Offer
		pool  types.WorkerPoolConfig
		want  bool
	}{
		{"the pool's GPU, spot as configured", l40s(), spotL40S(), true},
		{"GPU names compare without case", l40s(func(o *providers.Offer) { o.GPU = "l40s" }), spotL40S(), true},
		{"on-demand for a spot pool", l40s(func(o *providers.Offer) { o.Spot = false }), spotL40S(), false},
		{"spot for an on-demand pool", l40s(), spotL40S(func(p *types.WorkerPoolConfig) { p.Provisioning.Spot = false }), false},
		{"on-demand for an on-demand pool", l40s(func(o *providers.Offer) { o.Spot = false }), spotL40S(func(p *types.WorkerPoolConfig) { p.Provisioning.Spot = false }), true},
		{"another GPU", l40s(func(o *providers.Offer) { o.GPU = "A10G" }), spotL40S(), false},
		{"a GPU type with no GPUs", l40s(func(o *providers.Offer) { o.GPUCount = 0 }), spotL40S(), false},
		{"a CPU machine for a GPU pool", cpuOffer, spotL40S(), false},
		{"a CPU machine for a CPU pool", cpuOffer, cpuPool, true},
		{"a GPU machine for a CPU pool", l40s(), cpuPool, false},
		{"GPUs with no type for a CPU pool", l40s(func(o *providers.Offer) { o.GPU = "" }), cpuPool, false},
		{"in one of the pool's regions", l40s(in("FIN-02")), spotL40S(func(p *types.WorkerPoolConfig) { p.Provisioning.Regions = []string{"FIN-01", "FIN-02"} }), true},
		{"outside the pool's regions", l40s(in("ICE-01")), spotL40S(func(p *types.WorkerPoolConfig) { p.Provisioning.Regions = []string{"FIN-01", "FIN-02"} }), false},
		{"without a price", l40s(costing(0)), spotL40S(), false},
		{"at the pool's price limit", l40s(costing(150)), spotL40S(func(p *types.WorkerPoolConfig) { p.Provisioning.MaxNodeHourlyCents = 150 }), true},
		{"over the pool's price limit", l40s(costing(151)), spotL40S(func(p *types.WorkerPoolConfig) { p.Provisioning.MaxNodeHourlyCents = 150 }), false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, sells(test.offer, test.pool))
		})
	}
}

func TestHolds(t *testing.T) {
	offer := l40s() // 20 cores, 60000 MB of which 54000 can be counted on, 1 GPU

	tests := []struct {
		name string
		need scheduler.MachineNeed
		want bool
	}{
		{"nothing in particular", scheduler.MachineNeed{}, true},
		{"all of its cores", scheduler.MachineNeed{CPU: 20_000}, true},
		{"more cores than it has", scheduler.MachineNeed{CPU: 20_001}, false},
		{"the memory that can be counted on", scheduler.MachineNeed{Memory: 54_000}, true},
		{"memory the kernel keeps", scheduler.MachineNeed{Memory: 54_001}, false},
		{"its GPU", scheduler.MachineNeed{GPUCount: 1}, true},
		{"more GPUs than it has", scheduler.MachineNeed{GPUCount: 2}, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, holds(offer, test.need))
		})
	}
}

func TestChoose(t *testing.T) {
	// held returns n records of machines in the pool, at cents an hour each.
	held := func(pool string, n int, cents int64) []Machine {
		machines := make([]Machine, n)
		for i := range machines {
			machines[i] = Machine{Pool: pool, HourlyMicros: cents * microsPerCent}
		}
		return machines
	}
	releasing := held("spot-l40s", 1, 100)
	releasing[0].Release = &Release{Reason: "it had nothing to run"}

	tests := []struct {
		name    string
		buy     purchase
		want    string // the offer's ID and region
		wantErr error
	}{
		{
			name: "the cheapest that fits",
			buy:  purchase{offers: []providers.Offer{l40s(costing(120), named("big")), l40s(costing(90), named("small")), l40s(costing(100), named("medium"))}},
			want: "small in FIN-01",
		},
		{
			name: "the same price: by region, then by name",
			buy:  purchase{offers: []providers.Offer{l40s(named("b"), in("FIN-02")), l40s(named("b")), l40s(named("a"), in("FIN-02")), l40s(named("c"))}},
			want: "b in FIN-01",
		},
		{
			name: "a cheaper machine that is too small is passed over",
			buy: purchase{
				need:   scheduler.MachineNeed{GPUCount: 2},
				offers: []providers.Offer{l40s(costing(90)), l40s(costing(190), named("2L40S"), func(o *providers.Offer) { o.GPUCount = 2 })},
			},
			want: "2L40S in FIN-01",
		},
		{
			name: "a cheaper machine the pool does not buy is passed over",
			buy:  purchase{offers: []providers.Offer{l40s(costing(40), named("on-demand"), func(o *providers.Offer) { o.Spot = false }), l40s(costing(90))}},
			want: "1L40S.20V in FIN-01",
		},
		{
			name: "one below maxNodes",
			buy:  purchase{held: held("spot-l40s", 3, 100), offers: []providers.Offer{l40s()}},
			want: "1L40S.20V in FIN-01",
		},
		{
			name:    "at maxNodes",
			buy:     purchase{held: held("spot-l40s", 4, 100), offers: []providers.Offer{l40s()}},
			wantErr: errAtMaxNodes,
		},
		{
			name:    "a machine being given back counts until its record is gone",
			buy:     purchase{held: append(held("spot-l40s", 3, 100), releasing...), offers: []providers.Offer{l40s()}},
			wantErr: errAtMaxNodes,
		},
		{
			name: "other pools' machines do not count towards maxNodes",
			buy:  purchase{held: held("spot-a10g", 9, 100), offers: []providers.Offer{l40s()}},
			want: "1L40S.20V in FIN-01",
		},
		{
			name: "exactly the hourly cap",
			buy:  purchase{limits: Limits{MaxHourlyCents: 400}, held: held("spot-l40s", 3, 100), offers: []providers.Offer{l40s()}},
			want: "1L40S.20V in FIN-01",
		},
		{
			name:    "a cent over the hourly cap",
			buy:     purchase{limits: Limits{MaxHourlyCents: 399}, held: held("spot-l40s", 3, 100), offers: []providers.Offer{l40s()}},
			wantErr: errHourlyCap,
		},
		{
			name:    "the hourly cap covers every pool's machines",
			buy:     purchase{limits: Limits{MaxHourlyCents: 400}, held: append(held("spot-a10g", 2, 150), held("spot-l40s", 1, 100)...), offers: []providers.Offer{l40s()}},
			wantErr: errHourlyCap,
		},
		{
			name: "under the daily cap",
			buy:  purchase{limits: Limits{MaxDailyCents: 1000}, spentToday: 999.5, offers: []providers.Offer{l40s()}},
			want: "1L40S.20V in FIN-01",
		},
		{
			name:    "the daily cap is spent",
			buy:     purchase{limits: Limits{MaxDailyCents: 1000}, spentToday: 1000, offers: []providers.Offer{l40s()}},
			wantErr: errDailyCap,
		},
		{
			name: "no cap",
			buy:  purchase{spentToday: 1e9, held: held("spot-a10g", 50, 1000), offers: []providers.Offer{l40s()}},
			want: "1L40S.20V in FIN-01",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.buy.pool, test.buy.config = "spot-l40s", spotL40S()

			offer, err := choose(test.buy)
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, offer.ID+" in "+offer.Region)
		})
	}
}

// A pool that finds nothing to buy says what stands in the way: the first
// thing it asks for that nothing in stock has.
func TestChooseSaysWhatLeftNoOffer(t *testing.T) {
	onDemand := func(o *providers.Offer) { o.Spot = false }
	a10g := func(o *providers.Offer) { o.GPU = "A10G" }
	cpu := func(o *providers.Offer) { o.GPU, o.GPUCount = "", 0 }

	onDemandPool := func(pool *types.WorkerPoolConfig) { pool.Provisioning.Spot = false }
	cpuPool := func(pool *types.WorkerPoolConfig) { pool.GPUType = "" }
	inRegions := func(regions ...string) func(*types.WorkerPoolConfig) {
		return func(pool *types.WorkerPoolConfig) { pool.Provisioning.Regions = regions }
	}
	payingAtMost := func(cents int64) func(*types.WorkerPoolConfig) {
		return func(pool *types.WorkerPoolConfig) { pool.Provisioning.MaxNodeHourlyCents = cents }
	}

	tests := []struct {
		name   string
		pool   []func(*types.WorkerPoolConfig)
		need   scheduler.MachineNeed
		offers []providers.Offer
		want   string
	}{
		{
			name: "nothing in stock",
			want: "verda is not known to have anything in stock",
		},
		{
			name:   "a GPU type the provider does not sell",
			offers: []providers.Offer{l40s(a10g), l40s(cpu)},
			want:   "verda has no L40S in stock",
		},
		{
			name:   "a GPU type listed without a price",
			offers: []providers.Offer{l40s(costing(0))},
			want:   "verda has no L40S in stock",
		},
		{
			name:   "no machine without a GPU",
			pool:   []func(*types.WorkerPoolConfig){cpuPool},
			offers: []providers.Offer{l40s()},
			want:   "verda has no CPU machine in stock",
		},
		{
			name:   "spot is not asked for, and the provider sells nothing else",
			pool:   []func(*types.WorkerPoolConfig){onDemandPool},
			offers: []providers.Offer{l40s(), l40s(a10g, onDemand)},
			want:   "verda sells L40S only as spot: set provisioning.spot",
		},
		{
			name:   "spot is asked for, and the provider sells none",
			offers: []providers.Offer{l40s(onDemand)},
			want:   "verda sells L40S only on demand: remove provisioning.spot",
		},
		{
			name:   "machines without a GPU, sold only as spot",
			pool:   []func(*types.WorkerPoolConfig){cpuPool, onDemandPool},
			offers: []providers.Offer{l40s(cpu)},
			want:   "verda sells CPU machines only as spot: set provisioning.spot",
		},
		{
			name:   "regions the provider does not offer",
			pool:   []func(*types.WorkerPoolConfig){inRegions("FIN-03", "ICE-01")},
			offers: []providers.Offer{l40s(in("FIN-02")), l40s(), l40s(named("2L40S.40V")), l40s(in("ICE-01"), onDemand), l40s(in("FIN-03"), a10g)},
			want:   "verda offers no L40S in regions [FIN-03 ICE-01]; it offers them in FIN-01, FIN-02",
		},
		{
			name:   "a price limit under the cheapest",
			pool:   []func(*types.WorkerPoolConfig){payingAtMost(100)},
			offers: []providers.Offer{l40s(costing(150)), l40s(costing(135), named("1L40S.10V")), l40s(costing(90), onDemand)},
			want:   "verda has no L40S within provisioning.maxNodeHourlyCents 100: the cheapest costs 135 cents an hour",
		},
		{
			name:   "a price limit under the cheapest in the pool's regions",
			pool:   []func(*types.WorkerPoolConfig){payingAtMost(100), inRegions("FIN-02")},
			offers: []providers.Offer{l40s(costing(90)), l40s(in("FIN-02"), func(o *providers.Offer) { o.HourlyCostMicros = 1_353_700 })},
			want:   "verda has no L40S within provisioning.maxNodeHourlyCents 100: the cheapest costs 135.37 cents an hour",
		},
		{
			name:   "nothing the pool buys is big enough",
			need:   scheduler.MachineNeed{CPU: 8000, Memory: 32_000, GPUCount: 2},
			offers: []providers.Offer{l40s(), l40s(named("2L40S.40V"), onDemand, func(o *providers.Offer) { o.GPUCount = 2 })},
			want:   "no L40S that verda has for the pool has room for what is needed (cpu 8000m, memory 32000 MiB, gpus 2)",
		},

		// The first thing that stands in the way is the one that is said.
		{
			name:   "sold the other way, and elsewhere, and for too much",
			pool:   []func(*types.WorkerPoolConfig){onDemandPool, inRegions("FIN-03"), payingAtMost(50)},
			offers: []providers.Offer{l40s()},
			want:   "verda sells L40S only as spot: set provisioning.spot",
		},
		{
			name:   "elsewhere, and for too much",
			pool:   []func(*types.WorkerPoolConfig){inRegions("FIN-03"), payingAtMost(50)},
			offers: []providers.Offer{l40s()},
			want:   "verda offers no L40S in regions [FIN-03]; it offers them in FIN-01",
		},
		{
			name:   "for too much, and too small",
			pool:   []func(*types.WorkerPoolConfig){payingAtMost(50)},
			need:   scheduler.MachineNeed{GPUCount: 2},
			offers: []providers.Offer{l40s()},
			want:   "verda has no L40S within provisioning.maxNodeHourlyCents 50: the cheapest costs 100 cents an hour",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := choose(purchase{pool: "spot-l40s", config: spotL40S(test.pool...), need: test.need, offers: test.offers})
			require.EqualError(t, err, test.want)
		})
	}
}
