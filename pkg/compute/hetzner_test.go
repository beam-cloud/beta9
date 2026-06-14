package compute

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHetznerListOffersUsesBearerTokenAndFiltersCPUServerTypes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer hetzner-token", r.Header.Get("Authorization"))
		require.Equal(t, "1", r.URL.Query().Get("page"))
		require.Equal(t, "50", r.URL.Query().Get("per_page"))

		switch r.URL.Path {
		case "/locations":
			_, _ = w.Write([]byte(`{
				"locations": [
					{"id":1,"name":"fsn1","description":"Falkenstein DC Park 1","country":"DE","city":"Falkenstein","latitude":50.47612,"longitude":12.370071,"network_zone":"eu-central"},
					{"id":2,"name":"ash","description":"Ashburn, VA","country":"US","city":"Ashburn","latitude":39.0438,"longitude":-77.4874,"network_zone":"us-east"}
				],
				"meta":{"pagination":{"page":1,"per_page":50,"previous_page":null,"next_page":null,"last_page":1,"total_entries":2}}
			}`))
		case "/server_types":
			_, _ = w.Write([]byte(`{
				"server_types": [
					{
						"id": 45,
						"name": "cpx31",
						"description": "CPX31",
						"cores": 4,
						"memory": 8,
						"disk": 160,
						"deprecated": false,
						"category": "Shared vCPU",
						"cpu_type": "shared",
						"storage_type": "local",
						"architecture": "x86",
						"locations": [
							{"id":1,"name":"fsn1","available":true,"deprecation":null},
							{"id":2,"name":"ash","available":true,"deprecation":null}
						],
						"prices": [
							{"location":"fsn1","price_hourly":{"net":"0.0208","gross":"0.0248"}},
							{"location":"ash","price_hourly":{"net":"0.0312","gross":"0.0371"}}
						]
					},
					{
						"id": 46,
						"name": "deprecated",
						"description": "Deprecated",
						"cores": 4,
						"memory": 8,
						"disk": 160,
						"deprecated": true,
						"locations": [{"id":2,"name":"ash","available":true,"deprecation":null}],
						"prices": [{"location":"ash","price_hourly":{"net":"0.0312","gross":"0.0371"}}]
					},
					{
						"id": 47,
						"name": "missing-price",
						"description": "Missing Price",
						"cores": 4,
						"memory": 8,
						"disk": 160,
						"deprecated": false,
						"locations": [{"id":2,"name":"ash","available":true,"deprecation":null}],
						"prices": []
					}
				],
				"meta":{"pagination":{"page":1,"per_page":50,"previous_page":null,"next_page":null,"last_page":1,"total_entries":3}}
			}`))
		default:
			t.Fatalf("unexpected Hetzner path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewHetzner(HetznerConfig{
		APIToken: "hetzner-token",
		BaseURL:  server.URL,
		RegionMetadata: map[string]HetznerRegionMetadata{
			"ash": {DisplayName: "Ashburn", Latitude: 39.0438, Longitude: -77.4874},
		},
		ServerTypeCategories: map[string]string{"cpx31": "shared"},
	})

	offers, err := client.ListOffers(context.Background(), OfferRequest{TotalMachines: 3})

	require.NoError(t, err)
	require.Len(t, offers, 1)
	require.Equal(t, "cpx31", offers[0].ID)
	require.Equal(t, "hetzner", offers[0].Provider)
	require.Equal(t, "hetzner", offers[0].Cloud)
	require.Equal(t, "ash", offers[0].Region)
	require.Equal(t, uint32(1), offers[0].MachineCount)
	require.Equal(t, uint32(0), offers[0].GPUCount)
	require.Equal(t, int64(4000), offers[0].CPUMillicores)
	require.Equal(t, int64(8192), offers[0].MemoryMB)
	require.Equal(t, int64(163840), offers[0].StorageMB)
	require.Equal(t, DollarsToMicros(0.0312), offers[0].HourlyCostMicros)
	require.Equal(t, uint32(3), offers[0].Available)
	require.Equal(t, "CPX31", offers[0].DisplayName)
	require.Equal(t, "shared", offers[0].Category)
	require.Equal(t, "Ashburn", offers[0].RegionDisplayName)
	require.Equal(t, "us-east", offers[0].Labels["network_zone"])
}

func TestHetznerCreateGetDeleteReservation(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	var createBody map[string]any
	var sawDelete bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "Bearer hetzner-token", r.Header.Get("Authorization"))

		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/networks":
			_, _ = w.Write([]byte(`{
				"networks": [{"id":456,"name":"beam-workers","subnets":[{"type":"cloud","ip_range":"10.42.0.0/24","network_zone":"us-east"}]}],
				"meta":{"pagination":{"page":1,"per_page":50,"previous_page":null,"next_page":null,"last_page":1,"total_entries":1}}
			}`))
		case r.Method == http.MethodPost && r.URL.Path == "/servers":
			require.NoError(t, json.Unmarshal(body, &createBody))
			_, _ = w.Write([]byte(`{"server":{"id":42,"name":"beam-workspace-cpu-pool-machine-1","status":"initializing","location":{"name":"ash"},"server_type":{"id":45,"name":"cpx31","cores":4,"memory":8,"disk":160}}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/servers/42":
			_, _ = w.Write([]byte(`{"server":{"id":42,"name":"beam-workspace-cpu-pool-machine-1","status":"running","location":{"name":"ash"},"server_type":{"id":45,"name":"cpx31","cores":4,"memory":8,"disk":160}}}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/servers/42":
			sawDelete = true
			_, _ = w.Write([]byte(`{"action":{"id":1,"status":"running","command":"delete_server"}}`))
		default:
			t.Fatalf("unexpected Hetzner request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewHetzner(HetznerConfig{
		APIToken:        "hetzner-token",
		BaseURL:         server.URL,
		Image:           "ubuntu-24.04",
		SSHKeysByRegion: map[string][]string{"ash": {"beam-key"}},
		PrivateNetwork: HetznerPrivateNetworkConfig{
			Name: "beam-workers",
		},
		Now: func() time.Time { return now },
	})

	reservation, err := client.CreateReservation(context.Background(), ReservationRequest{
		PoolName:         "cpu-pool",
		MachineID:        "machine-1",
		Name:             "beam-workspace-cpu-pool-machine-1",
		Selector:         "cpu-pool",
		Offer:            Offer{ID: "cpx31", Provider: "hetzner", InstanceType: "cpx31", Region: "ash", MachineCount: 1, CPUMillicores: 4000, MemoryMB: 8192, StorageMB: 163840, HourlyCostMicros: DollarsToMicros(0.0312), Labels: map[string]string{"network_zone": "us-east"}},
		TTL:              2 * time.Hour,
		Source:           SourceCLIReservation,
		BootstrapCommand: "beam-agent join --token token",
	})

	require.NoError(t, err)
	require.Equal(t, "42", reservation.ID)
	require.Equal(t, ReservationPending, reservation.Status)
	require.Equal(t, uint32(1), reservation.MachineCount)
	require.Equal(t, int64(4000), reservation.CPUMillicores)
	require.Equal(t, int64(8192), reservation.MemoryMB)
	require.Equal(t, int64(163840), reservation.StorageMB)
	require.Equal(t, "cpx31", createBody["server_type"])
	require.Equal(t, "ubuntu-24.04", createBody["image"])
	require.Equal(t, "ash", createBody["location"])
	require.Equal(t, "beam-workspace-cpu-pool-machine-1", createBody["name"])
	require.Equal(t, true, createBody["start_after_create"])
	require.Equal(t, []any{float64(456)}, createBody["networks"])
	require.Equal(t, []any{"beam-key"}, createBody["ssh_keys"])
	require.Contains(t, createBody["user_data"], "beam-agent join --token token")

	current, err := client.GetReservation(context.Background(), "42")
	require.NoError(t, err)
	require.Equal(t, ReservationActive, current.Status)
	require.Equal(t, "ash", current.Region)
	require.Equal(t, "cpx31", current.OfferID)

	require.NoError(t, client.DeleteReservation(context.Background(), "42"))
	require.True(t, sawDelete)
}

func TestHetznerCreateReservationRequiresPrivateNetworkSubnetInNetworkZone(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer hetzner-token", r.Header.Get("Authorization"))
		switch r.URL.Path {
		case "/locations":
			_, _ = w.Write([]byte(`{
				"locations": [{"id":1,"name":"ash","description":"Ashburn","network_zone":"us-east"}],
				"meta":{"pagination":{"page":1,"per_page":50,"previous_page":null,"next_page":null,"last_page":1,"total_entries":1}}
			}`))
		case "/networks":
			_, _ = w.Write([]byte(`{
				"networks": [{"id":456,"name":"beam-workers","subnets":[{"type":"cloud","ip_range":"10.43.0.0/24","network_zone":"eu-central"}]}],
				"meta":{"pagination":{"page":1,"per_page":50,"previous_page":null,"next_page":null,"last_page":1,"total_entries":1}}
			}`))
		default:
			t.Fatalf("unexpected Hetzner request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewHetzner(HetznerConfig{
		APIToken:       "hetzner-token",
		BaseURL:        server.URL,
		PrivateNetwork: HetznerPrivateNetworkConfig{Name: "beam-workers"},
	})

	_, err := client.CreateReservation(context.Background(), ReservationRequest{
		PoolName:  "cpu-pool",
		MachineID: "machine-1",
		Name:      "beam-workspace-cpu-pool-machine-1",
		Selector:  "cpu-pool",
		Offer:     Offer{ID: "cpx31", Provider: "hetzner", InstanceType: "cpx31", Region: "ash", MachineCount: 1, HourlyCostMicros: DollarsToMicros(0.0312)},
		TTL:       2 * time.Hour,
		Source:    SourceCLIReservation,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no cloud subnet in network zone")
}

func TestHetznerReservationStatusMapping(t *testing.T) {
	require.Equal(t, ReservationPending, hetznerReservationStatus("initializing"))
	require.Equal(t, ReservationActive, hetznerReservationStatus("running"))
	require.Equal(t, ReservationDeleted, hetznerReservationStatus("deleting"))
	require.Equal(t, ReservationFailed, hetznerReservationStatus("off"))
}
