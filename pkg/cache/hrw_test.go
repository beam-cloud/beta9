package cache

import (
	"fmt"
	"reflect"
	"testing"

	rendezvous "github.com/beam-cloud/rendezvous"
)

var sampleKeys = []string{
	"352DAB08-C1FD-4462-B573-7640B730B721",
	"382080D3-B847-4BB5-AEA8-644C3E56F4E1",
	"2B340C12-7958-4DBE-952C-67496E15D0C8",
	"BE05F82B-902E-4868-8CC9-EE50A6C64636",
	"C7ECC571-E924-4523-A313-951DFD5D8073",
}

type getTestcase struct {
	key          string
	expectedHost *Host
}

func TestHashGet(t *testing.T) {
	hostMap := NewHostMap(GlobalConfig{}, nil)

	hostMap.Set(&Host{HostId: "a"})
	hostMap.Set(&Host{HostId: "b"})
	hostMap.Set(&Host{HostId: "c"})
	hostMap.Set(&Host{HostId: "d"})
	hostMap.Set(&Host{HostId: "e"})

	hash := rendezvous.New[*Host]()
	hash.Add(hostMap.GetAll()...)

	gotHost, _ := hash.Get("foo")
	if gotHost != nil && gotHost.HostId != "e" {
		t.Errorf("got: %#v, expected: %#v", gotHost, &Host{HostId: "e"})
	}

	hash.Add(hostMap.GetAll()...)

	testcases := []getTestcase{
		{"", &Host{HostId: "d"}},
		{"foo", &Host{HostId: "e"}},
		{"bar", &Host{HostId: "c"}},
	}

	for _, testcase := range testcases {
		gotHost, _ := hash.Get(testcase.key)
		if gotHost.HostId != testcase.expectedHost.HostId {
			t.Errorf("got: %#v, expected: %#v", gotHost, testcase.expectedHost)
		}
	}
}

type getNTestcase struct {
	count         int
	key           string
	expectedHosts []*Host
}

func Test_Hash_GetN(t *testing.T) {
	hostMap := NewHostMap(GlobalConfig{}, nil)

	hash := rendezvous.New[*Host]()

	hostMap.Set(&Host{HostId: "a"})
	hostMap.Set(&Host{HostId: "b"})
	hostMap.Set(&Host{HostId: "c"})
	hostMap.Set(&Host{HostId: "d"})
	hostMap.Set(&Host{HostId: "e"})

	hash.Add(hostMap.GetAll()...)

	testcases := []getNTestcase{
		{1, "foo", []*Host{{HostId: "e"}}},
		{2, "bar", []*Host{{HostId: "c"}, {HostId: "e"}}},
		{3, "baz", []*Host{{HostId: "d"}, {HostId: "a"}, {HostId: "b"}}},
		{2, "biz", []*Host{{HostId: "b"}, {HostId: "a"}}},
		{0, "boz", []*Host{}},
		{100, "floo", []*Host{{HostId: "d"}, {HostId: "a"}, {HostId: "b"}, {HostId: "c"}, {HostId: "e"}}},
	}

	for _, testcase := range testcases {
		gotHosts := hash.GetN(testcase.count, testcase.key)
		if !reflect.DeepEqual(gotHosts, testcase.expectedHosts) {
			t.Errorf("got: %#v, expected: %#v", gotHosts, testcase.expectedHosts)
		}
	}
}

func TestHashRemove(t *testing.T) {
	hostMap := NewHostMap(GlobalConfig{}, nil)

	hostMap.Set(&Host{HostId: "a"})
	hostMap.Set(&Host{HostId: "b"})
	hostMap.Set(&Host{HostId: "c"})
	hostMap.Set(&Host{HostId: "d"})
	hostMap.Set(&Host{HostId: "e"})

	hash := rendezvous.New[*Host]()
	hash.Add(hostMap.GetAll()...)

	var keyForB string
	for i := 0; i < 10000; i++ {
		randomKey := fmt.Sprintf("key-%d", i)
		if gotHost, _ := hash.Get(randomKey); gotHost.HostId == "b" {
			keyForB = randomKey
			break
		}
	}

	if keyForB == "" {
		t.Fatalf("Failed to find a key that maps to 'b'")
	}

	hash.Remove(hostMap.Get("b"))

	// Check if the key now maps to a different node
	newNode, _ := hash.Get(keyForB)
	if newNode.HostId == "b" {
		t.Errorf("Key %s still maps to removed node 'b'", keyForB)
	}

	if newNode == nil {
		t.Errorf("Key %s does not map to any node after removing 'b'", keyForB)
	}
}

func TestHashPlacementSurvivesRegistrationEndpointChurn(t *testing.T) {
	hostMap := NewHostMap(GlobalConfig{}, nil)
	hostMap.Set(&Host{
		HostId:         "cache-host-default-node-a-path",
		RegistrationID: "worker-a-1",
		NodeID:         "node-a",
		CachePathID:    "path",
		PrivateAddr:    "10.0.0.1:2049",
	})
	hostMap.Set(&Host{
		HostId:         "cache-host-default-node-b-path",
		RegistrationID: "worker-b-1",
		NodeID:         "node-b",
		CachePathID:    "path",
		PrivateAddr:    "10.0.0.2:2049",
	})

	var keyForNodeA string
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		hash := rendezvous.New[*Host]()
		hash.Add(hostMap.GetAll()...)
		host, _ := hash.Get(key)
		if host != nil && host.HostId == "cache-host-default-node-a-path" {
			keyForNodeA = key
			break
		}
	}
	if keyForNodeA == "" {
		t.Fatal("failed to find a key routed to node-a")
	}

	hashBefore := rendezvous.New[*Host]()
	hashBefore.Add(hostMap.GetAll()...)
	before, _ := hashBefore.Get(keyForNodeA)
	if before == nil {
		t.Fatal("expected key to route before endpoint churn")
	}

	hostMap.Set(&Host{
		HostId:         "cache-host-default-node-a-path",
		RegistrationID: "worker-a-2",
		NodeID:         "node-a",
		CachePathID:    "path",
		PrivateAddr:    "10.0.0.3:2049",
	})

	hashAfter := rendezvous.New[*Host]()
	hashAfter.Add(hostMap.GetAll()...)
	after, _ := hashAfter.Get(keyForNodeA)
	if after == nil {
		t.Fatal("expected key to route after endpoint churn")
	}

	if before.HostId != after.HostId {
		t.Fatalf("logical cache host changed across endpoint churn: before=%s after=%s", before.HostId, after.HostId)
	}
	if after.RegistrationID != "worker-a-2" {
		t.Fatalf("expected active endpoint metadata to update, got registration %q", after.RegistrationID)
	}
}
