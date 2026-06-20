package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	rendezvous "github.com/beam-cloud/rendezvous"
)

type host struct {
	HostID         string `json:"hostId"`
	RegistrationID string `json:"registrationId,omitempty"`
	NodeID         string `json:"nodeId,omitempty"`
	NodeName       string `json:"nodeName,omitempty"`
	CachePathID    string `json:"cachePathId,omitempty"`
	Addr           string `json:"addr,omitempty"`
	PrivateAddr    string `json:"privateAddr,omitempty"`
	Pod            string `json:"pod,omitempty"`
}

func (h *host) Bytes() []byte {
	return []byte(h.HostID)
}

type route struct {
	Key   string  `json:"key"`
	Hosts []*host `json:"hosts"`
}

func main() {
	hostsJSON := flag.String("hosts-json", "", "JSON array of cache hosts")
	keysCSV := flag.String("keys", "", "comma-separated routing keys")
	n := flag.Int("n", 1, "number of HRW hosts per key")
	flag.Parse()

	if *hostsJSON == "" || *keysCSV == "" {
		fmt.Fprintln(os.Stderr, "hosts-json and keys are required")
		os.Exit(2)
	}

	var hosts []*host
	if err := json.Unmarshal([]byte(*hostsJSON), &hosts); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	hash := rendezvous.New[*host]()
	for _, host := range hosts {
		if host != nil && host.HostID != "" {
			hash.Add(host)
		}
	}

	var routes []route
	for _, key := range strings.Split(*keysCSV, ",") {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		routes = append(routes, route{Key: key, Hosts: hash.GetN(*n, key)})
	}

	_ = json.NewEncoder(os.Stdout).Encode(routes)
}
