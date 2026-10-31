package common

// RestrictedEgressNetworks are never direct tenant destinations.
func RestrictedEgressNetworks() []string {
	return []string{"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12", "192.168.0.0/16", "198.18.0.0/15", "224.0.0.0/4", "240.0.0.0/4", "::/128", "::1/128", "fc00::/7", "fe80::/10", "ff00::/8", "64:ff9b::/96", "64:ff9b:1::/48"}
}
