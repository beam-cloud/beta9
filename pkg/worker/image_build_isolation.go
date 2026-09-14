package worker

// Tenant RUN commands must never inherit the privileged worker's namespaces.
// Explicit flags override both inherited environment and containers.conf.
func buildahIsolationArgs() []string {
	return []string{"--isolation=oci", "--pid=private", "--ipc=private", "--network=private"}
}
