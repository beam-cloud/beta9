package providers

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// The real create endpoint rejects an omitted ssh_key_ids with HTTP 400.
// Capture the HTTP-decoded body, including key presence, rather than just
// checking a Go field whose nil value can disappear during marshaling.
func TestVerdaSendsExplicitNullWithoutSSHKeys(t *testing.T) {
	for name, keys := range map[string][]string{"unset": nil, "empty list": {}} {
		t.Run(name, func(t *testing.T) {
			api := newVerdaAPI(t)
			client := api.client(testOwner, func(config *types.VerdaProviderConfig) { config.SSHKeyIDs = keys })
			_, err := client.Create(context.Background(), CreateRequest{
				Offer: Offer{ID: "1V100.4V", Region: "FIN-01", Spot: true}, Name: "beam-verda-no-ssh-test",
			})
			require.NoError(t, err)
			body := api.lastCreate()
			require.Contains(t, body, "ssh_key_ids", "omitting the key reproduces the production validation failure")
			require.Nil(t, body["ssh_key_ids"], "no configured keys must encode JSON null, rather than an empty UUID array")
		})
	}
}

func TestVerdaPreservesConfiguredSSHKeyIDs(t *testing.T) {
	api := newVerdaAPI(t)
	keys := []string{"1d57010e-7d75-488e-9e91-711565030d58", "dd50b622-1c36-48c2-a485-f4d4937a3ea1"}
	client := api.client(testOwner, func(config *types.VerdaProviderConfig) { config.SSHKeyIDs = keys })
	_, err := client.Create(context.Background(), CreateRequest{
		Offer: Offer{ID: "1V100.4V", Region: "FIN-01", Spot: true}, Name: "beam-verda-ssh-test",
	})
	require.NoError(t, err)
	require.Equal(t, []any{keys[0], keys[1]}, api.lastCreate()["ssh_key_ids"])
}
