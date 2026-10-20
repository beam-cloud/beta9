package disk

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQMPMatchesCommandReplies(t *testing.T) {
	for _, stale := range []struct{ name, reply string }{
		{"success", `{"return":{},"id":%s}`},
		{"error", `{"error":{"class":"GenericError","desc":"previous command failed"},"id":%s}`},
	} {
		t.Run(stale.name, func(t *testing.T) {
			previousID := json.RawMessage(`"unrelated-command"`)
			ids := make(chan json.RawMessage, 1)
			// Reconnect once, then replay the previous monitor's reply.
			for range 2 {
				conn, server := net.Pipe()
				t.Cleanup(func() { conn.Close(); server.Close() })
				client := &qmpClient{conn: conn, dec: json.NewDecoder(conn)}
				lateReply := json.RawMessage(fmt.Sprintf(stale.reply, previousID))
				go func() {
					var request map[string]json.RawMessage
					if json.NewDecoder(server).Decode(&request) != nil {
						return
					}
					enc := json.NewEncoder(server)
					if enc.Encode(json.RawMessage(`{"event":"BLOCK_JOB_COMPLETED"}`)) != nil || enc.Encode(lateReply) != nil {
						return
					}
					ids <- request["id"]
					enc.Encode(map[string]any{"return": []any{}, "id": request["id"]})
				}()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				raw, err := client.execute(ctx, "query-named-block-nodes", nil)
				cancel()
				require.NoError(t, err)
				require.JSONEq(t, `[]`, string(raw), "a delayed reply must not stand in for the graph query")
				previousID = <-ids
			}
		})
	}
}
