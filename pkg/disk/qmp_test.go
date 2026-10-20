package disk

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQMPMatchesCommandReplies(t *testing.T) {
	for _, stale := range []struct{ name, reply string }{
		{"success", `{"return":{},"id":"previous-monitor-command"}`},
		{"error", `{"error":{"class":"GenericError","desc":"previous command failed"},"id":"previous-monitor-command"}`},
	} {
		t.Run(stale.name, func(t *testing.T) {
			conn, server := net.Pipe()
			t.Cleanup(func() { conn.Close(); server.Close() })
			client := &qmpClient{conn: conn, dec: json.NewDecoder(conn)}
			go func() {
				var request map[string]json.RawMessage
				if json.NewDecoder(server).Decode(&request) != nil {
					return
				}
				enc := json.NewEncoder(server)
				if enc.Encode(json.RawMessage(`{"event":"BLOCK_JOB_COMPLETED"}`)) != nil || enc.Encode(json.RawMessage(stale.reply)) != nil {
					return
				}
				enc.Encode(map[string]any{"return": []any{}, "id": request["id"]})
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			raw, err := client.execute(ctx, "query-named-block-nodes", nil)
			require.NoError(t, err)
			require.JSONEq(t, `[]`, string(raw), "a delayed transaction reply must not stand in for the graph query")
		})
	}
}

func TestQMPCommandIDsSurviveMonitorReconnect(t *testing.T) {
	ids := make(chan string, 2)
	for range 2 {
		conn, server := net.Pipe()
		t.Cleanup(func() { conn.Close(); server.Close() })
		go func() {
			var request struct {
				ID string `json:"id"`
			}
			if json.NewDecoder(server).Decode(&request) != nil {
				return
			}
			ids <- request.ID
			json.NewEncoder(server).Encode(map[string]any{"return": map[string]any{}, "id": request.ID})
		}()
		client := &qmpClient{conn: conn, dec: json.NewDecoder(conn)}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := client.execute(ctx, "qmp_capabilities", nil)
		cancel()
		require.NoError(t, err)
	}
	first, second := <-ids, <-ids
	require.NotEmpty(t, first)
	require.NotEqual(t, first, second, "a new monitor must not reuse the timed-out command's identity")
}
