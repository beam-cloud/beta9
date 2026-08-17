package disk

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"
)

// qmpClient is a minimal QMP client for qemu-storage-daemon. It supports the
// handshake plus the three commands the disk engine needs: transactional
// snapshots, graph queries (recovery), and quit.
type qmpClient struct {
	conn net.Conn
	dec  *json.Decoder
}

const qmpDialTimeout = 10 * time.Second

func dialQMP(ctx context.Context, socketPath string) (*qmpClient, error) {
	dialer := net.Dialer{Timeout: qmpDialTimeout}
	conn, err := dialer.DialContext(ctx, "unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("dial qmp socket %s: %w", socketPath, err)
	}
	client := &qmpClient{conn: conn, dec: json.NewDecoder(conn)}

	// The server speaks first with a greeting banner.
	var greeting struct {
		QMP *json.RawMessage `json:"QMP"`
	}
	if err := client.decode(ctx, &greeting); err != nil {
		conn.Close()
		return nil, fmt.Errorf("read qmp greeting: %w", err)
	}
	if greeting.QMP == nil {
		conn.Close()
		return nil, fmt.Errorf("unexpected qmp greeting")
	}
	if _, err := client.execute(ctx, "qmp_capabilities", nil); err != nil {
		conn.Close()
		return nil, fmt.Errorf("negotiate qmp capabilities: %w", err)
	}
	return client, nil
}

func (c *qmpClient) Close() error {
	return c.conn.Close()
}

func (c *qmpClient) decode(ctx context.Context, v any) error {
	deadline := time.Now().Add(qmpDialTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = c.conn.SetReadDeadline(deadline)
	return c.dec.Decode(v)
}

type qmpError struct {
	Class string `json:"class"`
	Desc  string `json:"desc"`
}

func (c *qmpClient) execute(ctx context.Context, command string, arguments any) (json.RawMessage, error) {
	request := map[string]any{"execute": command}
	if arguments != nil {
		request["arguments"] = arguments
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(qmpDialTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = c.conn.SetWriteDeadline(deadline)
	if _, err := c.conn.Write(payload); err != nil {
		return nil, fmt.Errorf("write qmp command %s: %w", command, err)
	}

	// Skip asynchronous events until we see a return or error for our command.
	for {
		var response struct {
			Return json.RawMessage  `json:"return"`
			Error  *qmpError        `json:"error"`
			Event  *json.RawMessage `json:"event"`
		}
		if err := c.decode(ctx, &response); err != nil {
			return nil, fmt.Errorf("read qmp response for %s: %w", command, err)
		}
		if response.Event != nil {
			continue
		}
		if response.Error != nil {
			return nil, fmt.Errorf("qmp %s: %s: %s", command, response.Error.Class, response.Error.Desc)
		}
		return response.Return, nil
	}
}

// snapshotSync pivots the active qcow2 node onto a pre-created empty overlay.
// The overlay file must already exist (mode=existing); on success the old
// active layer is sealed and all writes land in the new overlay.
func (c *qmpClient) snapshotSync(ctx context.Context, nodeName, overlayPath, overlayNodeName string) error {
	_, err := c.execute(ctx, "transaction", map[string]any{
		"actions": []map[string]any{{
			"type": "blockdev-snapshot-sync",
			"data": map[string]any{
				"node-name":          nodeName,
				"snapshot-file":      overlayPath,
				"snapshot-node-name": overlayNodeName,
				"format":             "qcow2",
				"mode":               "existing",
			},
		}},
	})
	return err
}

// namedBlockNodes returns the filenames of all named nodes in the daemon's
// graph, used during recovery to determine whether a pivot committed.
func (c *qmpClient) namedBlockNodes(ctx context.Context) (map[string]string, error) {
	raw, err := c.execute(ctx, "query-named-block-nodes", nil)
	if err != nil {
		return nil, err
	}
	var nodes []struct {
		NodeName string `json:"node-name"`
		File     string `json:"file"`
	}
	if err := json.Unmarshal(raw, &nodes); err != nil {
		return nil, err
	}
	result := make(map[string]string, len(nodes))
	for _, node := range nodes {
		result[node.NodeName] = node.File
	}
	return result, nil
}

// writtenBytes returns the bytes written to a node since it was created.
// A freshly pivoted head starts at zero, which makes this an exact "anything
// changed since the last snapshot" signal.
func (c *qmpClient) writtenBytes(ctx context.Context, nodeName string) (int64, error) {
	raw, err := c.execute(ctx, "query-blockstats", map[string]any{"query-nodes": true})
	if err != nil {
		return 0, err
	}
	var stats []struct {
		NodeName string `json:"node-name"`
		Stats    struct {
			WrBytes int64 `json:"wr_bytes"`
		} `json:"stats"`
	}
	if err := json.Unmarshal(raw, &stats); err != nil {
		return 0, err
	}
	for _, node := range stats {
		if node.NodeName == nodeName {
			return node.Stats.WrBytes, nil
		}
	}
	return 0, fmt.Errorf("node %s not found in block stats", nodeName)
}

func (c *qmpClient) quit(ctx context.Context) error {
	_, err := c.execute(ctx, "quit", nil)
	return err
}
