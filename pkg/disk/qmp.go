package disk

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

// qmpClient is a minimal QMP client for qemu-storage-daemon. It supports the
// handshake plus the commands the disk engine needs: transactional snapshots,
// commit jobs (compaction), graph queries (recovery), and quit.
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
	if _, err := client.execute(ctx, types.QMPCommandCapabilities, nil); err != nil {
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

// addOverlay opens a pre-created empty overlay under explicit node names,
// with no backing attached yet. Naming the file child ourselves (instead of
// letting blockdev-snapshot-sync open the file anonymously) keeps the head's
// file node addressable for write statistics after every pivot.
func (c *qmpClient) addOverlay(ctx context.Context, fmtNode, fileNode, path string) error {
	_, err := c.execute(ctx, types.QMPCommandBlockdevAdd, map[string]any{
		"driver":    "qcow2",
		"node-name": fmtNode,
		"file": map[string]any{
			"driver": "file", "filename": path, "node-name": fileNode,
		},
		"backing": nil,
	})
	return err
}

// removeNode drops a node added with addOverlay that never got wired into the
// active chain. Implicitly created children (the file node) go with it.
func (c *qmpClient) removeNode(ctx context.Context, nodeName string) error {
	_, err := c.execute(ctx, types.QMPCommandBlockdevDel, map[string]any{"node-name": nodeName})
	return err
}

// pivot re-parents the active qcow2 node under the already-added overlay. On
// success the old active layer is sealed and all writes land in the overlay.
func (c *qmpClient) pivot(ctx context.Context, nodeName, overlayNode string) error {
	_, err := c.execute(ctx, types.QMPCommandTransaction, map[string]any{
		"actions": []map[string]any{{
			"type": types.QMPCommandBlockdevSnapshot,
			"data": map[string]any{"node": nodeName, "overlay": overlayNode},
		}},
	})
	return err
}

// commitChain runs an intermediate block-commit merging every layer between
// base and top (inclusive, identified by filename) down into base, and polls
// the job to its conclusion. The head keeps serving I/O; on completion the
// daemon drops the merged layers and re-parents the layer above top onto base.
func (c *qmpClient) commitChain(ctx context.Context, device, topPath, basePath string) error {
	jobID := fmt.Sprintf("commit-%d", time.Now().UnixNano())
	if _, err := c.execute(ctx, types.QMPCommandBlockCommit, map[string]any{
		"job-id":       jobID,
		"device":       device,
		"top":          topPath,
		"base":         basePath,
		"auto-dismiss": false, // hold the concluded job so its error is readable
	}); err != nil {
		return err
	}

	type jobInfo struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	for {
		raw, err := c.execute(ctx, types.QMPCommandQueryJobs, nil)
		if err != nil {
			return err
		}
		var jobs []jobInfo
		if err := json.Unmarshal(raw, &jobs); err != nil {
			return err
		}
		var job *jobInfo
		for i := range jobs {
			if jobs[i].ID == jobID {
				job = &jobs[i]
				break
			}
		}
		if job == nil {
			return fmt.Errorf("commit job %s disappeared before concluding", jobID)
		}
		if job.Status == "concluded" {
			_, _ = c.execute(ctx, types.QMPCommandJobDismiss, map[string]any{"id": jobID})
			if job.Error != "" {
				return fmt.Errorf("commit job: %s", job.Error)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(maxPollInterval):
		}
	}
}

// qmpNode is one entry of query-named-block-nodes. BackingFileDepth counts
// runtime backing links: an overlay added with backing=null reports zero
// until a pivot wires it, which is what recovery uses to tell a committed
// pivot from a dangling add.
type qmpNode struct {
	BackingFileDepth int      `json:"backing_file_depth"`
	Image            qmpImage `json:"image"`
}

// qmpImage mirrors the recursive ImageInfo the daemon reports per node.
type qmpImage struct {
	Filename     string    `json:"filename"`
	BackingImage *qmpImage `json:"backing-image"`
}

// namedBlockNodes returns all named nodes in the daemon's graph, used during
// recovery to determine whether a pivot committed.
func (c *qmpClient) namedBlockNodes(ctx context.Context) (map[string]qmpNode, error) {
	raw, err := c.execute(ctx, types.QMPCommandQueryNamedBlockNodes, nil)
	if err != nil {
		return nil, err
	}
	var nodes []struct {
		NodeName string `json:"node-name"`
		qmpNode
	}
	if err := json.Unmarshal(raw, &nodes); err != nil {
		return nil, err
	}
	result := make(map[string]qmpNode, len(nodes))
	for _, node := range nodes {
		result[node.NodeName] = node.qmpNode
	}
	return result, nil
}

// backingFilenames returns the image filenames in nodeName's live backing
// chain, the node's own file included.
func (c *qmpClient) backingFilenames(ctx context.Context, nodeName string) (map[string]bool, error) {
	nodes, err := c.namedBlockNodes(ctx)
	if err != nil {
		return nil, err
	}
	node, ok := nodes[nodeName]
	if !ok {
		return nil, fmt.Errorf("node %s not found in block graph", nodeName)
	}
	filenames := make(map[string]bool)
	for image := &node.Image; image != nil; image = image.BackingImage {
		filenames[image.Filename] = true
	}
	return filenames, nil
}

// writtenBytes reports whether anything was written to a node since the
// daemon opened it, via wr_highest_offset. wr_bytes cannot be used here: the
// daemon accounts it on the NBD export's block backend, so graph nodes always
// report zero. wr_highest_offset is tracked on the node itself, and a freshly
// created qcow2 overlay starts at zero, which makes this an exact "anything
// changed since the last snapshot" signal for the head's file node.
func (c *qmpClient) writtenBytes(ctx context.Context, nodeName string) (int64, error) {
	raw, err := c.execute(ctx, types.QMPCommandQueryBlockstats, map[string]any{"query-nodes": true})
	if err != nil {
		return 0, err
	}
	var stats []struct {
		NodeName string `json:"node-name"`
		Stats    struct {
			WrHighestOffset int64 `json:"wr_highest_offset"`
		} `json:"stats"`
	}
	if err := json.Unmarshal(raw, &stats); err != nil {
		return 0, err
	}
	for _, node := range stats {
		if node.NodeName == nodeName {
			return node.Stats.WrHighestOffset, nil
		}
	}
	return 0, fmt.Errorf("node %s not found in block stats", nodeName)
}

func (c *qmpClient) quit(ctx context.Context) error {
	_, err := c.execute(ctx, types.QMPCommandQuit, nil)
	return err
}
