package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"
)

var ErrStateVolumePivotIndeterminate = errors.New("state volume pivot result is indeterminate")
var ErrStateVolumeCompactionIndeterminate = errors.New("state volume compaction result is indeterminate")

type StateVolumeSnapshotAction struct {
	CurrentNode string
	NewNode     string
	NewPath     string
	Mode        string
}

type StateVolumeQMP interface {
	ProbeSnapshotSupport(ctx context.Context) error
	TransactionSnapshot(ctx context.Context, actions []StateVolumeSnapshotAction) error
	QueryNodeNames(ctx context.Context) (map[string]struct{}, error)
	QuerySnapshotGraph(ctx context.Context) (StateVolumeQMPSnapshotGraph, error)
	StartBlockStream(ctx context.Context, nodeName, jobID string) error
	QueryBlockJob(ctx context.Context, jobID string) (*StateVolumeQMPBlockJob, error)
	FinalizeBlockJob(ctx context.Context, jobID string) error
	DismissBlockJob(ctx context.Context, jobID string) error
	CancelBlockJob(ctx context.Context, jobID string) error
	Quit(ctx context.Context) error
	Close() error
}

// StateVolumeQMPSnapshotGraph contains only the graph relationships needed to
// reconcile a lost snapshot transaction reply. In QEMU 6.2 query-blockstats
// reports a node's immediate file child as "parent"; query-block-exports binds
// the stable NBD export ID to the raw wrapper node.
type StateVolumeQMPSnapshotGraph struct {
	Nodes   map[string]StateVolumeQMPNode
	Exports map[string]StateVolumeQMPExport
}

type StateVolumeQMPNode struct {
	Name             string
	ChildNode        string
	Driver           string
	FilePath         string
	BackingFilePath  string
	BackingFileDepth int
}

type StateVolumeQMPBlockJob struct {
	ID     string
	Status string
	Error  string
}

type StateVolumeQMPExport struct {
	ID           string
	NodeName     string
	ShuttingDown bool
}

type StateVolumeQMPDialer interface {
	Dial(ctx context.Context, socketPath string) (StateVolumeQMP, error)
}

type UnixStateVolumeQMPDialer struct{}

func (UnixStateVolumeQMPDialer) Dial(ctx context.Context, socketPath string) (StateVolumeQMP, error) {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("dial QSD QMP socket %s: %w", socketPath, err)
	}
	client := &stateVolumeQMPClient{
		conn: conn,
		enc:  json.NewEncoder(conn),
		dec:  json.NewDecoder(conn),
	}
	if err := client.handshake(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return client, nil
}

type stateVolumeQMPClient struct {
	mu     sync.Mutex
	conn   net.Conn
	enc    *json.Encoder
	dec    *json.Decoder
	nextID uint64
}

type qmpResponse struct {
	QMP    json.RawMessage `json:"QMP"`
	Return json.RawMessage `json:"return"`
	Error  *struct {
		Class string `json:"class"`
		Desc  string `json:"desc"`
	} `json:"error"`
	Event string          `json:"event"`
	ID    json.RawMessage `json:"id"`
}

type qmpTransportError struct{ err error }

func (e *qmpTransportError) Error() string { return e.err.Error() }
func (e *qmpTransportError) Unwrap() error { return e.err }

func (c *stateVolumeQMPClient) handshake(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.setDeadline(ctx); err != nil {
		return err
	}
	defer c.clearDeadline()

	var greeting qmpResponse
	if err := c.dec.Decode(&greeting); err != nil {
		return fmt.Errorf("read QMP greeting: %w", err)
	}
	if len(greeting.QMP) == 0 {
		return fmt.Errorf("QMP server did not send a greeting")
	}
	_, err := c.executeLocked("qmp_capabilities", nil)
	if err != nil {
		return fmt.Errorf("enable QMP capabilities: %w", err)
	}
	return nil
}

func (c *stateVolumeQMPClient) setDeadline(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		return fmt.Errorf("set QMP deadline: %w", err)
	}
	return nil
}

func (c *stateVolumeQMPClient) clearDeadline() {
	_ = c.conn.SetDeadline(time.Time{})
}

func (c *stateVolumeQMPClient) execute(ctx context.Context, command string, arguments any) (json.RawMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.setDeadline(ctx); err != nil {
		return nil, err
	}
	defer c.clearDeadline()
	return c.executeLocked(command, arguments)
}

func (c *stateVolumeQMPClient) executeLocked(command string, arguments any) (json.RawMessage, error) {
	c.nextID++
	id := c.nextID
	request := struct {
		Execute   string `json:"execute"`
		Arguments any    `json:"arguments,omitempty"`
		ID        uint64 `json:"id"`
	}{Execute: command, Arguments: arguments, ID: id}
	if err := c.enc.Encode(request); err != nil {
		// Encoder.Write may have delivered any prefix, including the complete
		// command, before returning an error. Callers of a mutating command must
		// therefore treat every write failure as an unknown execution outcome.
		return nil, &qmpTransportError{err: fmt.Errorf("write QMP command %s: %w", command, err)}
	}
	for {
		var response qmpResponse
		if err := c.dec.Decode(&response); err != nil {
			return nil, &qmpTransportError{err: fmt.Errorf("read QMP command %s response: %w", command, err)}
		}
		if response.Event != "" {
			continue
		}
		var responseID uint64
		if len(response.ID) == 0 || json.Unmarshal(response.ID, &responseID) != nil || responseID != id {
			continue
		}
		if response.Error != nil {
			return nil, fmt.Errorf("QMP command %s failed (%s): %s", command, response.Error.Class, response.Error.Desc)
		}
		return response.Return, nil
	}
}

func (c *stateVolumeQMPClient) ProbeSnapshotSupport(ctx context.Context) error {
	raw, err := c.execute(ctx, "query-commands", nil)
	if err != nil {
		return err
	}
	var commands []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw, &commands); err != nil {
		return fmt.Errorf("decode QMP command list: %w", err)
	}
	present := make(map[string]bool, len(commands))
	for _, command := range commands {
		present[command.Name] = true
	}
	for _, required := range []string{
		"transaction", "blockdev-snapshot-sync", "query-named-block-nodes", "query-blockstats", "query-block-exports",
		"block-stream", "query-block-jobs", "job-finalize", "job-dismiss", "block-job-cancel",
	} {
		if !present[required] {
			return fmt.Errorf("QSD does not expose required QMP command %q", required)
		}
	}
	return nil
}

func (c *stateVolumeQMPClient) TransactionSnapshot(ctx context.Context, actions []StateVolumeSnapshotAction) error {
	if len(actions) == 0 {
		return fmt.Errorf("QMP snapshot transaction has no actions")
	}
	seenNodes := make(map[string]struct{}, len(actions)*2)
	seenPaths := make(map[string]struct{}, len(actions))
	transactionActions := make([]map[string]any, 0, len(actions))
	for _, action := range actions {
		if action.CurrentNode == "" || action.NewNode == "" || action.NewPath == "" {
			return fmt.Errorf("QMP snapshot action contains an empty node or path")
		}
		if _, exists := seenNodes[action.CurrentNode]; exists {
			return fmt.Errorf("duplicate QMP current node %q", action.CurrentNode)
		}
		seenNodes[action.CurrentNode] = struct{}{}
		if _, exists := seenNodes[action.NewNode]; exists {
			return fmt.Errorf("duplicate QMP snapshot node %q", action.NewNode)
		}
		seenNodes[action.NewNode] = struct{}{}
		if _, exists := seenPaths[action.NewPath]; exists {
			return fmt.Errorf("duplicate QMP snapshot path %q", action.NewPath)
		}
		seenPaths[action.NewPath] = struct{}{}
		mode := action.Mode
		if mode == "" {
			mode = "existing"
		}
		if mode != "existing" {
			return fmt.Errorf("unsupported QMP snapshot mode %q", mode)
		}
		transactionActions = append(transactionActions, map[string]any{
			"type": "blockdev-snapshot-sync",
			"data": map[string]any{
				"node-name":          action.CurrentNode,
				"snapshot-file":      action.NewPath,
				"snapshot-node-name": action.NewNode,
				"format":             "qcow2",
				"mode":               mode,
			},
		})
	}
	// Keep the wire representation deterministic for logs and fake-QMP tests.
	sort.SliceStable(transactionActions, func(i, j int) bool {
		left := transactionActions[i]["data"].(map[string]any)["node-name"].(string)
		right := transactionActions[j]["data"].(map[string]any)["node-name"].(string)
		return left < right
	})
	if _, err := c.execute(ctx, "transaction", map[string]any{"actions": transactionActions}); err != nil {
		var transportErr *qmpTransportError
		if errors.As(err, &transportErr) {
			return fmt.Errorf("%w: %v", ErrStateVolumePivotIndeterminate, err)
		}
		return err
	}
	return nil
}

func (c *stateVolumeQMPClient) QueryNodeNames(ctx context.Context) (map[string]struct{}, error) {
	raw, err := c.execute(ctx, "query-named-block-nodes", nil)
	if err != nil {
		return nil, err
	}
	var nodes []struct {
		NodeName string `json:"node-name"`
	}
	if err := json.Unmarshal(raw, &nodes); err != nil {
		return nil, fmt.Errorf("decode QMP named block nodes: %w", err)
	}
	names := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		if node.NodeName != "" {
			names[node.NodeName] = struct{}{}
		}
	}
	return names, nil
}

func (c *stateVolumeQMPClient) QuerySnapshotGraph(ctx context.Context) (StateVolumeQMPSnapshotGraph, error) {
	statsRaw, err := c.execute(ctx, "query-blockstats", map[string]any{"query-nodes": true})
	if err != nil {
		return StateVolumeQMPSnapshotGraph{}, err
	}
	type blockStats struct {
		NodeName string      `json:"node-name"`
		Parent   *blockStats `json:"parent"`
	}
	var stats []blockStats
	if err := json.Unmarshal(statsRaw, &stats); err != nil {
		return StateVolumeQMPSnapshotGraph{}, fmt.Errorf("decode QMP block graph: %w", err)
	}
	graph := StateVolumeQMPSnapshotGraph{
		Nodes:   make(map[string]StateVolumeQMPNode, len(stats)),
		Exports: make(map[string]StateVolumeQMPExport),
	}
	for _, stat := range stats {
		if stat.NodeName == "" {
			continue
		}
		if _, exists := graph.Nodes[stat.NodeName]; exists {
			return StateVolumeQMPSnapshotGraph{}, fmt.Errorf("QMP block graph repeats node %q", stat.NodeName)
		}
		node := StateVolumeQMPNode{Name: stat.NodeName}
		if stat.Parent != nil {
			node.ChildNode = stat.Parent.NodeName
		}
		graph.Nodes[node.Name] = node
	}
	namedRaw, err := c.execute(ctx, "query-named-block-nodes", map[string]any{"flat": true})
	if err != nil {
		return StateVolumeQMPSnapshotGraph{}, err
	}
	var named []struct {
		NodeName         string `json:"node-name"`
		Driver           string `json:"drv"`
		File             string `json:"file"`
		BackingFile      string `json:"backing_file"`
		BackingFileDepth int    `json:"backing_file_depth"`
	}
	if err := json.Unmarshal(namedRaw, &named); err != nil {
		return StateVolumeQMPSnapshotGraph{}, fmt.Errorf("decode QMP named block graph: %w", err)
	}
	for _, info := range named {
		node, ok := graph.Nodes[info.NodeName]
		if !ok {
			return StateVolumeQMPSnapshotGraph{}, fmt.Errorf("QMP named node %q is missing from blockstats", info.NodeName)
		}
		node.Driver = info.Driver
		node.FilePath = info.File
		node.BackingFilePath = info.BackingFile
		node.BackingFileDepth = info.BackingFileDepth
		graph.Nodes[info.NodeName] = node
	}

	exportsRaw, err := c.execute(ctx, "query-block-exports", nil)
	if err != nil {
		return StateVolumeQMPSnapshotGraph{}, err
	}
	var exports []struct {
		ID           string `json:"id"`
		NodeName     string `json:"node-name"`
		ShuttingDown bool   `json:"shutting-down"`
	}
	if err := json.Unmarshal(exportsRaw, &exports); err != nil {
		return StateVolumeQMPSnapshotGraph{}, fmt.Errorf("decode QMP block exports: %w", err)
	}
	for _, export := range exports {
		if export.ID == "" || export.NodeName == "" {
			return StateVolumeQMPSnapshotGraph{}, fmt.Errorf("QMP block export has an empty ID or node")
		}
		if _, exists := graph.Exports[export.ID]; exists {
			return StateVolumeQMPSnapshotGraph{}, fmt.Errorf("QMP block graph repeats export %q", export.ID)
		}
		graph.Exports[export.ID] = StateVolumeQMPExport{
			ID: export.ID, NodeName: export.NodeName, ShuttingDown: export.ShuttingDown,
		}
	}
	return graph, nil
}

func (c *stateVolumeQMPClient) StartBlockStream(ctx context.Context, nodeName, jobID string) error {
	if nodeName == "" || jobID == "" {
		return fmt.Errorf("block-stream node and job ID are required")
	}
	_, err := c.execute(ctx, "block-stream", map[string]any{
		"device": nodeName, "job-id": jobID, "auto-finalize": false, "auto-dismiss": false,
	})
	if err != nil {
		var transportErr *qmpTransportError
		if errors.As(err, &transportErr) {
			return fmt.Errorf("%w: %v", ErrStateVolumeCompactionIndeterminate, err)
		}
	}
	return err
}

func (c *stateVolumeQMPClient) QueryBlockJob(ctx context.Context, jobID string) (*StateVolumeQMPBlockJob, error) {
	raw, err := c.execute(ctx, "query-block-jobs", nil)
	if err != nil {
		return nil, err
	}
	var jobs []struct {
		Device string `json:"device"`
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal(raw, &jobs); err != nil {
		return nil, fmt.Errorf("decode QMP block jobs: %w", err)
	}
	for _, job := range jobs {
		if job.Device == jobID {
			return &StateVolumeQMPBlockJob{ID: job.Device, Status: job.Status, Error: job.Error}, nil
		}
	}
	return nil, nil
}

func (c *stateVolumeQMPClient) FinalizeBlockJob(ctx context.Context, jobID string) error {
	_, err := c.execute(ctx, "job-finalize", map[string]any{"id": jobID})
	var transportErr *qmpTransportError
	if errors.As(err, &transportErr) {
		return fmt.Errorf("%w: %v", ErrStateVolumeCompactionIndeterminate, err)
	}
	return err
}

func (c *stateVolumeQMPClient) DismissBlockJob(ctx context.Context, jobID string) error {
	_, err := c.execute(ctx, "job-dismiss", map[string]any{"id": jobID})
	var transportErr *qmpTransportError
	if errors.As(err, &transportErr) {
		return fmt.Errorf("%w: %v", ErrStateVolumeCompactionIndeterminate, err)
	}
	return err
}

func (c *stateVolumeQMPClient) CancelBlockJob(ctx context.Context, jobID string) error {
	_, err := c.execute(ctx, "block-job-cancel", map[string]any{"device": jobID, "force": true})
	var transportErr *qmpTransportError
	if errors.As(err, &transportErr) {
		return fmt.Errorf("%w: %v", ErrStateVolumeCompactionIndeterminate, err)
	}
	return err
}

func (c *stateVolumeQMPClient) Quit(ctx context.Context) error {
	_, err := c.execute(ctx, "quit", nil)
	if err != nil {
		// QEMU is permitted to close the monitor before emitting quit's reply.
		var netErr net.Error
		if errors.As(err, &netErr) || errors.Is(err, net.ErrClosed) {
			return nil
		}
	}
	return err
}

func (c *stateVolumeQMPClient) Close() error {
	return c.conn.Close()
}
