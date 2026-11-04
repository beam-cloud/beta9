package disk

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

// initializeVhostWriteBack sets QSD's cache mode before a frontend connects.
// QSD initially advertises write-through independently of its block cache.
// Configuring it here makes Linux enable flushes during device discovery and
// avoids a guest config write racing vhost-user virtqueue initialization.
func initializeVhostWriteBack(ctx context.Context, socket string) error {
	c, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
	if err != nil {
		return err
	}
	defer c.Close()
	deadline := time.Now().Add(3 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := c.SetDeadline(deadline); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = c.SetDeadline(time.Now()) })
	defer stop()
	send := func(request uint32, payload []byte) error {
		msg := make([]byte, 12+len(payload))
		binary.LittleEndian.PutUint32(msg, request)
		binary.LittleEndian.PutUint32(msg[4:], 1)
		binary.LittleEndian.PutUint32(msg[8:], uint32(len(payload)))
		copy(msg[12:], payload)
		n, err := c.Write(msg)
		if err == nil && n != len(msg) {
			err = io.ErrShortWrite
		}
		return err
	}
	receive := func(request uint32) ([]byte, error) {
		header := make([]byte, 12)
		if _, err := io.ReadFull(c, header); err != nil {
			return nil, err
		}
		size := binary.LittleEndian.Uint32(header[8:])
		if binary.LittleEndian.Uint32(header) != request || binary.LittleEndian.Uint32(header[4:]) != 5 || size > 268 {
			return nil, fmt.Errorf("invalid vhost-user config response")
		}
		data := make([]byte, size)
		_, err := io.ReadFull(c, data)
		return data, err
	}
	exchange := func(request uint32, payload []byte) ([]byte, error) {
		if err := send(request, payload); err != nil {
			return nil, err
		}
		return receive(request)
	}
	u64 := func(n uint64) []byte { b := make([]byte, 8); binary.LittleEndian.PutUint64(b, n); return b }
	features, err := exchange(1, nil) // GET_FEATURES
	if err != nil {
		return fmt.Errorf("read vhost-user features: %w", err)
	}
	if len(features) != 8 {
		return fmt.Errorf("invalid vhost-user features response")
	}
	const protocolFeature = uint64(1) << 30
	if binary.LittleEndian.Uint64(features)&protocolFeature == 0 {
		return fmt.Errorf("vhost-user protocol features unavailable")
	}
	if err = send(3, nil); err != nil {
		return err
	} // SET_OWNER
	if err = send(2, u64(binary.LittleEndian.Uint64(features)&(protocolFeature|uint64(1)<<32))); err != nil {
		return err
	}
	protocols, err := exchange(15, nil) // GET_PROTOCOL_FEATURES
	if err != nil {
		return fmt.Errorf("read vhost-user protocols: %w", err)
	}
	if len(protocols) != 8 {
		return fmt.Errorf("invalid vhost-user protocols response")
	}
	const configProtocol = uint64(1) << 9
	if binary.LittleEndian.Uint64(protocols)&configProtocol == 0 {
		return fmt.Errorf("vhost-user config protocol unavailable")
	}
	if err = send(16, u64(configProtocol)); err != nil {
		return err
	} // SET_PROTOCOL_FEATURES
	// virtio_blk_config.wce is byte 32. GET_CONFIG starts at zero because
	// QSD's get_config callback produces its response starting at zero.
	payload := make([]byte, 13)
	binary.LittleEndian.PutUint32(payload, 32)
	binary.LittleEndian.PutUint32(payload[4:], 1)
	payload[12] = 1
	if err = send(25, payload); err != nil {
		return err
	} // SET_CONFIG, master flag 0
	payload = make([]byte, 45)
	binary.LittleEndian.PutUint32(payload[4:], 33)
	config, err := exchange(24, payload) // GET_CONFIG is also the SET_CONFIG fence
	if err != nil {
		return err
	}
	if len(config) != 45 || binary.LittleEndian.Uint32(config[4:]) != 33 || config[44] != 1 {
		return fmt.Errorf("vhost-user export did not enable write-back cache")
	}
	return nil
}
