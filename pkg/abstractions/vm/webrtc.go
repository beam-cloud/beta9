package vm

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/pion/stun/v3"
)

// Signaling uses the authenticated HTTP gateway. Media remains end-to-end
// DTLS/SRTP and shares the raw TCP load balancer with the existing TLS proxy.
// The first ICE check authenticates routing; no guest media port is public.
type vmWebRTC struct {
	config types.PodTCPConfig
	dialer *network.BackendDialer
	slots  chan struct{}
}

type iceRoute struct {
	VMID, ContainerID, TokenHash, Password string
	WorkspaceID                            uint
	Generation                             int64
}

var iceCredential = regexp.MustCompile(`^[a-zA-Z0-9+/]{4,256}$`)

func (s *Service) startWebRTC(config types.PodTCPConfig, dialer *network.BackendDialer) {
	runtime, ok := s.runtime.(interface{ SetVMICEHandler(func(net.Conn)) })
	if !ok || !config.Enabled || config.ExternalHost == "" || s.rdb == nil || dialer == nil {
		return
	}
	if config.ExternalPort == 0 {
		config.ExternalPort = 443
	}
	s.webrtc = &vmWebRTC{config: config, dialer: dialer, slots: make(chan struct{}, 256)}
	runtime.SetVMICEHandler(s.serveICE)
}

func (s *Service) configureKioskTransport(spec *types.VMSpec) error {
	if spec.Kiosk == nil {
		return nil
	}
	// Clone before normalizing a template/fork's shared pointer.
	kiosk := *spec.Kiosk
	spec.Kiosk = &kiosk
	if kiosk.Transport == "" {
		kiosk.Transport = "websockets"
		if s.webrtc != nil {
			kiosk.Transport = "webrtc"
		}
	}
	if kiosk.Transport == "webrtc" && s.webrtc == nil {
		return fmt.Errorf("kiosk WebRTC requires the shared TCP gateway; use transport=websockets on this installation")
	}
	return nil
}

func trafficHash(v *types.VM) string {
	sum := sha256.Sum256([]byte(v.TrafficAccessToken))
	return hex.EncodeToString(sum[:])
}

// sessionSDP rewrites only server ICE candidates. The original ICE credentials,
// DTLS fingerprint, codec negotiation and BUNDLE topology reach the browser.
func sessionSDP(sdp string, addresses []net.IP, port int) (string, map[string]string, error) {
	if len(sdp) > 65536 || !strings.HasPrefix(sdp, "v=0") || len(addresses) == 0 {
		return "", nil, fmt.Errorf("invalid WebRTC session description")
	}
	lines := strings.Split(strings.ReplaceAll(sdp, "\r\n", "\n"), "\n")
	credentials := map[string]string{}
	ufrag, password, media := "", "", false
	var output []string
	flush := func() error {
		if !media {
			return nil
		}
		if !iceCredential.MatchString(ufrag) || !iceCredential.MatchString(password) || len(password) < 22 {
			return fmt.Errorf("session requires valid ICE credentials")
		}
		if old, ok := credentials[ufrag]; ok && old != password {
			return fmt.Errorf("conflicting ICE credentials")
		}
		credentials[ufrag] = password
		for i, ip := range addresses {
			output = append(output, fmt.Sprintf("a=candidate:beam%d 1 TCP %d %s %d typ host tcptype passive", i, 1518280447-i, ip.String(), port))
		}
		output = append(output, "a=end-of-candidates")
		return nil
	}
	// Session-level credentials are inherited, media-level credentials override.
	sessionUfrag, sessionPassword := "", ""
	for _, line := range lines {
		if line == "" || strings.HasPrefix(line, "a=candidate:") || line == "a=end-of-candidates" || strings.HasPrefix(line, "a=remote-candidates:") {
			continue
		}
		if strings.HasPrefix(line, "m=") {
			if err := flush(); err != nil {
				return "", nil, err
			}
			media = true
			ufrag, password = sessionUfrag, sessionPassword
		}
		if strings.HasPrefix(line, "a=ice-ufrag:") {
			ufrag = strings.TrimPrefix(line, "a=ice-ufrag:")
			if !media {
				sessionUfrag = ufrag
			}
		}
		if strings.HasPrefix(line, "a=ice-pwd:") {
			password = strings.TrimPrefix(line, "a=ice-pwd:")
			if !media {
				sessionPassword = password
			}
		}
		output = append(output, line)
	}
	if err := flush(); err != nil {
		return "", nil, err
	}
	if len(credentials) == 0 || len(credentials) > 8 {
		return "", nil, fmt.Errorf("unsupported ICE session")
	}
	return strings.Join(output, "\r\n") + "\r\n", credentials, nil
}

func (s *Service) registerICE(c echo.Context, v *types.VM) error {
	if c.Request().Method != http.MethodPost || c.Request().Header.Get("X-Beam-Kiosk-Action") != "webrtc" {
		return echo.NewHTTPError(http.StatusForbidden, "explicit WebRTC registration required")
	}
	if s.webrtc == nil || v.Spec.Kiosk.Transport != "webrtc" {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "WebRTC is unavailable for this session")
	}
	var request struct {
		SDP string `json:"sdp"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(c.Response(), c.Request().Body, 128*1024)).Decode(&request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid WebRTC registration")
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), 5*time.Second)
	defer cancel()
	// Bound registrations per VM, including failed offers. One key works on
	// Redis Cluster and expires even if the requesting gateway crashes.
	count, err := s.rdb.Eval(ctx, `local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('EXPIRE',KEYS[1],60) end; return n`, []string{"vm:ice-rate:" + v.ID}).Int()
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "WebRTC registry unavailable")
	}
	if count > 64 {
		return echo.NewHTTPError(http.StatusTooManyRequests, "too many WebRTC sessions; retry shortly")
	}
	addresses, err := net.DefaultResolver.LookupIP(ctx, "ip", s.webrtc.config.ExternalHost)
	if err != nil || len(addresses) == 0 {
		// Some installations publish only the wildcard service record.
		addresses, err = net.DefaultResolver.LookupIP(ctx, "ip", "webrtc."+s.webrtc.config.ExternalHost)
	}
	if err != nil || len(addresses) == 0 {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "WebRTC TCP gateway DNS unavailable")
	}
	if len(addresses) > 8 {
		addresses = addresses[:8]
	}
	sdp, credentials, err := sessionSDP(request.SDP, addresses, s.webrtc.config.ExternalPort)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	for ufrag, password := range credentials {
		route := iceRoute{VMID: v.ID, WorkspaceID: v.WorkspaceID, ContainerID: v.ContainerID, Generation: v.Generation, TokenHash: trafficHash(v), Password: password}
		encoded, _ := json.Marshal(route)
		// Never replace another VM's colliding fragment. Retries can renew the
		// exact same session; a stopped/forked/rotated VM cannot inherit it.
		ok, err := s.rdb.Eval(ctx, `local old=redis.call('GET',KEYS[1]); if not old or old==ARGV[1] then redis.call('SET',KEYS[1],ARGV[1],'EX',600); return 1 end; return 0`, []string{"vm:ice:" + ufrag}, string(encoded)).Int()
		if err != nil {
			return echo.NewHTTPError(http.StatusServiceUnavailable, "WebRTC registry unavailable")
		}
		if ok != 1 {
			return echo.NewHTTPError(http.StatusConflict, "ICE session collision; reconnect")
		}
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusOK, map[string]any{"sdp": sdp})
}

func readICECheck(conn net.Conn) ([]byte, *stun.Message, string, error) {
	var header [2]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return nil, nil, "", err
	}
	size := int(binary.BigEndian.Uint16(header[:]))
	if size < 20 || size > 8192 {
		return nil, nil, "", fmt.Errorf("invalid ICE frame")
	}
	frame := make([]byte, size+2)
	copy(frame, header[:])
	if _, err := io.ReadFull(conn, frame[2:]); err != nil {
		return nil, nil, "", err
	}
	message := &stun.Message{Raw: frame[2:]}
	if err := message.Decode(); err != nil {
		return nil, nil, "", err
	}
	if message.Type != stun.BindingRequest || !message.Contains(stun.AttrMessageIntegrity) {
		return nil, nil, "", fmt.Errorf("authenticated ICE binding required")
	}
	var username stun.Username
	if err := username.GetFrom(message); err != nil {
		return nil, nil, "", err
	}
	ufrag, remote, ok := strings.Cut(string(username), ":")
	if !ok || !iceCredential.MatchString(ufrag) || !iceCredential.MatchString(remote) {
		return nil, nil, "", fmt.Errorf("invalid ICE username")
	}
	return frame, message, ufrag, nil
}

func (s *Service) iceVM(ctx context.Context, route iceRoute) (*types.VM, error) {
	v, err := s.repo.GetVM(ctx, route.WorkspaceID, route.VMID)
	if err != nil {
		return nil, err
	}
	if v.ContainerID != route.ContainerID || v.Generation != route.Generation || v.DesiredState != types.VMStateRunning || v.TrafficAccessToken == "" || trafficHash(v) != route.TokenHash || v.Spec.Kiosk == nil || v.Spec.Kiosk.Transport != "webrtc" {
		return nil, fmt.Errorf("WebRTC session is no longer active")
	}
	if _, err := s.accessOwner(ctx, v); err != nil {
		return nil, err
	}
	return v, nil
}

func (s *Service) serveICE(conn net.Conn) {
	select {
	case s.webrtc.slots <- struct{}{}:
	default:
		return
	}
	defer func() { <-s.webrtc.slots }()
	defer conn.Close()
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	frame, message, ufrag, err := readICECheck(conn)
	if err != nil {
		return
	}
	authCtx, authCancel := context.WithTimeout(ctx, 5*time.Second)
	defer authCancel()
	encoded, err := s.rdb.Get(authCtx, "vm:ice:"+ufrag).Bytes()
	if err != nil {
		return
	}
	var route iceRoute
	if json.Unmarshal(encoded, &route) != nil || stun.MessageIntegrity([]byte(route.Password)).Check(message) != nil {
		return
	}
	if message.Contains(stun.AttrFingerprint) && stun.Fingerprint.Check(message) != nil {
		return
	}
	v, err := s.iceVM(authCtx, route)
	if err != nil {
		return
	}
	// Limit each live ICE session on each gateway, too; possessing a session
	// credential must not permit unbounded connections to a guest.
	key := "vm:ice-open:" + ufrag
	lease := uuid.NewString()
	now := time.Now().UnixMilli()
	n, err := s.rdb.Eval(authCtx, `redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',ARGV[1]); redis.call('ZADD',KEYS[1],ARGV[2],ARGV[3]); redis.call('EXPIRE',KEYS[1],30); return redis.call('ZCARD',KEYS[1])`, []string{key}, now, now+30000, lease).Int()
	if err != nil {
		return
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), time.Second)
		defer done()
		s.rdb.ZRem(cleanup, key, lease)
	}()
	if n > 8 {
		return
	}
	addresses, err := s.containers.GetContainerAddressMap(v.ContainerID)
	if err != nil || addresses[int32(types.VMKioskMediaPort)] == "" {
		return
	}
	backend, err := s.webrtc.dialer.Dial(authCtx, addresses[int32(types.VMKioskMediaPort)])
	if err != nil {
		return
	}
	defer backend.Close()
	if _, err := s.iceVM(authCtx, route); err != nil {
		return
	}
	release, err := s.keepActive(ctx, v.ID, v.Spec.IdleTimeout)
	if err != nil {
		return
	}
	defer release()
	_ = backend.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := backend.Write(frame); err != nil {
		return
	}
	_ = backend.SetWriteDeadline(time.Time{})
	_ = conn.SetDeadline(time.Time{})
	stopBackend := context.AfterFunc(ctx, func() { backend.Close() })
	defer stopBackend()
	go func() {
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				check, done := context.WithTimeout(ctx, 5*time.Second)
				_, err := s.iceVM(check, route)
				if err == nil {
					err = s.rdb.Eval(check, `redis.call('ZADD',KEYS[1],ARGV[1],ARGV[2]); redis.call('EXPIRE',KEYS[1],30); return 1`, []string{key}, time.Now().UnixMilli()+30000, lease).Err()
				}
				done()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()
	done := make(chan struct{}, 2)
	copyStream := func(dst, src net.Conn) { io.Copy(dst, src); cancel(); done <- struct{}{} }
	go copyStream(backend, conn)
	go copyStream(conn, backend)
	<-done
	<-done
}
