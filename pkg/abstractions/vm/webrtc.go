package vm

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
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

// Authenticated ICE-TCP shares the TLS listener; DTLS/SRTP stays end to end.
type vmWebRTC struct {
	service   *Service
	config    types.VMWebRTCConfig
	tcpConfig types.PodTCPConfig
	dialer    *network.BackendDialer
	slots     chan struct{}
}

type iceCheck struct {
	frame    []byte
	message  *stun.Message
	fragment string
}

const iceCandidatePriority = 1518280447

var iceCredential = regexp.MustCompile(`^[a-zA-Z0-9+/]{4,256}$`)

func (s *Service) startWebRTC(tcpConfig types.PodTCPConfig, config types.VMWebRTCConfig, dialer *network.BackendDialer) error {
	if err := config.ApplyDefaults(); err != nil {
		return err
	}
	if !tcpConfig.Enabled || tcpConfig.ExternalHost == "" || s.controlPlane == nil || dialer == nil {
		return nil
	}
	if tcpConfig.ExternalPort == 0 {
		tcpConfig.ExternalPort = 443
	}
	s.webrtc = &vmWebRTC{
		service:   s,
		config:    config,
		tcpConfig: tcpConfig,
		dialer:    dialer,
		slots:     make(chan struct{}, config.MaxConnections),
	}
	s.runtime.SetVMICEHandler(s.webrtc.serve)
	return nil
}

func (s *Service) configureKioskTransport(spec *types.VMSpec) error {
	if spec.Kiosk == nil {
		return nil
	}
	// Clone before normalizing a template/fork's shared pointer.
	kiosk := spec.Kiosk.Clone()
	spec.Kiosk = kiosk
	if kiosk.Transport == "" {
		kiosk.Transport = types.VMKioskTransportWebSockets
		if s.webrtc != nil {
			kiosk.Transport = types.VMKioskTransportWebRTC
		}
	}
	if kiosk.Transport == types.VMKioskTransportWebRTC && s.webrtc == nil {
		return fmt.Errorf("kiosk WebRTC requires the shared TCP gateway; use transport=websockets on this installation")
	}
	return nil
}

func trafficHash(v *types.VM) string {
	sum := sha256.Sum256([]byte(v.TrafficAccessToken))
	return hex.EncodeToString(sum[:])
}

// sessionSDP replaces server candidates, preserving credentials and DTLS negotiation.
func sessionSDP(sdp string, addresses []net.IP, port int) (string, map[string]string, error) {
	if len(sdp) > types.VMKioskMaxSDPBytes || !strings.HasPrefix(sdp, "v=0") || len(addresses) == 0 {
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
			output = append(output, fmt.Sprintf("a=candidate:beam%d 1 TCP %d %s %d typ host tcptype passive", i, iceCandidatePriority-i, ip.String(), port))
		}
		output = append(output, "a=end-of-candidates")
		return nil
	}
	// Session-level credentials are inherited, media-level credentials override.
	sessionUfrag, sessionPassword := "", ""
	sections := 0
	for _, line := range lines {
		if line == "" || strings.HasPrefix(line, "a=candidate:") || line == "a=end-of-candidates" || strings.HasPrefix(line, "a=remote-candidates:") {
			continue
		}
		if strings.HasPrefix(line, "m=") {
			sections++
			if sections > types.VMKioskMaxMediaSections {
				return "", nil, fmt.Errorf("unsupported ICE media sections")
			}
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
	if len(credentials) == 0 {
		return "", nil, fmt.Errorf("unsupported ICE session")
	}
	return strings.Join(output, "\r\n") + "\r\n", credentials, nil
}

func (w *vmWebRTC) register(c echo.Context, v *types.VM) error {
	if c.Request().Method != http.MethodPost || c.Request().Header.Get(types.VMKioskActionHeader) != "webrtc" {
		return echo.NewHTTPError(http.StatusForbidden, "explicit WebRTC registration required")
	}
	if w == nil || v.Spec.Kiosk == nil || v.Spec.Kiosk.Transport != types.VMKioskTransportWebRTC {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "WebRTC is unavailable for this session")
	}
	controller := http.NewResponseController(c.Response().Writer)
	_ = controller.SetReadDeadline(time.Now().Add(w.config.HandshakeTimeout))
	defer controller.SetReadDeadline(time.Time{})
	var request types.VMWebRTCSessionDescription
	c.Request().Body = http.MaxBytesReader(c.Response(), c.Request().Body, 2*types.VMKioskMaxSDPBytes)
	if err := decode(c, &request); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid WebRTC registration")
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), w.config.HandshakeTimeout)
	defer cancel()
	// Rate-limit offers per VM, including failed registrations.
	count, err := w.service.controlPlane.IncrementICERegistrations(ctx, v.ID)
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "WebRTC registry unavailable")
	}
	if count > w.config.RegistrationsPerMinute {
		return echo.NewHTTPError(http.StatusTooManyRequests, "too many WebRTC sessions; retry shortly")
	}
	addresses, err := net.DefaultResolver.LookupIP(ctx, "ip", w.tcpConfig.ExternalHost)
	if err != nil || len(addresses) == 0 {
		// Some installations publish only the wildcard service record.
		addresses, err = net.DefaultResolver.LookupIP(ctx, "ip", "webrtc."+w.tcpConfig.ExternalHost)
	}
	if err != nil || len(addresses) == 0 {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "WebRTC TCP gateway DNS unavailable")
	}
	if len(addresses) > types.VMKioskMaxGatewayAddresses {
		addresses = addresses[:types.VMKioskMaxGatewayAddresses]
	}
	sdp, credentials, err := sessionSDP(request.SDP, addresses, w.tcpConfig.ExternalPort)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	for ufrag, password := range credentials {
		route := ICESession{
			VMID:        v.ID,
			WorkspaceID: v.WorkspaceID,
			ContainerID: v.ContainerID,
			Generation:  v.Generation,
			TokenHash:   trafficHash(v),
			Password:    password,
		}
		ok, err := w.service.controlPlane.RegisterICESession(ctx, ufrag, route, w.config.SessionTTL)
		if err != nil {
			return echo.NewHTTPError(http.StatusServiceUnavailable, "WebRTC registry unavailable")
		}
		if !ok {
			return echo.NewHTTPError(http.StatusConflict, "ICE session collision; reconnect")
		}
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusOK, types.VMWebRTCSessionDescription{SDP: sdp})
}

func readICECheck(conn net.Conn) (*iceCheck, error) {
	var header [2]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return nil, err
	}
	size := int(binary.BigEndian.Uint16(header[:]))
	if size < 20 || size > types.VMKioskMaxICEFrameBytes {
		return nil, fmt.Errorf("invalid ICE frame")
	}
	frame := make([]byte, size+2)
	copy(frame, header[:])
	if _, err := io.ReadFull(conn, frame[2:]); err != nil {
		return nil, err
	}
	message := &stun.Message{Raw: frame[2:]}
	if err := message.Decode(); err != nil {
		return nil, err
	}
	if message.Type != stun.BindingRequest || !message.Contains(stun.AttrMessageIntegrity) {
		return nil, fmt.Errorf("authenticated ICE binding required")
	}
	var username stun.Username
	if err := username.GetFrom(message); err != nil {
		return nil, err
	}
	ufrag, remote, ok := strings.Cut(string(username), ":")
	if !ok || !iceCredential.MatchString(ufrag) || !iceCredential.MatchString(remote) {
		return nil, fmt.Errorf("invalid ICE username")
	}
	return &iceCheck{frame: frame, message: message, fragment: ufrag}, nil
}

func (w *vmWebRTC) activeVM(ctx context.Context, route *ICESession) (*types.VM, error) {
	v, err := w.service.repo.GetVM(ctx, route.WorkspaceID, route.VMID)
	if err != nil {
		return nil, err
	}
	if v.ContainerID != route.ContainerID || v.Generation != route.Generation ||
		v.DesiredState != types.VMStateRunning || v.TrafficAccessToken == "" ||
		trafficHash(v) != route.TokenHash || v.Spec.Kiosk == nil ||
		v.Spec.Kiosk.Transport != types.VMKioskTransportWebRTC {
		return nil, fmt.Errorf("WebRTC session is no longer active")
	}
	if _, err := w.service.accessOwner(ctx, v); err != nil {
		return nil, err
	}
	return v, nil
}

func (w *vmWebRTC) serve(conn net.Conn) {
	defer conn.Close()
	select {
	case w.slots <- struct{}{}:
	default:
		return
	}
	defer func() { <-w.slots }()
	ctx, cancel := context.WithCancel(w.service.ctx)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	_ = conn.SetDeadline(time.Now().Add(w.config.HandshakeTimeout))
	check, err := readICECheck(conn)
	if err != nil {
		return
	}
	authCtx, authCancel := context.WithTimeout(ctx, w.config.HandshakeTimeout)
	defer authCancel()
	route, err := w.service.controlPlane.GetICESession(authCtx, check.fragment)
	if err != nil || route == nil || stun.MessageIntegrity([]byte(route.Password)).Check(check.message) != nil {
		return
	}
	if check.message.Contains(stun.AttrFingerprint) && stun.Fingerprint.Check(check.message) != nil {
		return
	}
	v, err := w.activeVM(authCtx, route)
	if err != nil {
		return
	}
	// Bound each ICE session across gateways with expiring connection leases.
	lease := uuid.NewString()
	n, err := w.service.controlPlane.AcquireICELease(authCtx, check.fragment, lease, w.config.LeaseTTL)
	if err != nil {
		return
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), time.Second)
		defer done()
		w.service.controlPlane.ReleaseICELease(cleanup, check.fragment, lease)
	}()
	if n > w.config.MaxConnectionsPerSession {
		return
	}
	addresses, err := w.service.containers.GetContainerAddressMap(v.ContainerID)
	if err != nil || addresses[int32(types.VMKioskMediaPort)] == "" {
		return
	}
	backend, err := w.dialer.Dial(authCtx, addresses[int32(types.VMKioskMediaPort)])
	if err != nil {
		return
	}
	defer backend.Close()
	if _, err := w.activeVM(authCtx, route); err != nil {
		return
	}
	release, err := w.service.keepActive(ctx, v.ID, v.Spec.IdleTimeout)
	if err != nil {
		return
	}
	defer release()
	_ = backend.SetWriteDeadline(time.Now().Add(w.config.HandshakeTimeout))
	if _, err := backend.Write(check.frame); err != nil {
		return
	}
	_ = backend.SetWriteDeadline(time.Time{})
	_ = conn.SetDeadline(time.Time{})
	stopBackend := context.AfterFunc(ctx, func() { backend.Close() })
	defer stopBackend()
	go func() {
		tick := time.NewTicker(w.config.CheckInterval)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				renewCtx, done := context.WithTimeout(ctx, w.config.HandshakeTimeout)
				_, err := w.activeVM(renewCtx, route)
				if err == nil {
					err = w.service.controlPlane.RenewICELease(renewCtx, check.fragment, lease, w.config.LeaseTTL)
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
	copyStream := func(dst, src net.Conn) {
		io.Copy(dst, src)
		cancel()
		done <- struct{}{}
	}
	go copyStream(backend, conn)
	go copyStream(conn, backend)
	<-done
	<-done
}
