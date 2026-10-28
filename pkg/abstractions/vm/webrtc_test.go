package vm

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/pion/stun/v3"
	"github.com/stretchr/testify/require"
)

const testICEPassword = "testPasswordLongEnoughForICE123"
const testSDP = "v=0\r\na=group:BUNDLE 0 1\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:0\r\na=ice-ufrag:fragment\r\na=ice-pwd:" + testICEPassword + "\r\na=fingerprint:sha-256 12:34\r\na=candidate:private 1 UDP 100 192.168.1.1 5555 typ host\r\na=end-of-candidates\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=mid:1\r\na=ice-ufrag:fragment\r\na=ice-pwd:" + testICEPassword + "\r\na=sctp-port:5000\r\n"

func TestWebRTCSDPUsesSharedGatewayAndPreservesDTLS(t *testing.T) {
	sdp, credentials, err := sessionSDP(testSDP, []net.IP{net.ParseIP("203.0.113.2"), net.ParseIP("2001:db8::1")}, 443)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"fragment": testICEPassword}, credentials)
	require.NotContains(t, sdp, "192.168.1.1")
	require.Equal(t, 4, strings.Count(sdp, "tcptype passive"))
	for _, line := range []string{"a=group:BUNDLE 0 1", "a=fingerprint:sha-256 12:34", "m=video 9 UDP/TLS/RTP/SAVPF 96", "a=sctp-port:5000", "a=ice-pwd:" + testICEPassword} {
		require.Contains(t, sdp, line)
	}
	_, _, err = sessionSDP(strings.ReplaceAll(testSDP, testICEPassword, "short"), []net.IP{net.ParseIP("127.0.0.1")}, 443)
	require.Error(t, err)
}

func iceTestFixture(t *testing.T) (*Service, *types.VM) {
	t.Helper()
	s, v, _, _, _ := fixture()
	s.ctx = context.Background()
	redis := miniredis.RunT(t)
	var err error
	s.rdb, err = common.NewRedisClient(types.RedisConfig{Addrs: []string{redis.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { s.rdb.Close() })
	v.TrafficAccessToken = "vm-traffic-secret"
	v.Spec.Kiosk = &types.VMKioskSpec{Command: []string{"application"}, Transport: "webrtc"}
	dialer := network.NewBackendDialer(nil, types.TailscaleConfig{}, s.containers, time.Second)
	require.NoError(t, s.startWebRTC(types.PodTCPConfig{Enabled: true, ExternalHost: "127.0.0.1", ExternalPort: 443}, types.VMWebRTCConfig{}, dialer))
	require.NotNil(t, s.runtime.(*vmRuntime).iceHandler)
	return s, v
}

func registerTestICE(t *testing.T, s *Service, v *types.VM, description string) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"sdp": description})
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	r.Header.Set("X-Beam-Kiosk-Action", "webrtc")
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(r, rec)
	if err := s.webrtc.register(c, v); err != nil {
		c.Echo().HTTPErrorHandler(err, c)
	}
	return rec
}

func TestWebRTCRegistrationIsAuthenticatedBoundedAndCollisionSafe(t *testing.T) {
	s, v := iceTestFixture(t)
	rec := registerTestICE(t, s, v, testSDP)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	first := s.rdb.Get(s.ctx, "vm:ice:fragment").Val()
	require.NotEmpty(t, first)
	require.Equal(t, http.StatusOK, registerTestICE(t, s, v, testSDP).Code)
	v.Generation++
	require.Equal(t, http.StatusConflict, registerTestICE(t, s, v, testSDP).Code)
	require.Equal(t, first, s.rdb.Get(s.ctx, "vm:ice:fragment").Val())
	require.NoError(t, s.rdb.Set(s.ctx, "vm:ice-rate:"+v.ID, 64, time.Minute).Err())
	require.Equal(t, http.StatusTooManyRequests, registerTestICE(t, s, v, testSDP).Code)
	c := echo.New().NewContext(httptest.NewRequest(http.MethodPost, "/", nil), httptest.NewRecorder())
	require.Error(t, s.webrtc.register(c, v))
}

func iceFrame(t *testing.T, password string) []byte {
	t.Helper()
	message, err := stun.Build(stun.TransactionID, stun.BindingRequest, stun.NewUsername("fragment:browser"), stun.MessageIntegrity([]byte(password)), stun.Fingerprint)
	require.NoError(t, err)
	frame := make([]byte, 2+len(message.Raw))
	binary.BigEndian.PutUint16(frame, uint16(len(message.Raw)))
	copy(frame[2:], message.Raw)
	return frame
}

func TestWebRTCAuthenticatesBeforeDialAndFencesVMChanges(t *testing.T) {
	for _, scenario := range []string{"valid", "expired lease", "wrong password", "rotated token", "old boot", "stopped", "revoked", "non-STUN lookalike"} {
		t.Run(scenario, func(t *testing.T) {
			s, v := iceTestFixture(t)
			require.Equal(t, http.StatusOK, registerTestICE(t, s, v, testSDP).Code)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer listener.Close()
			containers := &networkVMContainers{vmContainers: s.containers.(*vmContainers), addresses: map[string]map[int32]string{v.ContainerID: {int32(types.VMKioskMediaPort): listener.Addr().String()}}}
			s.containers = containers
			s.webrtc.dialer = network.NewBackendDialer(nil, types.TailscaleConfig{}, containers, time.Second)
			password := testICEPassword
			if scenario == "expired lease" {
				s.webrtc.config.CheckInterval = 10 * time.Millisecond
			}
			switch scenario {
			case "wrong password":
				password = "wrongPasswordLongEnoughForICE"
			case "rotated token":
				v.TrafficAccessToken = "rotated"
			case "old boot":
				v.Generation++
			case "stopped":
				v.DesiredState = types.VMStateStopped
			case "revoked":
				s.backend.(*vmBackend).token = &types.Token{Active: false}
			}
			frame := iceFrame(t, password)
			if scenario == "non-STUN lookalike" {
				frame = append([]byte{0, 20, 0, 1}, make([]byte, 18)...)
			}
			server, client := net.Pipe()
			defer client.Close()
			done := make(chan struct{})
			go func() { s.webrtc.serve(server); close(done) }()
			_ = client.SetDeadline(time.Now().Add(2 * time.Second))
			_, err = client.Write(frame)
			require.NoError(t, err)
			_ = listener.(*net.TCPListener).SetDeadline(time.Now().Add(300 * time.Millisecond))
			backend, err := listener.Accept()
			if scenario != "valid" && scenario != "expired lease" {
				require.Error(t, err, "unauthorized ICE must never reach the private VM")
				<-done
				return
			}
			require.NoError(t, err)
			defer backend.Close()
			_ = backend.SetDeadline(time.Now().Add(2 * time.Second))
			received := make([]byte, len(frame))
			_, err = io.ReadFull(backend, received)
			require.NoError(t, err)
			require.Equal(t, frame, received, "the original authenticated STUN check reaches the native ICE mux")
			go backend.Write([]byte("encrypted-media"))
			media := make([]byte, len("encrypted-media"))
			_, err = io.ReadFull(client, media)
			require.NoError(t, err)
			require.Equal(t, "encrypted-media", string(media))
			if scenario == "expired lease" {
				require.NoError(t, s.rdb.Del(s.ctx, "vm:ice-open:fragment").Err())
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("expired lease must close active media")
				}
				require.Zero(t, s.rdb.ZCard(s.ctx, "vm:ice-open:fragment").Val(), "renewal must not resurrect an expired lease")
			} else {
				client.Close()
				<-done
			}
		})
	}
}

func TestWebRTCRequiresPrivateMediaAndConfiguredGateway(t *testing.T) {
	s, _, _, _, _ := fixture()
	spec := types.VMSpec{ImageID: "image", Kiosk: &types.VMKioskSpec{Command: []string{"app"}, Transport: "webrtc"}}
	require.Error(t, s.configureKioskTransport(&spec))
	require.NoError(t, validate(&spec))
	require.Contains(t, spec.PrivatePorts, types.VMKioskMediaPort)
	spec.Ports = append(spec.Ports, types.VMKioskMediaPort)
	require.Error(t, validate(&spec))
}

func TestWebRTCClosesConnectionsWhenGatewayCapacityIsFull(t *testing.T) {
	s, _ := iceTestFixture(t)
	s.webrtc.slots = make(chan struct{}, 1)
	s.webrtc.slots <- struct{}{}
	server, client := net.Pipe()
	defer client.Close()
	require.NoError(t, client.SetReadDeadline(time.Now().Add(time.Second)))
	s.webrtc.serve(server)
	_, err := client.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
}
