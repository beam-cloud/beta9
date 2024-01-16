package gateway

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"

	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/repository"
)

const (
	listenPort int = 2005
)

type containerProxy struct {
	rdb           *common.RedisClient
	backendRepo   repository.BackendRepository
	listenPort    int
	useHostHeader bool
}

type replayConn struct {
	net.Conn
	prefix io.Reader
}

func newReplayConn(c net.Conn, prefix []byte) net.Conn {
	return &replayConn{
		Conn:   c,
		prefix: io.MultiReader(bytes.NewReader(prefix), c),
	}
}

func (pc *replayConn) Read(b []byte) (int, error) {
	return pc.prefix.Read(b)
}

func newContainerProxy(rdb *common.RedisClient, backendRepo repository.BackendRepository) (*containerProxy, error) {
	var useHostHeader bool = false
	// stage := common.Secrets().GetWithDefault("STAGE", common.EnvLocal)
	// if stage == common.EnvLocal {
	// 	useHostHeader = false
	// }

	return &containerProxy{
		rdb:           rdb,
		backendRepo:   backendRepo,
		listenPort:    listenPort,
		useHostHeader: useHostHeader,
	}, nil
}

func (cp *containerProxy) handleConnection(clientConn net.Conn) {
	defer clientConn.Close()

	// Use a bufio.Reader so we can use http.ReadRequest but keep the data for later.
	reader := bufio.NewReader(clientConn)

	// Read the request.
	request, err := http.ReadRequest(reader)
	if err != nil {
		cp.logError(err)
		return
	}

	targetHost, err := cp.getTargetHost(request)
	if err != nil {
		cp.logError(err)
		return
	}

	// Check for authorization
	// authed, err := cp.isAuthorized(request)
	// if !authed || err != nil {
	// 	cp.logError(err)
	// 	r := &http.Response{
	// 		ProtoMajor: 1,
	// 		ProtoMinor: 1,
	// 		StatusCode: http.StatusUnauthorized,
	// 	}
	// 	r.Write(clientConn)
	// 	return
	// }

	targetConn, err := net.Dial("tcp", targetHost)
	if err != nil {
		cp.logError(err)
		return
	}
	defer targetConn.Close()

	// We'll need to save a copy of the body data so we can replay it later
	var bodyData bytes.Buffer

	// Replace the request body with one that also writes to bodyData.
	request.Body = ioutil.NopCloser(io.TeeReader(request.Body, &bodyData))

	// Serialize the request to the target server, including the body.
	err = request.Write(targetConn)
	if err != nil {
		cp.logError(err)
		return
	}

	// Replay the rest of the original connection
	// Prepend the part of the body we've already read.
	clientConn = newReplayConn(clientConn, bodyData.Bytes())

	go cp.copy(clientConn, targetConn)
	cp.copy(targetConn, clientConn)
}

func (cp *containerProxy) copy(src, dest net.Conn) {
	_, err := io.Copy(dest, src)
	if err != nil {
		cp.logError(err)
	}
}

func (cp *containerProxy) getTargetHost(request *http.Request) (string, error) {
	var containerId string = ""

	if cp.useHostHeader {
		host := request.Host
		hostParts := strings.Split(host, ".")
		if len(hostParts) < 1 {
			return "", errors.New("invalid host")
		}

		containerId = hostParts[0]
	} else {
		path := request.URL.Path
		pathParts := strings.Split(path, "/")
		if len(pathParts) < 1 {
			return "", errors.New("invalid host path")
		}

		containerId = pathParts[1]
	}

	if containerId == "" {
		return "", errors.New("unable to extract container id")
	}

	hostname, err := cp.rdb.Get(context.TODO(), common.RedisKeys.SchedulerContainerHost(containerId)).Result()
	if err != nil {
		return "", err
	}

	return hostname, nil
}

func (cp *containerProxy) Listen() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cp.listenPort))
	if err != nil {
		cp.logError(err)
		return
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			cp.logError(err)
			continue
		}

		go cp.handleConnection(conn)
	}
}

func (cp *containerProxy) logError(err error) {
	log.Printf("error handling request: %v", err)
}

// Authenticates requests using bearer token
// func (cp *containerProxy) isAuthorized(r *http.Request) (bool, error) {
// 	u, p, ok := r.BasicAuth()
// 	if !ok {
// 		return false, errors.New("missing basic auth")
// 	}

// 	// todo: cache this result
// 	a, _, err := cp.backendRepo.AuthorizeToken(r.Context(), p)
// 	if err != nil {
// 		return false, err
// 	}

// 	return a, nil
// }

// Handles requests for reserved paths like /health and /
func handleReservedPaths(c net.Conn, r *http.Request) error {
	response := &http.Response{
		Header:     http.Header{"Content-Type": []string{"text/plain"}},
		ProtoMajor: 1,
		ProtoMinor: 1,
	}

	switch path := r.URL.Path; path {
	case "/healthz":
		response.Body = io.NopCloser(bytes.NewBufferString("OK"))
		response.StatusCode = http.StatusOK
	case "/robots.txt":
		response.Body = io.NopCloser(bytes.NewBufferString("User-agent: *\nDisallow: /"))
		response.StatusCode = http.StatusOK
	case "/", "/favicon.ico":
		response.StatusCode = http.StatusNotFound
	default:
		return fmt.Errorf("unknown path: %v", path)
	}

	return response.Write(c)
}
