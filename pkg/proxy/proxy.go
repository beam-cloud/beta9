package proxy

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
)

type Proxy struct {
	config        types.AppConfig
	tailscale     *network.Tailscale
	services      []types.InternalService
	tailscaleRepo repository.TailscaleRepository
	activeConns   sync.WaitGroup
	httpServer    *http.Server
}

func NewProxy() (*Proxy, error) {
	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		return nil, err
	}
	config := configManager.GetConfig()

	redisClient, err := common.NewRedisClient(config.Database.Redis, common.WithClientName("Beta9Proxy"))
	if err != nil {
		return nil, err
	}

	tailscaleRepo := repository.NewTailscaleRedisRepository(redisClient, config)

	var tailscale *network.Tailscale = nil
	if config.Tailscale.Enabled {
		tailscale = network.GetOrCreateTailscale(network.TailscaleConfig{
			ControlURL: config.Tailscale.ControlURL,
			AuthKey:    config.Tailscale.AuthKey,
			Debug:      config.Tailscale.Debug,
			Ephemeral:  true,
		},
			tailscaleRepo,
		)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)
	})
	httpServer := &http.Server{Handler: mux}

	return &Proxy{
		config:        config,
		services:      config.Proxy.Services,
		tailscale:     tailscale,
		tailscaleRepo: tailscaleRepo,
		httpServer:    httpServer,
	}, nil
}

func (p *Proxy) Start() error {
	terminationSignal := make(chan os.Signal, 1)
	signal.Notify(terminationSignal, os.Interrupt, syscall.SIGTERM)

	for _, service := range p.services {
		serviceId := uuid.New().String()[:8]

		// Just bind service proxy to a local port if tailscale is disabled
		if !p.config.Tailscale.Enabled {
			listener, err := net.Listen("tcp", fmt.Sprintf(":%d", service.LocalPort))
			if err != nil {
				return err
			}
			p.startServiceProxy(service, listener, serviceId)
			continue
		}

		// If tailscale is enabled, bind services as tailscale nodes
		tailscale := network.GetOrCreateTailscale(network.TailscaleConfig{
			Hostname:   fmt.Sprintf("%s-%s", service.Name, serviceId),
			ControlURL: p.config.Tailscale.ControlURL,
			AuthKey:    p.config.Tailscale.AuthKey,
			Debug:      p.config.Tailscale.Debug,
			Dir:        fmt.Sprintf("/tmp/%s", service.Name),
			Ephemeral:  true,
		}, p.tailscaleRepo)

		listener, err := tailscale.Serve(context.TODO(), service)
		if err != nil {
			return err
		}

		p.startServiceProxy(service, listener, serviceId)
	}

	go p.startHttpServer()

	<-terminationSignal
	log.Println("Termination signal received. Shutting down...")

	p.shutdown()

	return nil
}

func (p *Proxy) startServiceProxy(service types.InternalService, listener net.Listener, serviceId string) {
	log.Printf("Svc<%s> listening on port: %d", service.Name, service.LocalPort)

	if p.config.Tailscale.Enabled {
		go func() {
			for {
				hostName := fmt.Sprintf("%s-%s.%s:%d", service.Name, serviceId, p.config.Tailscale.HostName, service.LocalPort)

				// If user is != "", add it into hostname (for self-managed control servers like headscale)
				if p.config.Tailscale.User != "" {
					hostName = fmt.Sprintf("%s-%s.%s.%s:%d", service.Name, serviceId, p.config.Tailscale.User, p.config.Tailscale.HostName, service.LocalPort)
				}

				err := p.tailscaleRepo.SetHostname(service.Name, serviceId, hostName)
				if err != nil {
					log.Printf("Unable to set tailscale hostname: %+v\n", err)
				}

				time.Sleep(time.Second * 15)
			}
		}()
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("Failed to accept connection for svc<%s>: %v", service.Name, err)
				continue
			}

			go p.handleConnection(conn, service.Destination)
		}
	}()
}

func (p *Proxy) handleConnection(src net.Conn, destination string) {
	dst, err := net.Dial("tcp", destination)
	if err != nil {
		log.Printf("Failed to dial destination %s: %v", destination, err)
		src.Close()
		return
	}

	p.activeConns.Add(1)
	defer p.activeConns.Done()

	var wg sync.WaitGroup
	wg.Add(2)

	// Copy data from src->dst
	go func() {
		defer wg.Done()
		defer dst.Close()
		io.Copy(dst, src)
	}()

	// Copy data from dst->src
	go func() {
		defer wg.Done()
		defer src.Close()
		io.Copy(src, dst)
	}()

	wg.Wait()
}

func (p *Proxy) startHttpServer() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", p.config.Proxy.HTTPPort))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	if err := p.httpServer.Serve(lis); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Failed to start http server: %v", err)
	}
}

func (p *Proxy) shutdown() {
	p.httpServer.Shutdown(context.Background())
	log.Println("Waiting on active connections to finish ...")
	p.activeConns.Wait()
}
