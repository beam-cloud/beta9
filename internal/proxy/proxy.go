package proxy

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/scheduler"
	"github.com/beam-cloud/beta9/internal/types"
)

type Proxy struct {
	config            types.AppConfig
	workerPoolManager *scheduler.WorkerPoolManager
	tailscale         *common.Tailscale
	services          []types.InternalService
}

func NewProxy() (*Proxy, error) {
	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		return nil, err
	}
	config := configManager.GetConfig()

	return &Proxy{
		config:   config,
		services: config.Proxy.Services,
	}, nil
}

func (p *Proxy) Start() error {
	terminationSignal := make(chan os.Signal, 1)
	signal.Notify(terminationSignal, os.Interrupt, syscall.SIGTERM)

	for _, service := range p.services {
		// Just bind service proxy to a local port if tailscale is disabled
		if !p.config.Tailscale.Enabled {
			listener, err := net.Listen("tcp", fmt.Sprintf(":%d", service.LocalPort))
			if err != nil {
				return err
			}
			p.startServiceProxy(service, listener)
		}

		// If tailscale is enabled, bind services as tailscale nodes
		tailscale := common.NewTailscale(common.TailscaleConfig{
			Hostname:   service.Name,
			ControlURL: p.config.Tailscale.ControlURL,
			AuthKey:    p.config.Tailscale.AuthKey,
			Debug:      p.config.Tailscale.Debug,
			Dir:        fmt.Sprintf("/tmp/%s", service.Name),
		})

		listener, err := tailscale.Start(context.TODO(), service)
		if err != nil {
			return err
		}

		p.startServiceProxy(service, listener)
	}

	<-terminationSignal
	log.Println("Termination signal received. Shutting down...")
	return nil
}

func (p *Proxy) startServiceProxy(service types.InternalService, listener net.Listener) {
	log.Printf("Svc<%s> listening on port: %d", service.Name, service.LocalPort)

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
