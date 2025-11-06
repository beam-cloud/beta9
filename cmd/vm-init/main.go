package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

// guestMessage represents a message sent to/from the host
type guestMessage struct {
	Type     string   `json:"type"` // "run", "exec", "kill", "output", "exit", "error"
	Process  *process `json:"process,omitempty"`
	Signal   *int     `json:"signal,omitempty"`
	Output   *string  `json:"output,omitempty"`
	Error    *string  `json:"error,omitempty"`
	ExitCode *int     `json:"exit_code,omitempty"`
}

// process represents a process to run in the guest
type process struct {
	Args     []string `json:"args"`
	Env      []string `json:"env"`
	Cwd      string   `json:"cwd"`
	Terminal bool     `json:"terminal"`
}

const (
	// Firecracker host CID (always 2)
	hostCID = 2
	// Vsock port for communication (will be passed via kernel args or use default)
	defaultVsockPort = 10000
)

func main() {
	log.SetPrefix("[beta9-vm-init] ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// Initialize system
	if err := initSystem(); err != nil {
		log.Fatalf("Failed to initialize system: %v", err)
	}

	// Determine vsock port (from env or default)
	port := defaultVsockPort
	if portStr := os.Getenv("BETA9_VSOCK_PORT"); portStr != "" {
		fmt.Sscanf(portStr, "%d", &port)
	}

	// Connect to host via vsock
	conn, err := connectToHost(port)
	if err != nil {
		log.Fatalf("Failed to connect to host: %v", err)
	}
	defer conn.Close()

	log.Printf("Connected to host on vsock port %d", port)

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	// Start message handler
	if err := handleMessages(conn, sigCh); err != nil {
		log.Fatalf("Message handler failed: %v", err)
	}
}

// initSystem performs basic system initialization
func initSystem() error {
	// Mount essential filesystems
	if err := mountFilesystems(); err != nil {
		return fmt.Errorf("failed to mount filesystems: %w", err)
	}

	// Set up networking if needed
	if err := setupNetwork(); err != nil {
		log.Printf("Warning: network setup failed: %v", err)
		// Don't fail - networking might not be needed
	}

	return nil
}

// mountFilesystems mounts essential virtual filesystems
func mountFilesystems() error {
	mounts := []struct {
		source string
		target string
		fstype string
		flags  uintptr
	}{
		{"proc", "/proc", "proc", syscall.MS_NODEV | syscall.MS_NOSUID | syscall.MS_NOEXEC},
		{"sysfs", "/sys", "sysfs", syscall.MS_NODEV | syscall.MS_NOSUID | syscall.MS_NOEXEC},
		{"devtmpfs", "/dev", "devtmpfs", syscall.MS_NOSUID},
		{"devpts", "/dev/pts", "devpts", syscall.MS_NOEXEC | syscall.MS_NOSUID},
		{"tmpfs", "/dev/shm", "tmpfs", syscall.MS_NODEV | syscall.MS_NOSUID},
		{"tmpfs", "/tmp", "tmpfs", syscall.MS_NODEV | syscall.MS_NOSUID},
		{"tmpfs", "/run", "tmpfs", syscall.MS_NODEV | syscall.MS_NOSUID},
	}

	for _, m := range mounts {
		// Create mount point if it doesn't exist
		if err := os.MkdirAll(m.target, 0755); err != nil && !os.IsExist(err) {
			return fmt.Errorf("mkdir %s: %w", m.target, err)
		}

		// Try to mount
		if err := syscall.Mount(m.source, m.target, m.fstype, m.flags, ""); err != nil {
			// Log but don't fail - might already be mounted
			log.Printf("Warning: mount %s failed: %v", m.target, err)
		}
	}

	return nil
}

// setupNetwork performs basic network setup
func setupNetwork() error {
	// Bring up loopback
	if err := exec.Command("ip", "link", "set", "lo", "up").Run(); err != nil {
		return fmt.Errorf("failed to bring up loopback: %w", err)
	}

	// Try DHCP on eth0 if it exists
	if _, err := os.Stat("/sys/class/net/eth0"); err == nil {
		go func() {
			// Run dhclient in background - don't block
			cmd := exec.Command("dhclient", "-v", "eth0")
			if err := cmd.Run(); err != nil {
				log.Printf("DHCP failed: %v", err)
			}
		}()
	}

	return nil
}

// connectToHost establishes a connection to the host via vsock
func connectToHost(port int) (net.Conn, error) {
	// Try connecting with retries
	var conn net.Conn
	var err error

	for i := 0; i < 30; i++ {
		conn, err = net.Dial("vsock", fmt.Sprintf("%d:%d", hostCID, port))
		if err == nil {
			return conn, nil
		}
		time.Sleep(1 * time.Second)
	}

	return nil, fmt.Errorf("failed to connect after retries: %w", err)
}

// handleMessages processes messages from the host
func handleMessages(conn net.Conn, sigCh chan os.Signal) error {
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	var mainCmd *exec.Cmd
	var mainExited chan int

	for {
		select {
		case sig := <-sigCh:
			// Forward signal to main process
			if mainCmd != nil && mainCmd.Process != nil {
				mainCmd.Process.Signal(sig)
			}
			continue
		default:
		}

		var msg guestMessage
		if err := decoder.Decode(&msg); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("decode error: %w", err)
		}

		switch msg.Type {
		case "run":
			if msg.Process == nil {
				sendError(encoder, "run message missing process spec")
				continue
			}

			// Start main process
			var err error
			mainCmd, mainExited, err = startProcess(msg.Process, encoder)
			if err != nil {
				sendError(encoder, fmt.Sprintf("failed to start process: %v", err))
				continue
			}

			// Wait for process exit in background
			go func() {
				exitCode := <-mainExited
				sendExit(encoder, exitCode)
			}()

		case "exec":
			if msg.Process == nil {
				sendError(encoder, "exec message missing process spec")
				continue
			}

			// Start additional process
			_, _, err := startProcess(msg.Process, encoder)
			if err != nil {
				sendError(encoder, fmt.Sprintf("failed to exec process: %v", err))
				continue
			}

		case "kill":
			if mainCmd != nil && mainCmd.Process != nil {
				sig := syscall.SIGTERM
				if msg.Signal != nil {
					sig = syscall.Signal(*msg.Signal)
				}
				if err := mainCmd.Process.Signal(sig); err != nil {
					sendError(encoder, fmt.Sprintf("failed to send signal: %v", err))
				}
			}

		default:
			log.Printf("Unknown message type: %s", msg.Type)
		}
	}
}

// startProcess starts a process and wires up I/O
func startProcess(proc *process, encoder *json.Encoder) (*exec.Cmd, chan int, error) {
	if len(proc.Args) == 0 {
		return nil, nil, fmt.Errorf("no command specified")
	}

	cmd := exec.Command(proc.Args[0], proc.Args[1:]...)
	
	// Set environment
	if len(proc.Env) > 0 {
		cmd.Env = proc.Env
	} else {
		cmd.Env = os.Environ()
	}

	// Set working directory
	if proc.Cwd != "" {
		cmd.Dir = proc.Cwd
	}

	// Wire up I/O
	// For now, capture output and send as messages
	// TODO: Support interactive TTY
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Start the process
	if err := cmd.Start(); err != nil {
		return nil, nil, fmt.Errorf("failed to start: %w", err)
	}

	log.Printf("Started process: %v (PID %d)", proc.Args, cmd.Process.Pid)

	// Forward output
	go forwardOutput(stdout, encoder)
	go forwardOutput(stderr, encoder)

	// Wait for exit
	exitCh := make(chan int, 1)
	go func() {
		err := cmd.Wait()
		exitCode := 0
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				exitCode = exitErr.ExitCode()
			} else {
				exitCode = 1
			}
		}
		exitCh <- exitCode
	}()

	return cmd, exitCh, nil
}

// forwardOutput forwards process output to the host
func forwardOutput(reader io.Reader, encoder *json.Encoder) {
	buf := make([]byte, 4096)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			output := string(buf[:n])
			msg := guestMessage{
				Type:   "output",
				Output: &output,
			}
			if err := encoder.Encode(msg); err != nil {
				log.Printf("Failed to send output: %v", err)
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// sendError sends an error message to the host
func sendError(encoder *json.Encoder, errMsg string) {
	log.Printf("Error: %s", errMsg)
	msg := guestMessage{
		Type:  "error",
		Error: &errMsg,
	}
	encoder.Encode(msg)
}

// sendExit sends an exit message to the host
func sendExit(encoder *json.Encoder, exitCode int) {
	log.Printf("Process exited with code %d", exitCode)
	msg := guestMessage{
		Type:     "exit",
		ExitCode: &exitCode,
	}
	encoder.Encode(msg)
}
