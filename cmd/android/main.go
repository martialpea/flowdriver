package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/NullLatency/flow-driver/internal/config"
	"github.com/NullLatency/flow-driver/internal/httpclient"
	"github.com/NullLatency/flow-driver/internal/storage"
	"github.com/NullLatency/flow-driver/internal/transport"
	socks5 "github.com/things-go/go-socks5"
	"github.com/things-go/go-socks5/statute"
)

var (
	globalCancel context.CancelFunc
	globalMu     sync.Mutex
	running      bool
)

func genID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

type rawResolver struct{}

func (rawResolver) Resolve(ctx context.Context, name string) (context.Context, net.IP, error) {
	return ctx, nil, nil
}

// FIX: نام دقیق JNI = Java_ + package (نقطه‌ها به _) + کلاس + متد
// package: com.flowdriver.service
// class:   FlowBridge
// method:  flowStart

//export Java_com_flowdriver_service_FlowBridge_flowStart
func Java_com_flowdriver_service_FlowBridge_flowStart(
	env uintptr,
	obj uintptr,
	configJsonPtr uintptr,
	tokenJsonPtr uintptr,
	credFilePtr uintptr,
) int32 {
	// چون JNIEnv در CGO پیچیده است، از روش دیگری استفاده می‌کنیم:
	// Kotlin قبل از صدا زدن این تابع، string ها رو از طریق تابع helper ست می‌کنه
	return 0
}

//export Java_com_flowdriver_service_FlowBridge_flowStop
func Java_com_flowdriver_service_FlowBridge_flowStop(env uintptr, obj uintptr) {
	globalMu.Lock()
	defer globalMu.Unlock()
	if globalCancel != nil {
		globalCancel()
		globalCancel = nil
	}
	running = false
}

//export Java_com_flowdriver_service_FlowBridge_flowIsRunning
func Java_com_flowdriver_service_FlowBridge_flowIsRunning(env uintptr, obj uintptr) int32 {
	globalMu.Lock()
	defer globalMu.Unlock()
	if running {
		return 1
	}
	return 0
}

// startTunnel: تابع اصلی که از Kotlin صدا زده می‌شه با string های C
//
//export Java_com_flowdriver_service_FlowBridge_startTunnel
func Java_com_flowdriver_service_FlowBridge_startTunnel(
	env uintptr,
	obj uintptr,
	configJsonC *C.char,
	tokenJsonC *C.char,
	credFileC *C.char,
) int32 {
	globalMu.Lock()
	defer globalMu.Unlock()

	if running {
		return -1
	}

	configJson := C.GoString(configJsonC)
	tokenJson  := C.GoString(tokenJsonC)
	credFile   := C.GoString(credFileC)

	_ = tokenJson // token از طریق credFile استفاده می‌شه

	ctx, cancel := context.WithCancel(context.Background())
	globalCancel = cancel
	running = true

	go func() {
		defer func() {
			globalMu.Lock()
			running = false
			globalMu.Unlock()
			log.Println("[INFO] stopped")
		}()
		if err := runClient(ctx, configJson, credFile); err != nil {
			log.Printf("[ERROR] %v", err)
		}
	}()

	return 0
}

func runClient(ctx context.Context, configJson, credFilePath string) error {
	cfg, err := config.FromJSON(configJson)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	hc := httpclient.NewCustomClient(cfg.Transport)
	backend := storage.NewGoogleBackend(hc, credFilePath, cfg.GoogleFolderID)

	loginCtx, loginCancel := context.WithTimeout(ctx, 30*time.Second)
	defer loginCancel()
	if err := backend.Login(loginCtx); err != nil {
		return fmt.Errorf("login: %w", err)
	}
	log.Println("[INFO] logged in")

	if cfg.GoogleFolderID == "" {
		id, _ := backend.FindFolder(ctx, "Flow-Data")
		if id == "" {
			id, _ = backend.CreateFolder(ctx, "Flow-Data")
		}
		cfg.GoogleFolderID = id
	}

	cid := cfg.ClientID
	if cid == "" {
		cid = genID()[:8]
	}

	engine := transport.NewEngine(backend, true, cid)
	if cfg.RefreshRateMs > 0 {
		engine.SetPollRate(cfg.RefreshRateMs)
	}
	if cfg.FlushRateMs > 0 {
		engine.SetFlushRate(cfg.FlushRateMs)
	}
	engine.Start(ctx)

	listenAddr := cfg.ListenAddr
	if listenAddr == "" {
		listenAddr = "127.0.0.1:1080"
	}

	srv := socks5.NewServer(
		socks5.WithResolver(rawResolver{}),
		socks5.WithDial(func(dc context.Context, network, addr string) (net.Conn, error) {
			sid := genID()
			log.Printf("[OK] %s -> %s", sid[:8], addr)
			s := transport.NewSession(sid)
			s.TargetAddr = addr
			engine.AddSession(s)
			s.EnqueueTx(nil)
			return transport.NewVirtualConn(s, engine), nil
		}),
		socks5.WithAssociateHandle(func(actx context.Context, w io.Writer, req *socks5.Request) error {
			pc, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				socks5.SendReply(w, statute.RepCommandNotSupported, nil)
				return err
			}
			addr := pc.LocalAddr().(*net.UDPAddr)
			socks5.SendReply(w, statute.RepSuccess, &net.TCPAddr{
				IP: net.ParseIP("127.0.0.1"), Port: addr.Port,
			})
			go func() { defer pc.Close(); time.Sleep(5 * time.Minute) }()
			return nil
		}),
	)

	log.Printf("[INFO] SOCKS5 on %s", listenAddr)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe("tcp", listenAddr) }()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

func main() {}
