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
	"net/http"
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

//export Java_com_flowdriver_service_FlowBridge_startTunnel
func Java_com_flowdriver_service_FlowBridge_startTunnel(
	env uintptr,
	obj uintptr,
	configJsonC *C.char,
	credFileC *C.char,
	tokenFileC *C.char,
) int32 {
	globalMu.Lock()
	if running {
		globalMu.Unlock()
		return -1
	}

	configJson := C.GoString(configJsonC)
	credFile   := C.GoString(credFileC)
	tokenFile  := C.GoString(tokenFileC)

	ctx, cancel := context.WithCancel(context.Background())
	globalCancel = cancel
	running = true
	globalMu.Unlock()

	log.Printf("[INFO] credFile: %s", credFile)
	log.Printf("[INFO] tokenFile: %s", tokenFile)

	err := runClient(ctx, configJson, credFile, tokenFile)

	globalMu.Lock()
	running = false
	globalMu.Unlock()

	if err != nil {
		log.Printf("[ERROR] %v", err)
		return -2
	}
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

func runClient(ctx context.Context, configJson, credFilePath, tokenFilePath string) error {
	cfg, err := config.FromJSON(configJson)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	// FIX: برای login از http.Client معمولی استفاده کن
	// نه از transport سفارشی که TargetIP داره
	// چون TargetIP=216.239.38.120 فقط برای googleapis.com drive API هست
	// و oauth2.googleapis.com آدرس متفاوتی داره
	plainClient := &http.Client{Timeout: 30 * time.Second}
	backend := storage.NewGoogleBackendWithToken(plainClient, credFilePath, tokenFilePath, cfg.GoogleFolderID)

	log.Println("[INFO] Logging in with plain HTTP client...")
	loginCtx, loginCancel := context.WithTimeout(ctx, 30*time.Second)
	defer loginCancel()

	if err := backend.Login(loginCtx); err != nil {
		return fmt.Errorf("login: %w", err)
	}
	log.Println("[INFO] Login OK!")

	// بعد از login، برای Drive API از transport سفارشی استفاده کن
	customClient := httpclient.NewCustomClient(cfg.Transport)
	driveBackend := storage.NewGoogleBackendWithToken(customClient, credFilePath, tokenFilePath, cfg.GoogleFolderID)
	// token رو از backend اول کپی کن
	driveBackend.CopyTokenFrom(backend)

	if cfg.GoogleFolderID == "" {
		id, _ := driveBackend.FindFolder(ctx, "Flow-Data")
		if id == "" {
			id, _ = driveBackend.CreateFolder(ctx, "Flow-Data")
		}
		cfg.GoogleFolderID = id
		log.Printf("[INFO] Folder: %s", id)
	}

	cid := cfg.ClientID
	if cid == "" {
		cid = genID()[:8]
	}

	engine := transport.NewEngine(driveBackend, true, cid)
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
	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.ListenAndServe("tcp", listenAddr) }()

	select {
	case <-ctx.Done():
		return nil
	case err := <-srvErr:
		return fmt.Errorf("socks5: %w", err)
	}
}

func main() {}
