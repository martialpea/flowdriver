package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
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
	// callback برای ارسال لاگ به Kotlin
	logCallback func(string)
)

func logf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	log.Println(msg)
}

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
) int32 {
	globalMu.Lock()
	if running {
		globalMu.Unlock()
		return -1
	}

	configJson := C.GoString(configJsonC)
	credFile   := C.GoString(credFileC)

	ctx, cancel := context.WithCancel(context.Background())
	globalCancel = cancel
	running = true
	globalMu.Unlock()

	err := runClient(ctx, configJson, credFile)

	globalMu.Lock()
	running = false
	globalMu.Unlock()

	if err != nil {
		logf("[ERROR] %v", err)
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

func runClient(ctx context.Context, configJson, credFilePath string) error {
	// ── ۱. بررسی فایل‌ها ─────────────────────────────────────────────────────
	tokenPath := credFilePath + ".token"

	logf("[DEBUG] credFile: %s", credFilePath)
	logf("[DEBUG] tokenFile: %s", tokenPath)

	// بررسی وجود token file
	tokenData, err := os.ReadFile(tokenPath)
	if err != nil {
		return fmt.Errorf("token file not found at %s: %w", tokenPath, err)
	}

	// بررسی محتوای token
	var tokenCheck struct {
		RefreshToken string `json:"refresh_token"`
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
	}
	if err := json.Unmarshal(tokenData, &tokenCheck); err != nil {
		return fmt.Errorf("token file invalid JSON: %w", err)
	}
	if tokenCheck.RefreshToken == "" {
		return fmt.Errorf("token file missing refresh_token")
	}
	logf("[DEBUG] refresh_token: %s...", tokenCheck.RefreshToken[:20])
	logf("[DEBUG] client_id present: %v", tokenCheck.ClientID != "")
	logf("[DEBUG] client_secret present: %v", tokenCheck.ClientSecret != "")

	// ── ۲. parse config ───────────────────────────────────────────────────────
	cfg, err := config.FromJSON(configJson)
	if err != nil {
		return fmt.Errorf("config parse: %w", err)
	}
	logf("[DEBUG] folder_id: %s", cfg.GoogleFolderID)
	logf("[DEBUG] transport.TargetIP: %s", cfg.Transport.TargetIP)

	// ── ۳. login ──────────────────────────────────────────────────────────────
	hc := httpclient.NewCustomClient(cfg.Transport)
	backend := storage.NewGoogleBackend(hc, credFilePath, cfg.GoogleFolderID)

	logf("[INFO] Attempting login...")
	loginCtx, loginCancel := context.WithTimeout(ctx, 30*time.Second)
	defer loginCancel()

	if err := backend.Login(loginCtx); err != nil {
		return fmt.Errorf("login: %w", err)
	}
	logf("[INFO] Login OK")

	// ── ۴. folder ─────────────────────────────────────────────────────────────
	if cfg.GoogleFolderID == "" {
		logf("[INFO] Finding Drive folder...")
		id, _ := backend.FindFolder(ctx, "Flow-Data")
		if id == "" {
			logf("[INFO] Creating Drive folder...")
			id, _ = backend.CreateFolder(ctx, "Flow-Data")
		}
		cfg.GoogleFolderID = id
		logf("[INFO] Folder ID: %s", id)
	}

	// ── ۵. engine ─────────────────────────────────────────────────────────────
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

	// ── ۶. SOCKS5 ─────────────────────────────────────────────────────────────
	listenAddr := cfg.ListenAddr
	if listenAddr == "" {
		listenAddr = "127.0.0.1:1080"
	}

	srv := socks5.NewServer(
		socks5.WithResolver(rawResolver{}),
		socks5.WithDial(func(dc context.Context, network, addr string) (net.Conn, error) {
			sid := genID()
			logf("[OK] session %s -> %s", sid[:8], addr)
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

	logf("[INFO] SOCKS5 listening on %s", listenAddr)

	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.ListenAndServe("tcp", listenAddr) }()

	select {
	case <-ctx.Done():
		logf("[INFO] stopped by context")
		return nil
	case err := <-srvErr:
		return fmt.Errorf("socks5 server: %w", err)
	}
}

func main() {}
