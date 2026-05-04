// این فایل برای build اندروید به عنوان shared library است
// با CGO کامپایل می‌شه و از JNI برای ارتباط با Kotlin استفاده می‌کنه
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
	"github.com/things-go/go-socks5"
	"github.com/things-go/go-socks5/statute"
)

// ── state ─────────────────────────────────────────────────────────────────────

var (
	globalCancel context.CancelFunc
	globalMu     sync.Mutex
	isRunning    bool
	logCallback  func(string)
)

func genID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func appendLog(msg string) {
	log.Println(msg)
	if logCallback != nil {
		logCallback(msg)
	}
}

type rawResolver struct{}

func (rawResolver) Resolve(ctx context.Context, name string) (context.Context, net.IP, error) {
	return ctx, nil, nil
}

// ── JNI exports ───────────────────────────────────────────────────────────────

//export Java_com_flowdriver_service_FlowBridge_start
func Java_com_flowdriver_service_FlowBridge_start(
	_ *C.void,
	_ *C.void,
	configJsonC *C.char,
	tokenJsonC *C.char,
	credFileC *C.char,
) C.int {
	globalMu.Lock()
	defer globalMu.Unlock()

	if isRunning {
		return C.int(-1)
	}

	configJson := C.GoString(configJsonC)
	tokenJson  := C.GoString(tokenJsonC)
	credFile   := C.GoString(credFileC)

	ctx, cancel := context.WithCancel(context.Background())
	globalCancel = cancel
	isRunning = true

	go func() {
		defer func() {
			globalMu.Lock()
			isRunning = false
			globalMu.Unlock()
			appendLog("[INFO] FlowDriver stopped")
		}()

		if err := runClient(ctx, configJson, tokenJson, credFile); err != nil {
			appendLog(fmt.Sprintf("[ERROR] %v", err))
		}
	}()

	return C.int(0)
}

//export Java_com_flowdriver_service_FlowBridge_stop
func Java_com_flowdriver_service_FlowBridge_stop(_ *C.void, _ *C.void) {
	globalMu.Lock()
	defer globalMu.Unlock()
	if globalCancel != nil {
		globalCancel()
		globalCancel = nil
	}
}

//export Java_com_flowdriver_service_FlowBridge_isRunning
func Java_com_flowdriver_service_FlowBridge_isRunning(_ *C.void, _ *C.void) C.int {
	globalMu.Lock()
	defer globalMu.Unlock()
	if isRunning {
		return C.int(1)
	}
	return C.int(0)
}

// ── core logic ────────────────────────────────────────────────────────────────

func runClient(ctx context.Context, configJson, tokenJson, credFilePath string) error {
	cfg, err := config.FromJSON(configJson)
	if err != nil {
		return fmt.Errorf("config parse: %w", err)
	}

	var backend storage.Backend
	hc := httpclient.NewCustomClient(cfg.Transport)
	backend = storage.NewGoogleBackend(hc, credFilePath, cfg.GoogleFolderID)

	loginCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := backend.Login(loginCtx); err != nil {
		return fmt.Errorf("login: %w", err)
	}
	appendLog("[INFO] Logged in to Google Drive")

	if cfg.GoogleFolderID == "" {
		id, err := backend.FindFolder(ctx, "Flow-Data")
		if err != nil || id == "" {
			id, err = backend.CreateFolder(ctx, "Flow-Data")
			if err != nil {
				return fmt.Errorf("folder: %w", err)
			}
		}
		cfg.GoogleFolderID = id
	}

	cid := cfg.ClientID
	if cid == "" {
		cid = genID()[:8]
	}

	engine := transport.NewEngine(backend, true, cid)
	engine.SetPollRate(cfg.RefreshRateMs)
	engine.SetFlushRate(cfg.FlushRateMs)
	engine.Start(ctx)

	listenAddr := cfg.ListenAddr
	if listenAddr == "" {
		listenAddr = "127.0.0.1:1080"
	}

	srv := socks5.NewServer(
		socks5.WithResolver(rawResolver{}),
		socks5.WithDial(func(dc context.Context, network, addr string) (net.Conn, error) {
			sid := genID()
			appendLog(fmt.Sprintf("[OK] session %s -> %s", sid, addr))
			s := transport.NewSession(sid)
			s.TargetAddr = addr
			engine.AddSession(s)
			s.EnqueueTx(nil)
			return transport.NewVirtualConn(s, engine), nil
		}),
		socks5.WithAssociateHandle(func(ctx context.Context, w io.Writer, req *socks5.Request) error {
			pc, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				socks5.SendReply(w, statute.RepCommandNotSupported, nil)
				return err
			}
			bindAddr := pc.LocalAddr().(*net.UDPAddr)
			socks5.SendReply(w, statute.RepSuccess, &net.TCPAddr{
				IP: net.ParseIP("127.0.0.1"), Port: bindAddr.Port,
			})
			go func() { defer pc.Close(); time.Sleep(5 * time.Minute) }()
			return nil
		}),
	)

	appendLog(fmt.Sprintf("[INFO] SOCKS5 listening on %s", listenAddr))

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe("tcp", listenAddr)
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

func main() {}
