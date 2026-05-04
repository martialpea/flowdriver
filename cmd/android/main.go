package main

/*
#cgo CFLAGS: -Wno-unused-parameter
#include <jni.h>
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

//export Java_com_flowdriver_service_FlowBridge_start
func Java_com_flowdriver_service_FlowBridge_start(
	env *C.JNIEnv,
	obj C.jobject,
	configJsonC C.jstring,
	tokenJsonC C.jstring,
	credFileC C.jstring,
) C.jint {
	globalMu.Lock()
	defer globalMu.Unlock()

	if running {
		return C.jint(-1)
	}

	// تبدیل jstring به Go string
	cConfigJson := C.GetStringUTFChars(env, configJsonC, nil)
	cTokenJson  := C.GetStringUTFChars(env, tokenJsonC, nil)
	cCredFile   := C.GetStringUTFChars(env, credFileC, nil)

	configJson := C.GoString(cConfigJson)
	tokenJson  := C.GoString(cTokenJson)
	credFile   := C.GoString(cCredFile)

	C.ReleaseStringUTFChars(env, configJsonC, cConfigJson)
	C.ReleaseStringUTFChars(env, tokenJsonC, cTokenJson)
	C.ReleaseStringUTFChars(env, credFileC, cCredFile)

	ctx, cancel := context.WithCancel(context.Background())
	globalCancel = cancel
	running = true

	go func() {
		defer func() {
			globalMu.Lock()
			running = false
			globalMu.Unlock()
			log.Println("[INFO] FlowDriver stopped")
		}()
		if err := runClient(ctx, configJson, tokenJson, credFile); err != nil {
			log.Printf("[ERROR] %v", err)
		}
	}()

	return C.jint(0)
}

//export Java_com_flowdriver_service_FlowBridge_stop
func Java_com_flowdriver_service_FlowBridge_stop(env *C.JNIEnv, obj C.jobject) {
	globalMu.Lock()
	defer globalMu.Unlock()
	if globalCancel != nil {
		globalCancel()
		globalCancel = nil
	}
	running = false
}

//export Java_com_flowdriver_service_FlowBridge_isRunning
func Java_com_flowdriver_service_FlowBridge_isRunning(env *C.JNIEnv, obj C.jobject) C.jint {
	globalMu.Lock()
	defer globalMu.Unlock()
	if running {
		return C.jint(1)
	}
	return C.jint(0)
}

func runClient(ctx context.Context, configJson, tokenJson, credFilePath string) error {
	cfg, err := config.FromJSON(configJson)
	if err != nil {
		return fmt.Errorf("config parse: %w", err)
	}

	hc := httpclient.NewCustomClient(cfg.Transport)
	backend := storage.NewGoogleBackend(hc, credFilePath, cfg.GoogleFolderID)

	loginCtx, loginCancel := context.WithTimeout(ctx, 30*time.Second)
	defer loginCancel()

	if err := backend.Login(loginCtx); err != nil {
		return fmt.Errorf("login: %w", err)
	}
	log.Println("[INFO] Logged in to Google Drive")

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
			log.Printf("[OK] session %s -> %s", sid, addr)
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
			bindAddr := pc.LocalAddr().(*net.UDPAddr)
			socks5.SendReply(w, statute.RepSuccess, &net.TCPAddr{
				IP:   net.ParseIP("127.0.0.1"),
				Port: bindAddr.Port,
			})
			go func() {
				defer pc.Close()
				time.Sleep(5 * time.Minute)
			}()
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
