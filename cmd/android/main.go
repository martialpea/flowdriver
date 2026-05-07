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
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
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
	logFile      *os.File
)

func lg(format string, args ...interface{}) {
	if logFile == nil {
		return
	}
	msg := fmt.Sprintf(format, args...)
	ts := time.Now().Format("15:04:05")
	logFile.WriteString(ts + " " + msg + "\n")
	logFile.Sync()
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

// FIX: اول فایل لاگ رو باز کن — قبل از هر چیز دیگه
//
//export Java_com_flowdriver_service_FlowBridge_startTunnel
func Java_com_flowdriver_service_FlowBridge_startTunnel(
	env uintptr, obj uintptr,
	configJsonC *C.char,
	credFileC *C.char,
	tokenFileC *C.char,
) int32 {
	// اول از همه فایل لاگ رو باز کن
	credFileStr := C.GoString(credFileC)
	logPath := credFileStr[:strings.LastIndex(credFileStr, "/")] + "/fd_debug.log"
	f, _ := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	logFile = f
	lg("JNI startTunnel called")

	globalMu.Lock()
	if running {
		globalMu.Unlock()
		lg("already running")
		return -1
	}

	configJson := C.GoString(configJsonC)
	tokenFile  := C.GoString(tokenFileC)

	ctx, cancel := context.WithCancel(context.Background())
	globalCancel = cancel
	running = true
	globalMu.Unlock()

	lg("calling runClient...")
	runErr := runClient(ctx, configJson, credFileStr, tokenFile)

	globalMu.Lock()
	running = false
	globalMu.Unlock()

	if runErr != nil {
		lg("FATAL: %v", runErr)
		if logFile != nil {
			logFile.Close()
			logFile = nil
		}
		return -2
	}

	lg("done OK")
	if logFile != nil {
		logFile.Close()
		logFile = nil
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
	lg("=== runClient start ===")

	lg("reading cred file...")
	credData, err := os.ReadFile(credFilePath)
	if err != nil {
		return fmt.Errorf("read cred: %w", err)
	}
	lg("cred: %d bytes", len(credData))

	lg("reading token file...")
	tokenData, err := os.ReadFile(tokenFilePath)
	if err != nil {
		return fmt.Errorf("read token: %w", err)
	}
	lg("token: %d bytes", len(tokenData))

	var cred struct {
		Installed struct {
			ClientID     string `json:"client_id"`
			ClientSecret string `json:"client_secret"`
			TokenURI     string `json:"token_uri"`
		} `json:"installed"`
	}
	if err := json.Unmarshal(credData, &cred); err != nil {
		return fmt.Errorf("parse cred: %w", err)
	}

	var tc struct {
		RefreshToken string `json:"refresh_token"`
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
	}
	if err := json.Unmarshal(tokenData, &tc); err != nil {
		return fmt.Errorf("parse token: %w", err)
	}

	if tc.ClientID != "" {
		cred.Installed.ClientID = tc.ClientID
		cred.Installed.ClientSecret = tc.ClientSecret
	}
	tokenURI := cred.Installed.TokenURI
	if tokenURI == "" {
		tokenURI = "https://oauth2.googleapis.com/token"
	}

	lg("client_id=%s", safe(cred.Installed.ClientID, 20))
	lg("refresh_token=%s", safe(tc.RefreshToken, 20))
	lg("token_uri=%s", tokenURI)

	if cred.Installed.ClientID == "" {
		return fmt.Errorf("client_id empty")
	}
	if cred.Installed.ClientSecret == "" {
		return fmt.Errorf("client_secret empty")
	}
	if tc.RefreshToken == "" {
		return fmt.Errorf("refresh_token empty")
	}

	lg("sending OAuth request...")
	plainClient := &http.Client{Timeout: 30 * time.Second}

	v := url.Values{}
	v.Set("grant_type", "refresh_token")
	v.Set("refresh_token", tc.RefreshToken)
	v.Set("client_id", cred.Installed.ClientID)
	v.Set("client_secret", cred.Installed.ClientSecret)

	req, err := http.NewRequestWithContext(ctx, "POST", tokenURI,
		strings.NewReader(v.Encode()))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := plainClient.Do(req)
	if err != nil {
		return fmt.Errorf("oauth http: %w", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	lg("oauth status=%d", resp.StatusCode)
	lg("oauth body=%s", string(body))

	if resp.StatusCode != 200 {
		return fmt.Errorf("oauth failed %d: %s", resp.StatusCode, string(body))
	}
	lg("OAuth OK!")

	lg("setting up backend...")
	cfg, err := config.FromJSON(configJson)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	lg("folder=%s", cfg.GoogleFolderID)

	backend := storage.NewGoogleBackendWithToken(plainClient, credFilePath, tokenFilePath, cfg.GoogleFolderID)
	loginCtx, loginCancel := context.WithTimeout(ctx, 30*time.Second)
	defer loginCancel()
	if err := backend.Login(loginCtx); err != nil {
		return fmt.Errorf("login: %w", err)
	}
	lg("login OK")

	customClient := httpclient.NewCustomClient(cfg.Transport)
	driveBackend := storage.NewGoogleBackendWithToken(customClient, credFilePath, tokenFilePath, cfg.GoogleFolderID)
	driveBackend.CopyTokenFrom(backend)

	if cfg.GoogleFolderID == "" {
		id, _ := driveBackend.FindFolder(ctx, "Flow-Data")
		if id == "" {
			id, _ = driveBackend.CreateFolder(ctx, "Flow-Data")
		}
		cfg.GoogleFolderID = id
		lg("folder: %s", id)
	}

	cid := genID()[:8]
	engine := transport.NewEngine(driveBackend, true, cid)
	if cfg.RefreshRateMs > 0 {
		engine.SetPollRate(cfg.RefreshRateMs)
	}
	if cfg.FlushRateMs > 0 {
		engine.SetFlushRate(cfg.FlushRateMs)
	}
	engine.Start(ctx)
	lg("engine started")

	listenAddr := cfg.ListenAddr
	if listenAddr == "" {
		listenAddr = "127.0.0.1:1080"
	}

	srv := socks5.NewServer(
		socks5.WithResolver(rawResolver{}),
		socks5.WithDial(func(dc context.Context, network, addr string) (net.Conn, error) {
			sid := genID()
			lg("SOCKS %s->%s", sid[:6], addr)
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

	lg("SOCKS5 on %s READY", listenAddr)
	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.ListenAndServe("tcp", listenAddr) }()

	select {
	case <-ctx.Done():
		lg("stopped")
		return nil
	case err := <-srvErr:
		return fmt.Errorf("socks5: %w", err)
	}
}

func safe(s string, n int) string {
	if s == "" {
		return "(empty)"
	}
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func main() {}
