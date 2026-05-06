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
	logFilePath  string
	logBuf       []string
	logBufMu     sync.Mutex
)

// اضافه لاگ به فایل و buffer همزمان
func lg(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	ts := time.Now().Format("15:04:05")
	line := fmt.Sprintf("[%s] %s", ts, msg)

	logBufMu.Lock()
	logBuf = append(logBuf, line)
	if len(logBuf) > 2000 {
		logBuf = logBuf[len(logBuf)-2000:]
	}
	logBufMu.Unlock()

	// نوشتن به فایل — persist می‌شه حتی بعد از crash
	if logFile != nil {
		logFile.WriteString(line + "\n")
		logFile.Sync()
	}
	os.Stderr.WriteString(line + "\n")
}

func initLog(filesDir string) {
	logFilePath = filesDir + "/flowdriver_debug.log"
	f, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err == nil {
		logFile = f
	}
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

// ── JNI exports ───────────────────────────────────────────────────────────────

//export Java_com_flowdriver_service_FlowBridge_startTunnel
func Java_com_flowdriver_service_FlowBridge_startTunnel(
	env uintptr, obj uintptr,
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

	// init log در همون دایرکتوری فایل‌ها
	dir := credFile[:strings.LastIndex(credFile, "/")]
	initLog(dir)

	ctx, cancel := context.WithCancel(context.Background())
	globalCancel = cancel
	running = true
	globalMu.Unlock()

	err := runClient(ctx, configJson, credFile, tokenFile)

	globalMu.Lock()
	running = false
	globalMu.Unlock()

	if logFile != nil {
		logFile.Close()
	}

	if err != nil {
		lg("[FATAL] %v", err)
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

//export Java_com_flowdriver_service_FlowBridge_getLog
func Java_com_flowdriver_service_FlowBridge_getLog(env uintptr, obj uintptr) *C.char {
	logBufMu.Lock()
	defer logBufMu.Unlock()
	if len(logBuf) == 0 {
		return C.CString("")
	}
	result := strings.Join(logBuf, "\n")
	logBuf = nil
	return C.CString(result)
}

// getLogFile: مسیر فایل لاگ رو برمی‌گردونه — Kotlin می‌تونه بعد از crash بخوندش
//
//export Java_com_flowdriver_service_FlowBridge_getLogFilePath
func Java_com_flowdriver_service_FlowBridge_getLogFilePath(env uintptr, obj uintptr) *C.char {
	return C.CString(logFilePath)
}

// ── core ──────────────────────────────────────────────────────────────────────

func runClient(ctx context.Context, configJson, credFilePath, tokenFilePath string) error {
	lg("========== FlowDriver Start ==========")
	lg("credFile: %s", credFilePath)
	lg("tokenFile: %s", tokenFilePath)

	// ── ۱. بررسی فایل‌ها ─────────────────────────────────────────────────────
	lg("[1] Reading credentials.json...")
	credData, err := os.ReadFile(credFilePath)
	if err != nil {
		return fmt.Errorf("read credentials.json: %w", err)
	}
	lg("    size=%d bytes", len(credData))

	var cred struct {
		Installed struct {
			ClientID     string `json:"client_id"`
			ClientSecret string `json:"client_secret"`
			TokenURI     string `json:"token_uri"`
		} `json:"installed"`
	}
	if err := json.Unmarshal(credData, &cred); err != nil {
		return fmt.Errorf("parse credentials.json: %w", err)
	}
	lg("    client_id=%s", safe(cred.Installed.ClientID, 20))
	lg("    client_secret=%s", safe(cred.Installed.ClientSecret, 6))

	lg("[2] Reading token file...")
	tokenData, err := os.ReadFile(tokenFilePath)
	if err != nil {
		return fmt.Errorf("read token file: %w", err)
	}
	lg("    size=%d bytes", len(tokenData))

	var tc struct {
		RefreshToken string `json:"refresh_token"`
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
	}
	if err := json.Unmarshal(tokenData, &tc); err != nil {
		return fmt.Errorf("parse token file: %w", err)
	}
	lg("    refresh_token=%s", safe(tc.RefreshToken, 25))
	if tc.ClientID != "" {
		lg("    has embedded client_id=YES")
		cred.Installed.ClientID = tc.ClientID
		cred.Installed.ClientSecret = tc.ClientSecret
	} else {
		lg("    has embedded client_id=NO (using credentials.json)")
	}

	tokenURI := cred.Installed.TokenURI
	if tokenURI == "" {
		tokenURI = "https://oauth2.googleapis.com/token"
	}

	// ── ۳. مستقیم OAuth تست کن ───────────────────────────────────────────────
	lg("[3] Direct OAuth test to: %s", tokenURI)

	plainClient := &http.Client{Timeout: 30 * time.Second}

	v := url.Values{}
	v.Set("grant_type", "refresh_token")
	v.Set("refresh_token", tc.RefreshToken)
	v.Set("client_id", cred.Installed.ClientID)
	v.Set("client_secret", cred.Installed.ClientSecret)

	req, err := http.NewRequestWithContext(ctx, "POST", tokenURI, strings.NewReader(v.Encode()))
	if err != nil {
		return fmt.Errorf("create oauth request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	lg("    Sending request...")
	resp, err := plainClient.Do(req)
	if err != nil {
		return fmt.Errorf("oauth http error: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	lg("    Status: %d", resp.StatusCode)
	lg("    Body: %s", string(body))

	if resp.StatusCode != 200 {
		return fmt.Errorf("oauth failed %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	json.Unmarshal(body, &tokenResp)
	lg("    access_token=%s (expires_in=%d)", safe(tokenResp.AccessToken, 10), tokenResp.ExpiresIn)

	// ── ۴. storage backend ────────────────────────────────────────────────────
	lg("[4] Setting up storage backend...")
	cfg, err := config.FromJSON(configJson)
	if err != nil {
		return fmt.Errorf("config parse: %w", err)
	}
	lg("    folder=%s transport=%s", cfg.GoogleFolderID, cfg.Transport.TargetIP)

	backend := storage.NewGoogleBackendWithToken(plainClient, credFilePath, tokenFilePath, cfg.GoogleFolderID)
	loginCtx, loginCancel := context.WithTimeout(ctx, 30*time.Second)
	defer loginCancel()

	if err := backend.Login(loginCtx); err != nil {
		return fmt.Errorf("backend login: %w", err)
	}
	lg("    backend login OK")

	customClient := httpclient.NewCustomClient(cfg.Transport)
	driveBackend := storage.NewGoogleBackendWithToken(customClient, credFilePath, tokenFilePath, cfg.GoogleFolderID)
	driveBackend.CopyTokenFrom(backend)

	if cfg.GoogleFolderID == "" {
		id, _ := driveBackend.FindFolder(ctx, "Flow-Data")
		if id == "" {
			id, _ = driveBackend.CreateFolder(ctx, "Flow-Data")
		}
		cfg.GoogleFolderID = id
		lg("    folder found/created: %s", id)
	}

	// ── ۵. engine + SOCKS5 ────────────────────────────────────────────────────
	lg("[5] Starting engine...")
	cid := genID()[:8]
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
			lg("[SOCKS] %s -> %s", sid[:8], addr)
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

	lg("[6] SOCKS5 on %s — READY", listenAddr)
	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.ListenAndServe("tcp", listenAddr) }()

	select {
	case <-ctx.Done():
		lg("[INFO] stopped by context")
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
