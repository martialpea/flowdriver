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

// ── log buffer ────────────────────────────────────────────────────────────────
var (
	globalCancel context.CancelFunc
	globalMu     sync.Mutex
	running      bool
	logBuf       []string
	logBufMu     sync.Mutex
)

func init() {
	log.SetOutput(&logWriter{})
	log.SetFlags(log.Ltime | log.Lshortfile)
}

type logWriter struct{}

func (w *logWriter) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\n")
	logBufMu.Lock()
	logBuf = append(logBuf, msg)
	if len(logBuf) > 1000 {
		logBuf = logBuf[len(logBuf)-1000:]
	}
	logBufMu.Unlock()
	os.Stderr.Write(p)
	return len(p), nil
}

func appendLog(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	logBufMu.Lock()
	logBuf = append(logBuf, msg)
	logBufMu.Unlock()
	os.Stderr.WriteString(msg + "\n")
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
	ctx, cancel := context.WithCancel(context.Background())
	globalCancel = cancel
	running = true
	globalMu.Unlock()

	err := runClient(ctx, configJson, credFile, tokenFile)

	globalMu.Lock()
	running = false
	globalMu.Unlock()

	if err != nil {
		appendLog("[FATAL] %v", err)
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

// ── core ──────────────────────────────────────────────────────────────────────

func runClient(ctx context.Context, configJson, credFilePath, tokenFilePath string) error {

	// ── ۱. بررسی فایل‌ها ─────────────────────────────────────────────────────
	appendLog("[STEP1] Checking files...")

	credData, err := os.ReadFile(credFilePath)
	if err != nil {
		return fmt.Errorf("cannot read credentials.json: %w", err)
	}
	appendLog("[FILE] credentials.json: %d bytes", len(credData))

	tokenData, err := os.ReadFile(tokenFilePath)
	if err != nil {
		return fmt.Errorf("cannot read token file: %w", err)
	}
	appendLog("[FILE] token file: %d bytes", len(tokenData))

	// بررسی محتوای token
	var tokenCheck map[string]interface{}
	if err := json.Unmarshal(tokenData, &tokenCheck); err != nil {
		return fmt.Errorf("token file invalid JSON: %w", err)
	}
	appendLog("[FILE] token keys: %v", keys(tokenCheck))

	// بررسی محتوای credentials
	var credCheck map[string]interface{}
	if err := json.Unmarshal(credData, &credCheck); err != nil {
		return fmt.Errorf("credentials.json invalid JSON: %w", err)
	}
	appendLog("[FILE] cred keys: %v", keys(credCheck))

	// ── ۲. parse config ───────────────────────────────────────────────────────
	appendLog("[STEP2] Parsing config...")
	cfg, err := config.FromJSON(configJson)
	if err != nil {
		return fmt.Errorf("config parse: %w", err)
	}
	appendLog("[CFG] folder=%s refresh=%d flush=%d transport=%s",
		cfg.GoogleFolderID, cfg.RefreshRateMs, cfg.FlushRateMs, cfg.Transport.TargetIP)

	// ── ۳. تست مستقیم OAuth ───────────────────────────────────────────────────
	appendLog("[STEP3] Testing OAuth directly (no DPI bypass)...")

	plainClient := &http.Client{Timeout: 30 * time.Second}
	oauthErr := testOAuth(plainClient, credFilePath, tokenFilePath)
	if oauthErr != nil {
		return fmt.Errorf("OAuth test failed: %w", oauthErr)
	}
	appendLog("[OAUTH] OK!")

	// ── ۴. login با storage backend ───────────────────────────────────────────
	appendLog("[STEP4] Login via storage backend...")
	backend := storage.NewGoogleBackendWithToken(plainClient, credFilePath, tokenFilePath, cfg.GoogleFolderID)

	loginCtx, loginCancel := context.WithTimeout(ctx, 30*time.Second)
	defer loginCancel()

	if err := backend.Login(loginCtx); err != nil {
		return fmt.Errorf("backend login: %w", err)
	}
	appendLog("[LOGIN] Backend login OK")

	// ── ۵. Drive API ──────────────────────────────────────────────────────────
	appendLog("[STEP5] Setting up Drive backend...")
	customClient := httpclient.NewCustomClient(cfg.Transport)
	driveBackend := storage.NewGoogleBackendWithToken(customClient, credFilePath, tokenFilePath, cfg.GoogleFolderID)
	driveBackend.CopyTokenFrom(backend)

	if cfg.GoogleFolderID == "" {
		id, _ := driveBackend.FindFolder(ctx, "Flow-Data")
		if id == "" {
			id, _ = driveBackend.CreateFolder(ctx, "Flow-Data")
		}
		cfg.GoogleFolderID = id
		appendLog("[DRIVE] folder: %s", id)
	}

	// ── ۶. engine ─────────────────────────────────────────────────────────────
	appendLog("[STEP6] Starting engine...")
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

	// ── ۷. SOCKS5 ─────────────────────────────────────────────────────────────
	listenAddr := cfg.ListenAddr
	if listenAddr == "" {
		listenAddr = "127.0.0.1:1080"
	}

	srv := socks5.NewServer(
		socks5.WithResolver(rawResolver{}),
		socks5.WithDial(func(dc context.Context, network, addr string) (net.Conn, error) {
			sid := genID()
			appendLog("[SOCKS] %s -> %s", sid[:8], addr)
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

	appendLog("[STEP7] SOCKS5 listening on %s", listenAddr)
	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.ListenAndServe("tcp", listenAddr) }()

	select {
	case <-ctx.Done():
		appendLog("[INFO] stopped")
		return nil
	case err := <-srvErr:
		return fmt.Errorf("socks5: %w", err)
	}
}

// testOAuth: مستقیم OAuth رو تست می‌کنه بدون storage backend
func testOAuth(client *http.Client, credFilePath, tokenFilePath string) error {
	// خواندن client_id و client_secret
	var clientID, clientSecret, tokenURI string
	if data, err := os.ReadFile(credFilePath); err == nil {
		var cred struct {
			Installed struct {
				ClientID     string `json:"client_id"`
				ClientSecret string `json:"client_secret"`
				TokenURI     string `json:"token_uri"`
			} `json:"installed"`
		}
		if json.Unmarshal(data, &cred) == nil {
			clientID = cred.Installed.ClientID
			clientSecret = cred.Installed.ClientSecret
			tokenURI = cred.Installed.TokenURI
		}
	}

	// خواندن refresh_token
	var refreshToken string
	if data, err := os.ReadFile(tokenFilePath); err == nil {
		var tc struct {
			RefreshToken string `json:"refresh_token"`
			ClientID     string `json:"client_id"`
			ClientSecret string `json:"client_secret"`
		}
		if json.Unmarshal(data, &tc) == nil {
			refreshToken = tc.RefreshToken
			if tc.ClientID != "" {
				clientID = tc.ClientID
				clientSecret = tc.ClientSecret
			}
		}
	}

	if tokenURI == "" {
		tokenURI = "https://oauth2.googleapis.com/token"
	}

	appendLog("[OAUTH] client_id=%s", safeStr(clientID, 20))
	appendLog("[OAUTH] client_secret=%s", safeStr(clientSecret, 8))
	appendLog("[OAUTH] refresh_token=%s", safeStr(refreshToken, 25))
	appendLog("[OAUTH] token_uri=%s", tokenURI)

	if clientID == "" {
		return fmt.Errorf("client_id is empty")
	}
	if clientSecret == "" {
		return fmt.Errorf("client_secret is empty")
	}
	if refreshToken == "" {
		return fmt.Errorf("refresh_token is empty")
	}

	v := url.Values{}
	v.Set("grant_type", "refresh_token")
	v.Set("refresh_token", refreshToken)
	v.Set("client_id", clientID)
	v.Set("client_secret", clientSecret)

	appendLog("[OAUTH] Sending token request to %s...", tokenURI)

	req, err := http.NewRequest("POST", tokenURI, strings.NewReader(v.Encode()))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	appendLog("[OAUTH] Response status: %d", resp.StatusCode)
	appendLog("[OAUTH] Response body: %s", string(body))

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("token refresh failed %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func keys(m map[string]interface{}) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

func safeStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func main() {}
