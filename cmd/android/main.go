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
	"path/filepath"
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
	logFiles     []*os.File // لاگ در چند مکان
)

// lg: نوشتن به همه فایل‌های لاگ
func lg(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	ts := time.Now().Format("15:04:05.000")
	line := ts + " " + msg + "\n"
	for _, f := range logFiles {
		if f != nil {
			f.WriteString(line)
			f.Sync()
		}
	}
}

// openLog: باز کردن فایل لاگ در چند مکان مختلف
func openLog(filesDir string) {
	logFiles = nil

	// مکان ۱: filesDir (همیشه قابل نوشتن)
	f1, err := os.OpenFile(
		filepath.Join(filesDir, "fd_debug.log"),
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644,
	)
	if err == nil {
		logFiles = append(logFiles, f1)
	}

	// مکان ۲: پوشه Downloads عمومی
	downloads := "/sdcard/Download"
	if _, err := os.Stat(downloads); err == nil {
		f2, err := os.OpenFile(
			filepath.Join(downloads, "flowdriver_debug.log"),
			os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644,
		)
		if err == nil {
			logFiles = append(logFiles, f2)
		}
	}
}

func closeLog() {
	for _, f := range logFiles {
		if f != nil {
			f.Close()
		}
	}
	logFiles = nil
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
	env uintptr, obj uintptr,
	configJsonC *C.char,
	credFileC *C.char,
	tokenFileC *C.char,
) int32 {
	credFileStr := C.GoString(credFileC)
	filesDir := credFileStr[:strings.LastIndex(credFileStr, "/")]

	// اول از همه لاگ رو باز کن
	openLog(filesDir)
	lg("=== JNI startTunnel called ===")
	lg("filesDir: %s", filesDir)

	globalMu.Lock()
	if running {
		globalMu.Unlock()
		lg("already running!")
		closeLog()
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
		closeLog()
		return -2
	}

	lg("=== done OK ===")
	closeLog()
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
	lg("--- runClient start ---")
	lg("cred: %s", credFilePath)
	lg("token: %s", tokenFilePath)

	// ۱. خواندن فایل‌ها
	lg("[1] Reading credential file...")
	credData, err := os.ReadFile(credFilePath)
	if err != nil {
		return fmt.Errorf("read cred: %w", err)
	}
	lg("    OK: %d bytes", len(credData))

	lg("[2] Reading token file...")
	tokenData, err := os.ReadFile(tokenFilePath)
	if err != nil {
		return fmt.Errorf("read token: %w", err)
	}
	lg("    OK: %d bytes", len(tokenData))

	// ۲. parse
	lg("[3] Parsing credentials...")
	var cred struct {
		Installed struct {
			ClientID     string `json:"client_id"`
			ClientSecret string `json:"client_secret"`
			TokenURI     string `json:"token_uri"`
		} `json:"installed"`
	}
	if err := json.Unmarshal(credData, &cred); err != nil {
		return fmt.Errorf("parse cred JSON: %w", err)
	}
	lg("    client_id: %s", safe(cred.Installed.ClientID, 20))

	lg("[4] Parsing token...")
	var tc struct {
		RefreshToken string `json:"refresh_token"`
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
	}
	if err := json.Unmarshal(tokenData, &tc); err != nil {
		return fmt.Errorf("parse token JSON: %w", err)
	}
	lg("    refresh_token: %s", safe(tc.RefreshToken, 20))

	if tc.ClientID != "" {
		lg("    using client_id from token file")
		cred.Installed.ClientID = tc.ClientID
		cred.Installed.ClientSecret = tc.ClientSecret
	}

	tokenURI := cred.Installed.TokenURI
	if tokenURI == "" {
		tokenURI = "https://oauth2.googleapis.com/token"
	}

	if cred.Installed.ClientID == "" {
		return fmt.Errorf("client_id is empty — check credentials.json")
	}
	if cred.Installed.ClientSecret == "" {
		return fmt.Errorf("client_secret is empty — check credentials.json")
	}
	if tc.RefreshToken == "" {
		return fmt.Errorf("refresh_token is empty — check token file")
	}

	// ۳. OAuth
	lg("[5] OAuth token refresh to: %s", tokenURI)
	plainClient := &http.Client{Timeout: 30 * time.Second}

	v := url.Values{}
	v.Set("grant_type", "refresh_token")
	v.Set("refresh_token", tc.RefreshToken)
	v.Set("client_id", cred.Installed.ClientID)
	v.Set("client_secret", cred.Installed.ClientSecret)

	req, err := http.NewRequestWithContext(ctx, "POST", tokenURI,
		strings.NewReader(v.Encode()))
	if err != nil {
		return fmt.Errorf("create oauth request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := plainClient.Do(req)
	if err != nil {
		return fmt.Errorf("oauth network error: %w", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	lg("    HTTP status: %d", resp.StatusCode)
	lg("    Response: %s", string(body))

	if resp.StatusCode != 200 {
		return fmt.Errorf("oauth failed %d: %s", resp.StatusCode, string(body))
	}
	lg("    OAuth OK!")

	// ۴. backend
	lg("[6] Setting up storage backend...")
	cfg, err := config.FromJSON(configJson)
	if err != nil {
		return fmt.Errorf("config parse: %w", err)
	}
	lg("    folder_id: %s", cfg.GoogleFolderID)
	lg("    transport.TargetIP: %s", cfg.Transport.TargetIP)

	backend := storage.NewGoogleBackendWithToken(
		plainClient, credFilePath, tokenFilePath, cfg.GoogleFolderID,
	)
	loginCtx, loginCancel := context.WithTimeout(ctx, 30*time.Second)
	defer loginCancel()

	if err := backend.Login(loginCtx); err != nil {
		return fmt.Errorf("backend login: %w", err)
	}
	lg("    Backend login OK")

	customClient := httpclient.NewCustomClient(cfg.Transport)
	driveBackend := storage.NewGoogleBackendWithToken(
		customClient, credFilePath, tokenFilePath, cfg.GoogleFolderID,
	)
	driveBackend.CopyTokenFrom(backend)

	if cfg.GoogleFolderID == "" {
		lg("[7] Finding/creating Drive folder...")
		id, _ := driveBackend.FindFolder(ctx, "Flow-Data")
		if id == "" {
			id, _ = driveBackend.CreateFolder(ctx, "Flow-Data")
		}
		cfg.GoogleFolderID = id
		lg("    folder: %s", id)
	}

	// ۵. engine
	lg("[8] Starting engine...")
	cid := genID()[:8]
	engine := transport.NewEngine(driveBackend, true, cid)
	if cfg.RefreshRateMs > 0 {
		engine.SetPollRate(cfg.RefreshRateMs)
	}
	if cfg.FlushRateMs > 0 {
		engine.SetFlushRate(cfg.FlushRateMs)
	}
	engine.Start(ctx)
	lg("    Engine started with client_id=%s", cid)

	// ۶. SOCKS5
	listenAddr := cfg.ListenAddr
	if listenAddr == "" {
		listenAddr = "127.0.0.1:1080"
	}

	srv := socks5.NewServer(
		socks5.WithResolver(rawResolver{}),
		socks5.WithDial(func(dc context.Context, network, addr string) (net.Conn, error) {
			sid := genID()
			lg("[SOCKS] %s -> %s", sid[:6], addr)
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

	lg("[9] SOCKS5 listening on %s...", listenAddr)
	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.ListenAndServe("tcp", listenAddr) }()

	lg("=== READY — tunnel is up ===")

	select {
	case <-ctx.Done():
		lg("context cancelled — shutting down")
		return nil
	case err := <-srvErr:
		return fmt.Errorf("socks5 server error: %w", err)
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
