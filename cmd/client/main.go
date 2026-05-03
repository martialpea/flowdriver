package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/NullLatency/flow-driver/internal/config"
	"github.com/NullLatency/flow-driver/internal/httpclient"
	"github.com/NullLatency/flow-driver/internal/storage"
	"github.com/NullLatency/flow-driver/internal/transport"
	"github.com/things-go/go-socks5"
	"github.com/things-go/go-socks5/statute"
)

func genID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// rawResolver: DNS رو به سرور می‌فرستیم — جلوگیری کامل از DNS leak
type rawResolver struct{}

func (rawResolver) Resolve(ctx context.Context, name string) (context.Context, net.IP, error) {
	return ctx, nil, nil
}

func main() {
	var configPath, gcPath string
	flag.StringVar(&configPath, "c", "config.json", "Path to config file")
	flag.StringVar(&gcPath, "gc", "credentials.json", "Path to Google credentials JSON")
	flag.Parse()

	log.Println("[client] starting...")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	appCfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("[client] config: %v", err)
	}

	backend, err := initBackend(ctx, appCfg, gcPath)
	if err != nil {
		log.Fatalf("[client] backend: %v", err)
	}

	appCfg = autoFolder(ctx, appCfg, backend, configPath)

	cid := appCfg.ClientID
	if cid == "" {
		cid = genID()[:8]
	}

	engine := transport.NewEngine(backend, true, cid)
	if appCfg.RefreshRateMs > 0 {
		engine.SetPollRate(appCfg.RefreshRateMs)
	}
	if appCfg.FlushRateMs > 0 {
		engine.SetFlushRate(appCfg.FlushRateMs)
	}
	engine.Start(ctx)

	listenAddr := appCfg.ListenAddr
	if listenAddr == "" {
		listenAddr = "127.0.0.1:1080"
	}

	srv := socks5.NewServer(
		socks5.WithResolver(rawResolver{}),

		socks5.WithDial(func(dc context.Context, network, addr string) (net.Conn, error) {
			sid := genID()
			host, port, splitErr := net.SplitHostPort(addr)
			if splitErr == nil {
				if net.ParseIP(host) != nil {
					log.Printf("[WARN] DNS leak: session %s got raw IP %s:%s", sid, host, port)
				} else {
					log.Printf("[OK] session %s -> %s:%s", sid, host, port)
				}
			}

			s := transport.NewSession(sid)
			s.TargetAddr = addr
			engine.AddSession(s)
			s.EnqueueTx(nil)
			return transport.NewVirtualConn(s, engine), nil
		}),

		// FIX: UDP associate — جلوگیری از hang تلگرام / واتساپ
		// این برنامه‌ها برای تشخیص سرعت و voice از SOCKS5 UDP استفاده می‌کنن
		// اگه "command not supported" برگردونیم، کلاینت hang می‌کنه
		// راه‌حل: یه UDP socket موقت باز می‌کنیم و موفقیت اعلام می‌کنیم
		// داده واقعی UDP از tunnel نمی‌گذره ولی app دیگه hang نمی‌کنه
		socks5.WithAssociateHandle(func(ctx context.Context, w io.Writer, req *socks5.Request) error {
			pc, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				socks5.SendReply(w, statute.RepCommandNotSupported, nil)
				return fmt.Errorf("udp listen failed: %v", err)
			}
			bindAddr := pc.LocalAddr().(*net.UDPAddr)
			if err := socks5.SendReply(w, statute.RepSuccess, &net.TCPAddr{
				IP:   net.ParseIP("127.0.0.1"),
				Port: bindAddr.Port,
			}); err != nil {
				pc.Close()
				return err
			}
			// socket رو تا بسته شدن TCP control connection نگه دار
			go func() {
				defer pc.Close()
				time.Sleep(5 * time.Minute)
			}()
			return nil
		}),
	)

	log.Printf("[client] SOCKS5 listening on %s", listenAddr)
	go func() {
		if err := srv.ListenAndServe("tcp", listenAddr); err != nil {
			log.Fatalf("[client] socks5: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("[client] shutting down...")
	cancel()
}

func initBackend(ctx context.Context, cfg *config.AppConfig, gcPath string) (storage.Backend, error) {
	var backend storage.Backend
	var err error
	if cfg.StorageType == "google" {
		hc := httpclient.NewCustomClient(cfg.Transport)
		backend = storage.NewGoogleBackend(hc, gcPath, cfg.GoogleFolderID)
	} else {
		backend, err = storage.NewLocalBackend(cfg.LocalDir)
		if err != nil {
			return nil, err
		}
	}
	if err := backend.Login(ctx); err != nil {
		return nil, err
	}
	return backend, nil
}

func autoFolder(ctx context.Context, cfg *config.AppConfig, backend storage.Backend, cfgPath string) *config.AppConfig {
	if cfg.StorageType != "google" || cfg.GoogleFolderID != "" {
		return cfg
	}
	log.Println("[client] searching for Drive folder 'Flow-Data'...")
	id, err := backend.FindFolder(ctx, "Flow-Data")
	if err != nil {
		log.Printf("[client] find folder: %v", err)
		return cfg
	}
	if id == "" {
		log.Println("[client] creating Drive folder 'Flow-Data'...")
		id, err = backend.CreateFolder(ctx, "Flow-Data")
		if err != nil {
			log.Printf("[client] create folder: %v", err)
			return cfg
		}
	}
	cfg.GoogleFolderID = id
	if err := cfg.Save(cfgPath); err != nil {
		log.Printf("[client] save config: %v", err)
	}
	return cfg
}
