package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/NullLatency/flow-driver/internal/config"
	"github.com/NullLatency/flow-driver/internal/httpclient"
	"github.com/NullLatency/flow-driver/internal/storage"
	"github.com/NullLatency/flow-driver/internal/transport"
)

func main() {
	var configPath, gcPath string
	flag.StringVar(&configPath, "c", "config.json", "Path to config file")
	flag.StringVar(&gcPath, "gc", "credentials.json", "Path to Google credentials JSON")
	flag.Parse()

	log.Println("[server] starting...")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	appCfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("[server] config: %v", err)
	}

	var backend storage.Backend
	if appCfg.StorageType == "google" {
		hc := httpclient.NewCustomClient(appCfg.Transport)
		backend = storage.NewGoogleBackend(hc, gcPath, appCfg.GoogleFolderID)
	} else {
		backend, err = storage.NewLocalBackend(appCfg.LocalDir)
		if err != nil {
			log.Fatalf("[server] local backend: %v", err)
		}
	}
	if err := backend.Login(ctx); err != nil {
		log.Fatalf("[server] login: %v", err)
	}

	// auto folder
	if appCfg.StorageType == "google" && appCfg.GoogleFolderID == "" {
		log.Println("[server] searching for Drive folder 'Flow-Data'...")
		id, err := backend.FindFolder(ctx, "Flow-Data")
		if err != nil {
			log.Printf("[server] find folder: %v", err)
		} else if id == "" {
			log.Println("[server] creating Drive folder 'Flow-Data'...")
			id, err = backend.CreateFolder(ctx, "Flow-Data")
			if err != nil {
				log.Printf("[server] create folder: %v", err)
			}
		}
		if id != "" {
			appCfg.GoogleFolderID = id
			appCfg.Save(configPath)
		}
	}

	engine := transport.NewEngine(backend, false, "")
	if appCfg.RefreshRateMs > 0 {
		engine.SetPollRate(appCfg.RefreshRateMs)
	}
	if appCfg.FlushRateMs > 0 {
		engine.SetFlushRate(appCfg.FlushRateMs)
	}

	engine.OnNewSession = func(sessionID, targetAddr string, session *transport.Session) {
		log.Printf("[server] session %s -> %s", sessionID, targetAddr)
		go handleConn(sessionID, targetAddr, session, engine)
	}

	engine.Start(ctx)
	log.Println("[server] running, waiting for sessions...")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("[server] shutting down...")
	cancel()
}

func handleConn(sessionID, targetAddr string, session *transport.Session, engine *transport.Engine) {
	defer engine.RemoveSession(sessionID)

	conn, err := net.Dial("tcp", targetAddr)
	if err != nil {
		log.Printf("[server] dial %s: %v", targetAddr, err)
		return
	}
	defer conn.Close()

	errCh := make(chan error, 2)

	// سرور -> کلاینت
	go func() {
		// BUG FIX: buffer رو بزرگ‌تر کردیم — 4096 برای اکثر پکت‌ها کوچیکه
		buf := make([]byte, 32*1024)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				chunk := make([]byte, n)
				copy(chunk, buf[:n])
				session.EnqueueTx(chunk)
			}
			if err != nil {
				errCh <- err
				return
			}
		}
	}()

	// کلاینت -> سرور
	go func() {
		for {
			data, ok := <-session.RxChan
			if !ok {
				errCh <- fmt.Errorf("session closed by client")
				return
			}
			if len(data) > 0 {
				if _, err := conn.Write(data); err != nil {
					errCh <- err
					return
				}
			}
		}
	}()

	<-errCh
}
