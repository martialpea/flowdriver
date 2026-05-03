package httpclient

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

// TransportConfig defines the rules for bypassing censorship
type TransportConfig struct {
	TargetIP           string
	SNI                string
	HostHeader         string
	InsecureSkipVerify bool
}

type hostRewriteTransport struct {
	Transport  http.RoundTripper
	HostHeader string
}

func (t *hostRewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.HostHeader != "" {
		req.Host = t.HostHeader
	}
	return t.Transport.RoundTrip(req)
}

// NewCustomClient creates an http.Client configured to bypass DPI/DNS.
func NewCustomClient(cfg TransportConfig) *http.Client {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if cfg.TargetIP != "" {
				return dialer.DialContext(ctx, "tcp", cfg.TargetIP)
			}
			return dialer.DialContext(ctx, network, addr)
		},
		TLSClientConfig: &tls.Config{
			ServerName:         cfg.SNI,
			InsecureSkipVerify: cfg.InsecureSkipVerify,
		},
		ForceAttemptHTTP2:     true,
		// بهینه‌سازی: افزایش MaxIdleConns برای استفاده بهتر از connection pooling
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   20,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		// بهینه‌سازی: فعال‌سازی compression خودکار
		DisableCompression: false,
	}

	var rt http.RoundTripper = transport
	if cfg.HostHeader != "" {
		rt = &hostRewriteTransport{
			Transport:  transport,
			HostHeader: cfg.HostHeader,
		}
	}

	return &http.Client{
		Transport: rt,
		// بهینه‌سازی: افزایش timeout برای upload فایل‌های بزرگ
		Timeout: 90 * time.Second,
	}
}
