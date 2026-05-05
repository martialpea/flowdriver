package storage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type oauthClientJSON struct {
	Installed struct {
		ClientID     string   `json:"client_id"`
		ClientSecret string   `json:"client_secret"`
		AuthURI      string   `json:"auth_uri"`
		TokenURI     string   `json:"token_uri"`
		RedirectURIs []string `json:"redirect_uris"`
	} `json:"installed"`
}

type tokenCache struct {
	RefreshToken string `json:"refresh_token"`
	ClientID     string `json:"client_id,omitempty"`
	ClientSecret string `json:"client_secret,omitempty"`
}

type GoogleBackend struct {
	httpClient   *http.Client
	saPath       string
	folderID     string
	clientID     string
	clientSecret string
	tokenURI     string
	redirectURI  string
	token        string
	refreshToken string
	tokenEx      time.Time
	mu           sync.Mutex
	fileIDs      map[string]string
	fileIdsMu    sync.RWMutex
}

func NewGoogleBackend(client *http.Client, saPath, folderID string) *GoogleBackend {
	return &GoogleBackend{
		httpClient: client,
		saPath:     saPath,
		folderID:   folderID,
		fileIDs:    make(map[string]string),
	}
}

// NewGoogleBackendWithToken: هر دو فایل رو جداگانه می‌گیره
// credPath  = credentials.json (client_id و client_secret)
// tokenPath = credentials.json.token (refresh_token)
func NewGoogleBackendWithToken(client *http.Client, credPath, tokenPath, folderID string) *GoogleBackend {
	b := &GoogleBackend{
		httpClient: client,
		// FIX: saPath رو روی tokenPath بذار بدون .token
		// چون Login میاد saPath+".token" رو می‌خونه
		saPath:   strings.TrimSuffix(tokenPath, ".token"),
		folderID: folderID,
		fileIDs:  make(map[string]string),
	}
	// از credPath فقط client_id و client_secret می‌خونیم
	if data, err := os.ReadFile(credPath); err == nil {
		var cred oauthClientJSON
		if json.Unmarshal(data, &cred) == nil {
			b.clientID = cred.Installed.ClientID
			b.clientSecret = cred.Installed.ClientSecret
			if cred.Installed.TokenURI != "" {
				b.tokenURI = cred.Installed.TokenURI
			}
		}
	}
	return b
}

func (b *GoogleBackend) Login(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.tokenURI == "" {
		b.tokenURI = "https://oauth2.googleapis.com/token"
	}
	b.redirectURI = "http://localhost"

	tokenCachePath := b.saPath + ".token"

	// خواندن credentials.json برای client_id و client_secret
	if b.clientID == "" {
		if data, err := os.ReadFile(b.saPath); err == nil {
			var cred oauthClientJSON
			if json.Unmarshal(data, &cred) == nil && cred.Installed.ClientID != "" {
				b.clientID = cred.Installed.ClientID
				b.clientSecret = cred.Installed.ClientSecret
				if cred.Installed.TokenURI != "" {
					b.tokenURI = cred.Installed.TokenURI
				}
				if len(cred.Installed.RedirectURIs) > 0 {
					b.redirectURI = cred.Installed.RedirectURIs[0]
				}
			}
		}
	}

	// خواندن token file
	if cacheData, err := os.ReadFile(tokenCachePath); err == nil {
		var cache tokenCache
		if json.Unmarshal(cacheData, &cache) == nil && cache.RefreshToken != "" {
			b.refreshToken = cache.RefreshToken
			// اگه token file این مقادیر رو داشت override کن
			if cache.ClientID != "" {
				b.clientID = cache.ClientID
				b.clientSecret = cache.ClientSecret
			}
		}
	}

	// اگه refresh_token داریم، refresh کن
	if b.refreshToken != "" && b.clientID != "" && b.clientSecret != "" {
		if err := b.refreshWithRetry(ctx); err != nil {
			return fmt.Errorf("token refresh failed (client_id=%s): %w", b.clientID[:10]+"...", err)
		}
		return nil
	}

	if b.refreshToken != "" && (b.clientID == "" || b.clientSecret == "") {
		return fmt.Errorf("refresh_token found but client_id or client_secret missing")
	}

	// Interactive OAuth — فقط روی PC
	authURI := "https://accounts.google.com/o/oauth2/auth"
	link := fmt.Sprintf("%s?client_id=%s&redirect_uri=%s&response_type=code&scope=https://www.googleapis.com/auth/drive.file&access_type=offline",
		authURI, url.QueryEscape(b.clientID), url.QueryEscape(b.redirectURI))
	fmt.Printf("\nOpen this URL:\n%s\n\nEnter code: ", link)
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)
	code := input
	if strings.HasPrefix(input, "http") {
		if u, err := url.Parse(input); err == nil {
			if q := u.Query().Get("code"); q != "" {
				code = q
			}
		}
	}
	if err := b.exchangeCode(ctx, code); err != nil {
		return err
	}
	cache := tokenCache{
		RefreshToken: b.refreshToken,
		ClientID:     b.clientID,
		ClientSecret: b.clientSecret,
	}
	cacheBytes, _ := json.MarshalIndent(cache, "", "  ")
	os.WriteFile(tokenCachePath, cacheBytes, 0600)
	return nil
}

func (b *GoogleBackend) refreshWithRetry(ctx context.Context) error {
	for attempt := 0; attempt < 4; attempt++ {
		if err := b.refreshAccessToken(ctx); err == nil {
			return nil
		} else if attempt == 3 {
			return err
		}
		wait := time.Duration(1<<uint(attempt)) * time.Second
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return nil
}

func (b *GoogleBackend) exchangeCode(ctx context.Context, code string) error {
	v := url.Values{}
	v.Set("grant_type", "authorization_code")
	v.Set("code", code)
	v.Set("client_id", b.clientID)
	v.Set("client_secret", b.clientSecret)
	v.Set("redirect_uri", b.redirectURI)
	return b.executeTokenRequest(ctx, v)
}

func (b *GoogleBackend) refreshAccessToken(ctx context.Context) error {
	v := url.Values{}
	v.Set("grant_type", "refresh_token")
	v.Set("refresh_token", b.refreshToken)
	v.Set("client_id", b.clientID)
	v.Set("client_secret", b.clientSecret)
	return b.executeTokenRequest(ctx, v)
}

func (b *GoogleBackend) executeTokenRequest(ctx context.Context, v url.Values) error {
	req, err := http.NewRequestWithContext(ctx, "POST", b.tokenURI, strings.NewReader(v.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("token request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("token %d: %s", resp.StatusCode, string(body))
	}
	var res struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return err
	}
	b.token = res.AccessToken
	if res.RefreshToken != "" {
		b.refreshToken = res.RefreshToken
	}
	b.tokenEx = time.Now().Add(time.Duration(res.ExpiresIn-60) * time.Second)
	return nil
}

func (b *GoogleBackend) getValidToken(ctx context.Context) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if time.Now().After(b.tokenEx) {
		if err := b.refreshWithRetry(ctx); err != nil {
			return "", err
		}
	}
	return b.token, nil
}

func (b *GoogleBackend) Upload(ctx context.Context, filename string, data io.Reader) error {
	tok, err := b.getValidToken(ctx)
	if err != nil {
		return err
	}
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		defer pw.Close()
		defer mw.Close()
		h := make(textproto.MIMEHeader)
		h.Set("Content-Type", "application/json; charset=UTF-8")
		p1, _ := mw.CreatePart(h)
		meta := map[string]interface{}{"name": filename}
		if b.folderID != "" {
			meta["parents"] = []string{b.folderID}
		}
		json.NewEncoder(p1).Encode(meta)
		h = make(textproto.MIMEHeader)
		h.Set("Content-Type", "application/octet-stream")
		p2, _ := mw.CreatePart(h)
		io.Copy(p2, data)
	}()
	req, err := http.NewRequestWithContext(ctx, "POST",
		"https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart", pr)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (b *GoogleBackend) ListQuery(ctx context.Context, prefix string) ([]string, error) {
	tok, err := b.getValidToken(ctx)
	if err != nil {
		return nil, err
	}
	q := fmt.Sprintf("name contains '%s'", prefix)
	if b.folderID != "" {
		q += fmt.Sprintf(" and '%s' in parents", b.folderID)
	}
	u, _ := url.Parse("https://www.googleapis.com/drive/v3/files")
	v := u.Query()
	v.Set("q", q)
	v.Set("fields", "files(id, name)")
	v.Set("pageSize", "100")
	u.RawQuery = v.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list %d: %s", resp.StatusCode, string(body))
	}
	var res struct {
		Files []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"files"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	b.fileIdsMu.Lock()
	if len(b.fileIDs) > 1500 {
		b.fileIDs = make(map[string]string)
	}
	var names []string
	for _, f := range res.Files {
		if strings.HasPrefix(f.Name, prefix) {
			b.fileIDs[f.Name] = f.ID
			names = append(names, f.Name)
		}
	}
	b.fileIdsMu.Unlock()
	return names, nil
}

func (b *GoogleBackend) Download(ctx context.Context, filename string) (io.ReadCloser, error) {
	b.fileIdsMu.RLock()
	fileID, ok := b.fileIDs[filename]
	b.fileIdsMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("file-id not found: %s", filename)
	}
	tok, err := b.getValidToken(ctx)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "GET",
		"https://www.googleapis.com/drive/v3/files/"+fileID+"?alt=media", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("download %d: %s", resp.StatusCode, string(body))
	}
	return resp.Body, nil
}

func (b *GoogleBackend) Delete(ctx context.Context, filename string) error {
	b.fileIdsMu.RLock()
	fileID, ok := b.fileIDs[filename]
	b.fileIdsMu.RUnlock()
	if !ok {
		return nil
	}
	tok, err := b.getValidToken(ctx)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "DELETE",
		"https://www.googleapis.com/drive/v3/files/"+fileID, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b.fileIdsMu.Lock()
	delete(b.fileIDs, filename)
	b.fileIdsMu.Unlock()
	return nil
}

func (b *GoogleBackend) CreateFolder(ctx context.Context, name string) (string, error) {
	tok, err := b.getValidToken(ctx)
	if err != nil {
		return "", err
	}
	meta := map[string]interface{}{
		"name":     name,
		"mimeType": "application/vnd.google-apps.folder",
	}
	body, _ := json.Marshal(meta)
	req, err := http.NewRequestWithContext(ctx, "POST",
		"https://www.googleapis.com/drive/v3/files", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b2, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("create folder %d: %s", resp.StatusCode, string(b2))
	}
	var res struct{ ID string `json:"id"` }
	json.NewDecoder(resp.Body).Decode(&res)
	b.folderID = res.ID
	return res.ID, nil
}

func (b *GoogleBackend) FindFolder(ctx context.Context, name string) (string, error) {
	tok, err := b.getValidToken(ctx)
	if err != nil {
		return "", err
	}
	q := fmt.Sprintf("name = '%s' and mimeType = 'application/vnd.google-apps.folder' and trashed = false", name)
	u, _ := url.Parse("https://www.googleapis.com/drive/v3/files")
	v := u.Query()
	v.Set("q", q)
	v.Set("fields", "files(id)")
	u.RawQuery = v.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var res struct {
		Files []struct{ ID string `json:"id"` } `json:"files"`
	}
	json.NewDecoder(resp.Body).Decode(&res)
	if len(res.Files) > 0 {
		b.folderID = res.Files[0].ID
		return res.Files[0].ID, nil
	}
	return "", nil
}
