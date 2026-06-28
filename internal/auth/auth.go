package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"sync"
	"time"

	"github.com/maestro-flink/maestro/internal/config"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// ponytail: in-memory session store — lost on restart. Add Redis when replicas > 1.
var (
	sessions sync.Map
	cfg      *oauth2.Config
)

func Init(auth config.AuthConfig) {
	if auth.GoogleClientID == "" {
		return
	}
	redirect := auth.RedirectURL
	if redirect == "" {
		redirect = "http://localhost:8080/auth/callback"
	}
	cfg = &oauth2.Config{
		ClientID:     auth.GoogleClientID,
		ClientSecret: auth.GoogleClientSecret,
		RedirectURL:  redirect,
		Scopes:       []string{"openid", "email"},
		Endpoint:     google.Endpoint,
	}
}

func Enabled() bool { return cfg != nil }

func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !Enabled() || isPublicPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		if cookie, err := r.Cookie("maestro_session"); err == nil {
			if exp, ok := sessions.Load(cookie.Value); ok && time.Now().Before(exp.(time.Time)) {
				next.ServeHTTP(w, r)
				return
			}
		}
		if len(r.URL.Path) >= 5 && r.URL.Path[:5] == "/api/" {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		http.Redirect(w, r, "/login", http.StatusFound)
	})
}

func OAuthStartHandler(w http.ResponseWriter, r *http.Request) {
	if !Enabled() {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}
	state := randHex(16)
	http.SetCookie(w, &http.Cookie{
		Name: "maestro_state", Value: state, Path: "/",
		HttpOnly: true, SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, cfg.AuthCodeURL(state), http.StatusFound)
}

func OAuthCallbackHandler(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("maestro_state")
	if err != nil || cookie.Value != r.URL.Query().Get("state") {
		http.Error(w, "invalid state", http.StatusBadRequest)
		return
	}
	if _, err := cfg.Exchange(context.Background(), r.URL.Query().Get("code")); err != nil {
		http.Error(w, "token exchange failed: "+err.Error(), http.StatusUnauthorized)
		return
	}
	token := randHex(32)
	sessions.Store(token, time.Now().Add(8*time.Hour))
	http.SetCookie(w, &http.Cookie{
		Name: "maestro_session", Value: token, Path: "/",
		HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: 28800,
	})
	http.Redirect(w, r, "/", http.StatusFound)
}

func LogoutHandler(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie("maestro_session"); err == nil {
		sessions.Delete(cookie.Value)
	}
	http.SetCookie(w, &http.Cookie{Name: "maestro_session", MaxAge: -1, Path: "/"})
	http.Redirect(w, r, "/login", http.StatusFound)
}

func isPublicPath(path string) bool {
	return path == "/login" || path == "/auth/login" || path == "/auth/callback" ||
		path == "/auth/logout" || path == "/healthz"
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
