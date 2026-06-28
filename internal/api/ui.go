package api

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed web/*
var uiFiles embed.FS

func registerUI(mux *http.ServeMux) {
	assets, err := fs.Sub(uiFiles, "web")
	if err != nil {
		panic(err)
	}

	mux.Handle("GET /ui/", http.StripPrefix("/ui/", http.FileServer(http.FS(assets))))
	mux.HandleFunc("GET /docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		contents, err := uiFiles.ReadFile("web/docs.html")
		if err != nil {
			http.Error(w, "docs unavailable", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(contents)
	})
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		contents, err := uiFiles.ReadFile("web/index.html")
		if err != nil {
			http.Error(w, "control plane UI unavailable", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(contents)
	})
}
