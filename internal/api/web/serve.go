package main

import (
	"log"
	"net/http"
)

func main() {
	http.Handle("/ui/", http.StripPrefix("/ui/", http.FileServer(http.Dir("."))))
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/login.html", http.StatusFound)
	})
	log.Println("Listening on :8080...")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}
}
