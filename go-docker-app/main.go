package main

import (
	"fmt"
	"net/http"
)

func hello(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello from Go inside Docker!")
}

func main() {
	http.HandleFunc("/", hello)

	fmt.Println("Server starting on port 8080...")
	http.ListenAndServe(":8080", nil)
}
