package main

import (
	"fmt"
	"log"

	"github.com/lyteabovenyte/distributed_services_with_go/proglog/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	fmt.Println("listening")
	log.Fatal(srv.ListenAndServe())
}
