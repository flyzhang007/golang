package main

import (
	"route"
	"net/http"
	"fmt"
	"os"
	"db"
)

func main() {
	if err := db.Init(); err != nil {
		fmt.Fprintf(os.Stderr, "Init db: %v\n", err)
		return
	}
	defer db.Destroy()

	if err := initSvr(); err != nil {
		fmt.Fprintf(os.Stderr, "initSvr failed: %v\n", err)
		return
	}
}

func initSvr() error {
	route.Init()

	if err := http.ListenAndServe("0.0.0.0:6666", nil); err != nil {
		return err
	}

	return nil
}
