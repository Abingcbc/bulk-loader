package main

import (
	"bulkloader/pkg/config"
	"bulkloader/pkg/local"
	"os"
)

func main() {
	cfg := config.Must(config.LoadConfig(os.Args[1:], nil))
	local.Run(cfg)
}
