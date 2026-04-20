package main

import (
	"flag"
	"log"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config.yaml", "path to workload config YAML")
	flag.Parse()

	log.Printf("agentdb-workload stub; config=%s (real main in later task)", configPath)
}
