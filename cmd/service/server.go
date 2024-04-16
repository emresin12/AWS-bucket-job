package main

import (
	"cimri/config"
	"cimri/internal/api"
	"cimri/internal/database"
	_ "github.com/lib/pq"
	"log"
)

func main() {

	configs := config.LoadConfigs()
	DB := database.NewConnection(configs.Database)
	database.InitDB(DB)

	r := api.SetupRouter(DB)
	log.Fatal(r.Run(configs.ServerCfg.Host + ":" + configs.ServerCfg.Port))
}
