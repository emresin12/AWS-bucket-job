package database

import (
	"database/sql"
	"fmt"
	"log"
)

type PostgresConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
}

func NewConnection(cfg PostgresConfig) *sql.DB {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DBName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	if err = db.Ping(); err != nil {
		log.Fatal(err)
	}

	return db
}
