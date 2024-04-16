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

func InitDB(db *sql.DB) {

	query := `create table if not exists public.products
(
    id          integer primary key,
    price       numeric(12, 2),
    title       varchar(255),
    category    varchar(100),
    brand       varchar(100),
    url         text,
    description text
);`

	_, err := db.Exec(query)
	if err != nil {
		log.Fatal("error initializing db tables: ", err)
	}
}
