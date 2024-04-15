package config

import (
	"cimri/internal/awswrapper"
	"cimri/internal/database"
	"github.com/joho/godotenv"
	"log"
	"os"
	"strconv"
)

type Config struct {
	Database  database.PostgresConfig
	S3        awswrapper.S3ClientConfig
	ServerCfg ServerConfig
}

type ServerConfig struct {
	Host string
	Port string
}

func LoadConfigs() *Config {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading env variables: ", err)
	}

	port, _ := strconv.Atoi(os.Getenv("POSTGRES_PORT"))
	dbConf := database.PostgresConfig{
		Host:     os.Getenv("POSTGRES_HOST"),
		Port:     port,
		User:     os.Getenv("POSTGRES_USER"),
		Password: os.Getenv("POSTGRES_PASSWORD"),
		DBName:   os.Getenv("POSTGRES_DB_NAME"),
	}

	s3ClientConfig := awswrapper.S3ClientConfig{
		AccessKey:  os.Getenv("AWS_S3_ACCESS_KEY"),
		SecretKey:  os.Getenv("AWS_S3_SECRET_KEY"),
		Region:     os.Getenv("AWS_S3_REGION"),
		BucketName: os.Getenv("AWS_S3_DEFAULT_BUCKET_NAME"),
	}

	serverConf := ServerConfig{
		Host: os.Getenv("SERVER_HOST"),
		Port: os.Getenv("SERVER_PORT"),
	}

	configs := Config{
		Database:  dbConf,
		S3:        s3ClientConfig,
		ServerCfg: serverConf,
	}

	return &configs

}
