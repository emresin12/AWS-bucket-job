package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"log"
	"os"
	"runtime/pprof"
	"strconv"
)

type Product struct {
	ID          int     `json:"id"`
	Price       float64 `json:"price"`
	Title       string  `json:"title"`
	Category    string  `json:"category"`
	Brand       string  `json:"brand"`
	Url         string  `json:"url"`
	Description string  `json:"description"`
}

func main() {

	envErr := godotenv.Load()
	if envErr != nil {
		log.Fatal(envErr)
	}

	f, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatal(err)
	}

	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	accessKey := os.Getenv("AWS_S3_ACCESS_KEY")
	secretKey := os.Getenv("AWS_S3_SECRET_KEY")
	region := os.Getenv("AWS_S3_REGION")
	bucketName := os.Getenv("AWS_S3_BUCKET_NAME")
	objectKey := "products-1.jsonl"

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(accessKey, secretKey, ""),
	})
	if err != nil {
		fmt.Println("Error creating session ", err)
		return
	}

	svc := s3.New(sess)

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	}

	result, err := svc.GetObject(input)
	if err != nil {
		fmt.Println("Error getting object", err)
		return
	}

	var (
		host     = os.Getenv("POSTGRES_HOST")
		portStr  = os.Getenv("POSTGRES_PORT")
		user     = os.Getenv("POSTGRES_USER")
		password = os.Getenv("POSTGRES_PASSWORD")
		dbname   = os.Getenv("POSTGRES_DB_NAME")
	)

	port, portConvertErr := strconv.Atoi(portStr)
	if portConvertErr != nil {
		log.Fatal("port number is not valid err: ", portConvertErr)
	}

	connectionStr := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", connectionStr)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	defer result.Body.Close()
	scanner := bufio.NewScanner(result.Body)
	//var products []Product

	query := `insert into products (id,price,title, category, brand, url, description) values ($1, $2, $3, $4, $5, $6, $7)`
	stmt, queryErr := db.Prepare(query)
	if queryErr != nil {
		log.Fatal(queryErr)
	}

	for scanner.Scan() {
		line := scanner.Bytes()
		var p Product
		jsonErr := json.Unmarshal(line, &p)
		if jsonErr != nil {
			log.Println(jsonErr)
		}

		_, err := stmt.Exec(p.ID, p.Price, p.Title, p.Category, p.Brand, p.Url, p.Description)
		if err != nil {
			log.Println(err)
		}

	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading S3 object:", err)
	}

}
