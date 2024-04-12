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
	"strings"
	"sync"
	"time"
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

const BATCH_SIZE = 2000

func main() {

	start := time.Now()

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

	bucketName := os.Getenv("AWS_S3_BUCKET_NAME")
	objectKey := "products-1.jsonl"

	svc, s3SessionErr := createS3Session()
	if s3SessionErr != nil {
		log.Fatal(s3SessionErr)
	}

	result, getObjectErr := getObject(svc, bucketName, objectKey)
	if getObjectErr != nil {
		log.Fatal(getObjectErr)
	}

	db, dbConnErr := connectToDB()
	if dbConnErr != nil {
		log.Fatal(dbConnErr)
	}

	scanner := bufio.NewScanner(result.Body)
	defer result.Body.Close()

	nWorker := 1
	results := make(chan *Product, nWorker)
	parsingJobs := make(chan []byte, nWorker)
	wg := sync.WaitGroup{}

	for _ = range nWorker {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(parsingJobs, results)
		}()
	}
	go func() {
		// Scanning thread
		for scanner.Scan() {
			line := scanner.Bytes()
			lineCopy := make([]byte, len(line))
			copy(lineCopy, scanner.Bytes())
			parsingJobs <- lineCopy
		}
		close(parsingJobs)
		wg.Wait()
		close(results)
	}()

	products := make([]*Product, BATCH_SIZE)
	i := 0

	//insert to db in batches
	for product := range results {
		products[i] = product
		if i == BATCH_SIZE-1 {
			insertErr := insertProducts(db, products, BATCH_SIZE)
			if insertErr != nil {
				log.Fatal("error on insertion : ", insertErr)
			}
			i = 0
			continue
		}
		i++
	}
	if len(products) != 0 {
		residualInsertErr := insertProducts(db, products[:i], len(products))
		if residualInsertErr != nil {
			log.Fatal(residualInsertErr)
		}
	}

	fmt.Println("Total duration: ", time.Since(start))
}

func worker(parsingJobs <-chan []byte, results chan<- *Product) {
	for line := range parsingJobs {
		p := new(Product)
		err := json.Unmarshal(line, p)
		if err != nil {
			log.Fatal("json parsing error: ", err)
		}
		results <- p
	}

}

func insertProducts(db *sql.DB, products []*Product, batchSize int) error {

	query := "insert into products (id,price,title, category, brand, url, description) values "

	var (
		valueStrings []string
		valueVals    []interface{}
	)

	residual := len(products) % batchSize
	batchCount := len(products) / batchSize

	for i := range batchSize {
		values := fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d)", 7*i+1, 7*i+2, 7*i+3, 7*i+4, 7*i+5, 7*i+6, 7*i+7)
		valueStrings = append(valueStrings, values)
	}

	queryString := query + strings.Join(valueStrings, ",") + " ON CONFLICT (id) DO NOTHING"
	stmt, queryPrepareErr := db.Prepare(queryString)
	defer stmt.Close()
	if queryPrepareErr != nil {
		return queryPrepareErr
	}

	for n_batch := range batchCount {

		for i := range batchSize {
			p := products[n_batch*batchSize+i]
			valueVals = append(valueVals, p.ID, p.Price, p.Title, p.Category, p.Brand, p.Url, p.Description)
		}
		_, queryExecErr := stmt.Exec(valueVals...)
		if queryExecErr != nil {
			return queryExecErr
		}

		valueVals = valueVals[:0]
	}

	if residual != 0 {
		valueStrings = valueStrings[:0]
		for i := range residual {
			values := fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d)", 7*i+1, 7*i+2, 7*i+3, 7*i+4, 7*i+5, 7*i+6, 7*i+7)
			valueStrings = append(valueStrings, values)
			p := products[batchCount*batchSize+i]
			valueVals = append(valueVals, p.ID, p.Price, p.Title, p.Category, p.Brand, p.Url, p.Description)
		}

		queryString = query + strings.Join(valueStrings, ",") + " ON CONFLICT (id) DO NOTHING"

		_, err := db.Exec(queryString, valueVals...)
		if err != nil {
			return err
		}
	}

	return nil
}

func createS3Session() (*s3.S3, error) {

	accessKey := os.Getenv("AWS_S3_ACCESS_KEY")
	secretKey := os.Getenv("AWS_S3_SECRET_KEY")
	region := os.Getenv("AWS_S3_REGION")

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(accessKey, secretKey, ""),
	})
	if err != nil {
		return nil, err
	}

	return s3.New(sess), nil
}

func getObject(svc *s3.S3, bucketName string, objectKey string) (*s3.GetObjectOutput, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	}

	return svc.GetObject(input)
}

func connectToDB() (*sql.DB, error) {
	var (
		host     = os.Getenv("POSTGRES_HOST")
		portStr  = os.Getenv("POSTGRES_PORT")
		user     = os.Getenv("POSTGRES_USER")
		password = os.Getenv("POSTGRES_PASSWORD")
		dbname   = os.Getenv("POSTGRES_DB_NAME")
	)

	port, portConvertErr := strconv.Atoi(portStr)
	if portConvertErr != nil {
		return nil, portConvertErr
	}

	connectionStr := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", connectionStr)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}
