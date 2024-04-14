package main

import (
	"bufio"
	"cimri/config"
	"cimri/internal/awswrapper"
	"cimri/internal/database"
	"cimri/internal/model"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"sync"
	"time"
)

const BATCH_SIZE = 4000
const N_WORKER = 2

func main() {

	f, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatal(err)
	}

	pprof.StartCPUProfile(f)

	defer pprof.StopCPUProfile()

	traceFile, err := os.Create("trace.out")
	if err != nil {
		panic(err)
	}
	defer traceFile.Close()

	if err := trace.Start(traceFile); err != nil {
		panic(err)
	}
	defer trace.Stop()

	start := time.Now()

	configs := config.LoadConfigs()

	s3Client, s3ClientErr := configs.S3.NewS3Client()
	if s3ClientErr != nil {
		log.Fatal(s3ClientErr)
	}

	DB, dbConnErr := database.NewConnection(configs.Database)
	if dbConnErr != nil {
		log.Fatal(dbConnErr)
	}
	defer DB.Close()

	processingErr := processFiles(s3Client, DB)
	if processingErr != nil {
		return
	}

	fmt.Println("Total duration: ", time.Since(start))
}

func processFiles(s3Client *awswrapper.S3Client, DB *sql.DB) error {
	productStore := database.NewProductStore(DB)

	objectOutput1, s3GetObjectErr := s3Client.GetObjectFromBucket("products-1.jsonl")
	if s3GetObjectErr != nil {
		log.Fatal(s3GetObjectErr)
	}
	defer objectOutput1.Body.Close()

	//f, err := os.Open("output.jsonl")
	//if err != nil {
	//	return err
	//}

	scanner := bufio.NewScanner(objectOutput1.Body)
	//scanner = bufio.NewScanner(f)

	productCh := make(chan *model.Product, 32)
	lineCh := make(chan []byte, 32)
	wg := sync.WaitGroup{}

	// Start workers
	for _ = range N_WORKER {
		wg.Add(1)
		go func() {
			for line := range lineCh {
				p := new(model.Product)
				err := json.Unmarshal(line, p)
				if err != nil {
					log.Fatal(err)
				}
				productCh <- p
			}
			wg.Done()

		}()
	}
	// Scan input and send to workers
	go func() {
		for scanner.Scan() {
			line := scanner.Bytes()
			lineCh <- append(make([]byte, 0, len(line)), line...)
		}
		close(lineCh)
		wg.Wait()
		close(productCh)

	}()

	products := make([]*model.Product, BATCH_SIZE)

	wgInsert := sync.WaitGroup{}
	i := 0
	//insert to DB in batches
	for product := range productCh {
		products[i] = product
		if i == BATCH_SIZE-1 {
			copyProducts := append(make([]*model.Product, 0, BATCH_SIZE), products...)
			wgInsert.Add(1)
			go func() {
				defer wgInsert.Done()
				mainInsertErr := productStore.InsertProductsInBatches(copyProducts, BATCH_SIZE)
				if mainInsertErr != nil {
					log.Fatal(mainInsertErr)
				}

			}()

			i = 0
			continue
		}
		i++
	}
	if len(products) != 0 {
		residualInsertErr := productStore.InsertProductsInBatches(products[:i], i)
		if residualInsertErr != nil {
			log.Fatal(residualInsertErr)
		}
	}

	wgInsert.Wait()

	return nil

}

func getFileToLocal(r io.Reader, isHalting bool) {
	file, err := os.Create("output.jsonl")
	if err != nil {
		log.Fatal(err)
	}
	buf := make([]byte, 1024)
	rd := bufio.NewReader(r)
	for {
		// read a chunk
		n, err := rd.Read(buf)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if n == 0 {
			break
		}

		// write a chunk
		if _, err := file.Write(buf[:n]); err != nil {
			panic(err)
		}
	}
	if isHalting {
		os.Exit(0)
	}

}
