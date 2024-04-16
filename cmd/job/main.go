package main

import (
	"bufio"
	"cimri/config"
	"cimri/internal/awswrapper"
	"cimri/internal/database"
	"cimri/internal/model"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

const BatchSize = 4000
const NWorker = 3

func main() {

	start := time.Now()

	configs := config.LoadConfigs()

	s3Client, s3ClientErr := configs.S3.NewS3Client()
	if s3ClientErr != nil {
		log.Fatal(s3ClientErr)
	}

	DB := database.NewConnection(configs.Database)
	database.InitDB(DB)
	defer DB.Close()

	productStore := database.NewProductStore(DB)

	objectKeys := []string{"products-1.jsonl", "products-2.jsonl", "products-3.jsonl", "products-4.jsonl"}

	processErr := processFiles(s3Client, productStore, objectKeys)
	if processErr != nil {
		log.Println(processErr)
	}

	fmt.Println("Total duration of the job: ", time.Since(start))
}

func processFiles(s3Client *awswrapper.S3Client, productStore *database.ProductStore, objectKeys []string) error {

	var outputScanners []*bufio.Scanner

	//for _, key := range objectKeys {
	//	output, s3GetObjectErr := s3Client.GetObjectFromBucket(key)
	//	defer output.Body.Close()
	//	if s3GetObjectErr != nil {
	//		log.Fatal(s3GetObjectErr)
	//	}
	//
	//	outputScanners = append(outputScanners, bufio.NewScanner(output.Body))
	//}

	for _, key := range objectKeys {
		f, err := os.Open(key)
		if err != nil {
			return err
		}
		outputScanners = append(outputScanners, bufio.NewScanner(f))

	}

	productCh := make(chan *model.Product, 32)
	lineCh := make(chan []byte, 32)

	// Scan input and send to workers
	scannerWg := sync.WaitGroup{}
	for _, scanner := range outputScanners {
		scannerWg.Add(1)
		scanner := scanner
		go func() {
			scannerRoutine(&scannerWg, scanner, lineCh)
		}()
		// To close the channel after all scanners are done
	}
	go func() {
		scannerWg.Wait()
		close(lineCh)
	}()

	wgParser := sync.WaitGroup{}
	// Start workers
	for _ = range NWorker {

		wgParser.Add(1)
		go func() {
			parserRoutine(&wgParser, lineCh, productCh)
		}()

	}
	go func() {
		wgParser.Wait()
		close(productCh)
	}()

	products := make([]*model.Product, BatchSize)

	wgInsert := sync.WaitGroup{}
	i := 0
	//insert to DB in batches

	for product := range productCh {
		products[i] = product
		if i == BatchSize-1 {
			copyProducts := append(make([]*model.Product, 0, BatchSize), products...)
			wgInsert.Add(1)
			go func() {
				defer wgInsert.Done()
				mainInsertErr := productStore.InsertProductsInBatches(copyProducts, BatchSize)
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

func scannerRoutine(wg *sync.WaitGroup, scanner *bufio.Scanner, lineCh chan<- []byte) {
	for scanner.Scan() {
		line := scanner.Bytes()
		lineCh <- append(make([]byte, 0, len(line)), line...)
	}
	wg.Done()
}

func parserRoutine(wg *sync.WaitGroup, lineCh <-chan []byte, productCh chan<- *model.Product) {
	for line := range lineCh {
		p := new(model.Product)
		err := json.Unmarshal(line, p)
		if err != nil {
			log.Fatal(err)
		}
		productCh <- p
	}
	wg.Done()
}

func getFileToLocal(r io.Reader, name string, isHalting bool) {
	file, err := os.Create(name)
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
