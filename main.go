package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"runtime"
	"strconv"

	//"go.mongodb.org/mongo-driver/bson"
	"io"
	"log"
	"math"
	"os"
	"sync"
	"time"
)

var ctx = context.Background()

type domain struct {
	GlobalRank int `bson:"GlobalRank"`
	TldRank int `bson:"TldRank"`
	Domain string `bson:"Domain"`
	TLD string `bson:"TLD"`
	RefSubNets int `bson:"RefSubNets"`
	RefIPs int `bson:"RefIPs"`
	IDNDomain string `bson:"IDN_Domain"`
	IDNTLD string `bson:"IDN_TLD"`
	PrevGlobalRank int `bson:"PrevGlobalRank"`
	PrevTldRank int `bson:"PrevTldRank"`
	PrevRefSubNets int `bson:"PrevRefSubNets"`
	PrevRefIPs int `bson:"PrevRefIPs"`
}

func connect() (*mongo.Database, error) {
	clientOptions := options.Client()
	clientOptions.ApplyURI("mongodb://localhost:27017")
	client, err := mongo.NewClient(clientOptions)

	if err != nil {
		return nil, err
	}

	err = client.Connect(ctx)

	return client.Database("tes-insert-satujuta"), nil
}

func insert(db *mongo.Database, data domain) error {
	_, err := db.Collection("domain").InsertOne(ctx, data)
	if err != nil {
		return err
	}

	return nil
}

const csvFile = "majestic_million.csv"
const totalWorkers = 100

func openCsvFile() (*csv.Reader, *os.File, error) {
	log.Println("Open csv file")

	f, err := os.Open(csvFile)
	if err != nil {
		return nil, nil, err
	}

	reader := csv.NewReader(f)
	return reader, f, nil
}

var dataHeaders = make([]string, 0)

func readCsvPerLine(reader *csv.Reader, jobs chan domain, wg *sync.WaitGroup) {
	for {
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		if len(dataHeaders) == 0 {
			dataHeaders = row
			continue
		}
		GlobalRank, _ := strconv.Atoi(row[0])
		TldRank, _ := strconv.Atoi(row[1])
		RefSubNets, _ := strconv.Atoi(row[4])
		RefIPs, _ := strconv.Atoi(row[5])
		PrevGlobalRank, _ := strconv.Atoi(row[8])
		PrevTldRank, _ := strconv.Atoi(row[9])
		PrevRefSubNets, _ := strconv.Atoi(row[10])
		PrevRefIPs, _ := strconv.Atoi(row[11])

		rowOrdered := domain{
			GlobalRank:     GlobalRank,
			TldRank:        TldRank,
			Domain:         row[2],
			TLD:            row[3],
			RefSubNets:     RefSubNets,
			RefIPs:         RefIPs,
			IDNDomain:      row[6],
			IDNTLD:         row[7],
			PrevGlobalRank: PrevGlobalRank,
			PrevTldRank:    PrevTldRank,
			PrevRefSubNets: PrevRefSubNets,
			PrevRefIPs:     PrevRefIPs,
		}

		wg.Add(1)
		jobs <- rowOrdered
	}
	close(jobs)
}

func dispatchWorkers(db *mongo.Database, jobs <- chan domain, wg *sync.WaitGroup) {
	for workerIndex := 0; workerIndex <= totalWorkers; workerIndex++ {
		go func(workerIndex int, db *mongo.Database, jobs <- chan domain, wg *sync.WaitGroup) {
			counter := 0

			for job := range jobs {
				doTheJob(workerIndex, counter, db, job)
				wg.Done()
				counter++
			}
		}(workerIndex, db, jobs, wg)
	}
}

func doTheJob(workerIndex int, counter int, db *mongo.Database, data domain) {
	for {
		var outerErr error
		func(outerErr *error) {
			defer func() {
				if err := recover(); err != nil {
					*outerErr = fmt.Errorf("%v", err)
				}
			}()

			err := insert(db, data)
			if err != nil {
				log.Fatal(err.Error())
			}
		}(&outerErr)
		if outerErr == nil {
			break
		}
	}

	if counter%100 == 0 {
		log.Println("=> worker", workerIndex, "inserted", counter, "data")
	}
}

func main() {
	runtime.GOMAXPROCS(2)
	start := time.Now()

	db, err := connect()
	if err != nil {
		log.Fatal(err.Error())
	}

	csvReader, csvFile, err := openCsvFile()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer func() {
		_ = csvFile.Close()
	}()

	jobs := make(chan domain, 0)
	wg := new(sync.WaitGroup)

	go dispatchWorkers(db, jobs, wg)
	readCsvPerLine(csvReader, jobs, wg)

	wg.Wait()

	duration := time.Since(start)

	fmt.Println("done in", int(math.Ceil(duration.Seconds())), "seconds")
}