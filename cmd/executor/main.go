package main

import (
	"database/sql"
	"flag"
	"log"
	"time"

	_ "github.com/lib/pq"
)

var (
	connectionString = flag.String(`db`, `host=/tmp dbname=queue`, `connection string`)
	maxDBConnections = flag.Int(`max-db-connections`, 80, `max db connections`)
	maxWorkers       = flag.Int(`max-workers`, 10000, `max workers`)
)

const (
	TransactionTimeout = 9 * time.Second
)

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}
	db, err := sql.Open(`postgres`, *connectionString)
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(*maxDBConnections)
	db.SetMaxIdleConns(*maxDBConnections) // alway openning connections as much as we need

	processed, failed, succeeded := 0, 0, 0
	timer := time.Now()
	limit := make(chan bool, *maxWorkers)
	for {
		limit <- true
		go func() {
			defer func() { <-limit }()
			w := &worker{db: db}
			w.process()
			processed++
			if err := w.execErr; err != nil {
				log.Printf("[DEBUG] process error: %s\n", err.Error())
				failed++
			} else {
				succeeded++
			}
			if processed%1000 == 0 {
				log.Printf("[INFO] processed: %d failed: %d succeeded: %d speed: %f values per second\n", processed, failed, succeeded, time.Now().Sub(timer).Seconds()/1000)
				timer = time.Now()
			}
		}()
	}
}
