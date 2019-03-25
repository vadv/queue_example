package main

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	_ "github.com/lib/pq"
)

var (
	connectionString = flag.String(`db`, `host=/tmp dbname=queue`, `connection string`)
	maxWorkers       = flag.Int(`max-workers`, 4, `max workers`)
)

const (
	UserAgent   = `worker`
	HttpTimeout = 10 * time.Second
)

type worker struct {
	timeout    time.Duration
	db         *sql.DB
	httpClient *http.Client
	error      error
}

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}
	db, err := sql.Open(`postgres`, *connectionString)
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(*maxWorkers)
	db.SetMaxIdleConns(*maxWorkers) // alway openning connections as much as we need

	httpClient := &http.Client{Timeout: HttpTimeout}
	transport := &http.Transport{MaxIdleConns: *maxWorkers, MaxIdleConnsPerHost: *maxWorkers}
	httpClient.Transport = transport

	limit := make(chan bool, *maxWorkers)
	for {
		limit <- true
		go func() {
			defer func() { <-limit }()
			w := &worker{db: db, httpClient: httpClient, timeout: 10 * time.Second}
			w.run()
			if w.error != nil {
				time.Sleep(100 * time.Millisecond)
				if err != sql.ErrNoRows {
					log.Printf("[ERROR] info: %s\n", err.Error())
				}
			}
		}()
	}
}

func (w *worker) run() {
	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()
	tx, err := w.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		w.error = err
		return
	}

	defer func() {
		if w.error != nil {
			tx.Rollback()
		} else {
			w.error = tx.Commit()
		}
	}()

	row := tx.QueryRow(`select transaction_id, payload from queue where state = 'pending' for update skip locked limit 1`)
	transactionID, payload := ``, ``
	switch err := row.Scan(&transactionID, &payload); err {
	case nil:
		buffer := &bytes.Buffer{}
		buffer.WriteString(payload)
		req, _ := http.NewRequest(`POST`, `http://ya.ru`, buffer)
		resp, err := w.httpClient.Do(req)
		if err != nil {
			tx.Rollback()
			w.error = err
			return
		}
		defer resp.Body.Close()
		// for example, check only status code :)
		if resp.StatusCode != http.StatusOK {
			_, err := tx.Exec(`update queue set status = 'failed', reason = $2 where transaction_id = $1`, transactionID, fmt.Sprintf(`status code: %d`, resp.StatusCode))
			if err != nil {
				w.error = err
				return
			}
		}
		_, err = tx.Exec(`update queue set status = 'succeeded' where transaction_id = $1`, transactionID)
		if err != nil {
			w.error = err
		}
		return
	default:
		w.error = err
		return
	}
}
