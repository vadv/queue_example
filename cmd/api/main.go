package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"net"
	"net/http"
	"time"

	_ "github.com/lib/pq"
)

var (
	connectionString = flag.String(`db`, `host=/tmp dbname=queue`, `connection string`)
	maxConnections   = flag.Int(`max-connections`, 10, `max connections`)
	listenAddr       = flag.String(`bind`, `:1234`, `api listen`)
)

type apiServer struct {
	db *sql.DB
}

func main() {
	if !flag.Parsed() {
		flag.Parse()
	}
	db, err := sql.Open(`postgres`, *connectionString)
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(*maxConnections)
	db.SetMaxIdleConns(*maxConnections) // alway openning connections as much as we need
	server := &apiServer{db: db}
	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		panic(err)
	}
	api := &http.Server{Handler: server, IdleTimeout: time.Second * 60}
	if err := api.Serve(listener); err != nil {
		panic(err)
	}
}

type requestCreatePayload struct {
	ID      string           `json:"id"`
	Payload *json.RawMessage `json:"payload"`
}
type requestStatePayload struct {
	ID string `json:"id"`
}
type responsePayload struct {
	Ok          bool            `json:"ok"`
	ErrorCode   int             `json:"error_code"`
	Description string          `json:"description"`
	Result      json.RawMessage `json:"result,omitempty"`
}

func (s *apiServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	resp := &responsePayload{Ok: false, Description: `unknown`, ErrorCode: http.StatusNotAcceptable}
	defer func() {
		data, err := json.Marshal(resp)
		if err != nil {
			panic(err)
		}
		w.Header().Set(`Content-Type`, `application/json`)
		w.Write(data)
		if resp.ErrorCode != http.StatusOK {
			w.WriteHeader(resp.ErrorCode)
		}
	}()

	if req.Method != `POST` {
		resp.Description = `Not acceptable method`
		return
	}

	decoder := json.NewDecoder(req.Body)
	defer req.Body.Close()

	if req.URL.Path == `/status` {
		p := &requestStatePayload{}
		if err := decoder.Decode(p); err != nil {
			resp.ErrorCode = http.StatusNotAcceptable
			resp.Description = err.Error()
			return
		}
		row, state := s.db.QueryRow(`select state from queue where transaction_id = $1`, p.ID), ``
		switch err := row.Scan(&state); err {
		case sql.ErrNoRows:
			resp.ErrorCode = http.StatusNotFound
			resp.Description = `not found`
		case nil:
			resp.ErrorCode = http.StatusOK
			resp.Ok = true
			resp.Result = []byte(state)
		default:
			resp.ErrorCode = http.StatusInternalServerError
			resp.Description = err.Error()
		}
		return
	}

	if req.URL.Path == `/healthcheck` {
		if err := s.db.Ping(); err != nil {
			resp.ErrorCode = http.StatusInternalServerError
			resp.Description = err.Error()
			return
		} else {
			resp.ErrorCode = http.StatusOK
			return
		}
	}

	if req.URL.Path == `/create` {
		p := &requestCreatePayload{}
		if err := decoder.Decode(p); err != nil {
			resp.ErrorCode = http.StatusNotAcceptable
			resp.Description = err.Error()
			return
		}
		_, err := s.db.Exec(`insert into queue (transaction_id, payload) values ($1, $2)`, p.ID, p.Payload)
		if err != nil {
			resp.ErrorCode = http.StatusNotAcceptable
			resp.Description = err.Error()
			return
		}
		return
	}

}
