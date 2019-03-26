package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"
)

type taskStatus int

const (
	taskStatusFailed taskStatus = iota
	taskStatusSucceeded
)

const (
	tableNameStatusFailed    = `queue_failed`
	tableNameStatusSucceeded = `queue_succeeded`
)

type worker struct {
	db      *sql.DB
	execErr error
}

func (w *worker) process() {
	w.execErr = nil

	ctx, cancel := context.WithTimeout(context.Background(), TransactionTimeout)
	defer cancel()

	tx, err := w.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		w.execErr = err
		return
	}

	defer func() {
		if w.execErr == nil {
			w.execErr = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	// get task
	txID, payload := ``, ``
	row := tx.QueryRow(`select tx_id, payload from queue_pending order by id for update skip locked limit 1`)
	err = row.Scan(&txID, &payload)
	if err != nil {
		if err == sql.ErrNoRows {
			// nothing to do
		} else {
			w.execErr = err
		}
		return
	}
	// try lock
	_, err = tx.Exec(`select queue_try_obtain_lock_tx_id($1)`, txID)
	if err != nil {
		w.execErr = err
		return
	}

	// random :)
	value := rand.Int63n(100)
	time.Sleep(time.Millisecond * time.Duration(value))
	if value > 90 {
		w.execErr = changeTaskStatus(tx, txID, payload, "random too big", taskStatusFailed)
		return
	}
	w.execErr = changeTaskStatus(tx, txID, payload, "ok", taskStatusSucceeded)
}

// you must get queue obtain_lock_tx_id before using
func changeTaskStatus(tx *sql.Tx, txID, payload, reason string, status taskStatus) error {
	targetTableName := tableNameStatusFailed
	if status == taskStatusSucceeded {
		targetTableName = tableNameStatusSucceeded
	}
	result, err := tx.Exec(`delete from queue_pending where tx_id = $1`, txID)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("must be one row, but get %d", affected)
	}
	queue := fmt.Sprintf(`insert into %s (tx_id, payload, reason) values ($1, $2, $3)`, targetTableName)
	result, err = tx.Exec(queue, txID, payload, reason)
	if err != nil {
		return err
	}
	affected, _ = result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("must be one row, but get %d", affected)
	}
	return nil
}
