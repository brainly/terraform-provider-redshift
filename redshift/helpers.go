package redshift

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/lib/pq"
)

const (
	pqErrorCodeConcurrent        = "XX000"
	pqErrorCodeInvalidSchemaName = "3F000"
	pqErrorCodeDeadlock          = "40P01"
	pqErrorCodeFailedTransaction = "25P02"
)

// startTransaction starts a new DB transaction on the specified database.
// If the database is specified and different from the one configured in the provider,
// it will create a new connection pool if needed.
func startTransaction(client *Client, database string) (*sql.Tx, error) {
	if database != "" && database != client.databaseName {
		client = client.config.NewClient(database)
	}
	db, err := client.Connect()
	if err != nil {
		return nil, err
	}

	txn, err := db.Begin()
	if err != nil {
		return nil, fmt.Errorf("could not start transaction: %w", err)
	}

	return txn, nil
}

// deferredRollback can be used to rollback a transaction in a defer.
// It will log an error if it fails
func deferredRollback(txn *sql.Tx) {
	err := txn.Rollback()
	switch {
	case err == sql.ErrTxDone:
		// transaction has already been committed or rolled back
		log.Printf("[DEBUG]: %v", err)
	case err != nil:
		log.Printf("[ERR] could not rollback transaction: %v", err)
	}
}

// pqQuoteLiteral returns a string literal safe for inclusion in a PostgreSQL
// query as a parameter.  The resulting string still needs to be wrapped in
// single quotes in SQL (i.e. fmt.Sprintf(`'%s'`, pqQuoteLiteral("str"))).  See
// quote_literal_internal() in postgresql/backend/utils/adt/quote.c:77.
func pqQuoteLiteral(in string) string {
	in = strings.Replace(in, `\`, `\\`, -1)
	in = strings.Replace(in, `'`, `''`, -1)
	return in
}

func getGroupIDFromName(tx *sql.Tx, group string) (groupID int, err error) {
	err = tx.QueryRow("SELECT grosysid FROM pg_group WHERE groname = $1", group).Scan(&groupID)
	return
}

func getSchemaIDFromName(tx *sql.Tx, schema string) (schemaID int, err error) {
	err = tx.QueryRow("SELECT oid FROM pg_namespace WHERE nspname = $1", schema).Scan(&schemaID)
	return
}

func RedshiftResourceFunc(fn func(*DBConnection, *schema.ResourceData) error) func(*schema.ResourceData, interface{}) error {
	return func(d *schema.ResourceData, meta interface{}) error {
		client := meta.(*Client)

		db, err := client.Connect()
		if err != nil {
			return err
		}

		return fn(db, d)
	}
}

func RedshiftResourceRetryOnPQErrors(fn func(*DBConnection, *schema.ResourceData) error) func(*DBConnection, *schema.ResourceData) error {
	return func(db *DBConnection, d *schema.ResourceData) error {
		for i := 0; i < 10; i++ {
			err := fn(db, d)
			if err == nil {
				return nil
			}

			if pqErr, ok := err.(*pq.Error); !ok || !isRetryablePQError(string(pqErr.Code)) {
				return err
			}

			time.Sleep(time.Duration(i+1) * time.Second)
		}
		return nil
	}
}

func RedshiftResourceExistsFunc(fn func(*DBConnection, *schema.ResourceData) (bool, error)) func(*schema.ResourceData, interface{}) (bool, error) {
	return func(d *schema.ResourceData, meta interface{}) (bool, error) {
		client := meta.(*Client)

		db, err := client.Connect()
		if err != nil {
			return false, err
		}

		return fn(db, d)
	}
}

func isRetryablePQError(code string) bool {
	retryable := map[string]bool{
		pqErrorCodeConcurrent:        true,
		pqErrorCodeInvalidSchemaName: true,
		pqErrorCodeDeadlock:          true,
		pqErrorCodeFailedTransaction: true,
	}

	_, ok := retryable[code]
	return ok
}

func splitCsvAndTrim(raw string, delimiter rune) ([]string, error) {
	if raw == "" {
		return []string{}, nil
	}
	reader := csv.NewReader(strings.NewReader(raw))
	reader.Comma = delimiter
	rawSlice, err := reader.Read()
	if err != nil {
		return nil, err
	}
	result := []string{}
	for _, s := range rawSlice {
		trimmed := strings.TrimSpace(s)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result, nil
}
