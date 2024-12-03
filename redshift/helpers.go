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
	pqErrorCodeDuplicateSchema   = "42P06"
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

func getUserIDFromName(tx *sql.Tx, user string) (userID int, err error) {
	err = tx.QueryRow("SELECT usesysid FROM pg_user WHERE usename = $1", user).Scan(&userID)
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

func splitCsvAndTrim(raw string) ([]string, error) {
	if raw == "" {
		return []string{}, nil
	}
	reader := csv.NewReader(strings.NewReader(raw))
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

func validatePrivileges(privileges []string, objectType string) bool {
	if objectType == "language" && len(privileges) == 0 {
		return false
	}
	for _, p := range privileges {
		switch strings.ToUpper(objectType) {
		case "SCHEMA":
			switch strings.ToUpper(p) {
			case "CREATE", "USAGE", "ALTER":
				continue
			default:
				return false
			}
		case "TABLE":
			switch strings.ToUpper(p) {
			case "SELECT", "UPDATE", "INSERT", "DELETE", "DROP", "REFERENCES", "RULE", "TRIGGER", "ALTER", "TRUNCATE":
				continue
			default:
				return false
			}
		case "DATABASE":
			switch strings.ToUpper(p) {
			case "CREATE", "TEMPORARY":
				continue
			default:
				return false
			}
		case "PROCEDURE", "FUNCTION":
			switch strings.ToUpper(p) {
			case "EXECUTE":
				continue
			default:
				return false
			}
		case "LANGUAGE":
			switch strings.ToUpper(p) {
			case "USAGE":
				continue
			default:
				return false
			}
		default:
			return false
		}
	}

	return true
}

func appendIfTrue(condition bool, item string, list *[]string) {
	if condition {
		*list = append(*list, item)
	}
}

func setToPgIdentList(identifiers *schema.Set, prefix string) string {
	quoted := make([]string, identifiers.Len())
	for i, identifier := range identifiers.List() {
		if prefix == "" {
			quoted[i] = pq.QuoteIdentifier(identifier.(string))
		} else {
			quoted[i] = fmt.Sprintf("%s.%s", pq.QuoteIdentifier(prefix), pq.QuoteIdentifier(identifier.(string)))
		}
	}

	return strings.Join(quoted, ",")
}

// Quoted identifiers somehow does not work for grants/revokes on functions and procedures
func setToPgIdentListNotQuoted(identifiers *schema.Set, prefix string) string {
	quoted := make([]string, identifiers.Len())
	for i, identifier := range identifiers.List() {
		if prefix == "" {
			quoted[i] = identifier.(string)
		} else {
			quoted[i] = fmt.Sprintf("%s.%s", prefix, identifier.(string))
		}
	}

	return strings.Join(quoted, ",")
}

func stripArgumentsFromCallablesDefinitions(defs *schema.Set) []string {
	parser := func(name string) string {
		return strings.Split(name, "(")[0]
	}

	names := make([]string, defs.Len())
	for _, def := range defs.List() {
		names = append(names, parser(def.(string)))
	}
	return names
}
