package redshift

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"sync"

	_ "github.com/lib/pq"
)

var (
	dbRegistryLock sync.Mutex
	dbRegistry     map[string]*DBConnection = make(map[string]*DBConnection, 1)
)

// Config - provider config
type Config struct {
	Host     string
	Username string
	Password string
	Port     int
	Database string
	SSLMode  string
	MaxConns int
}

// Client struct holding connection string
type Client struct {
	config       Config
	databaseName string

	db *sql.DB
}

type DBConnection struct {
	*sql.DB

	client *Client
}

// NewClient returns client config for the specified database.
func (c *Config) NewClient(database string) *Client {
	return &Client{
		config:       *c,
		databaseName: database,
	}
}

// Connect returns a copy to an sql.Open()'ed database connection wrapped in a DBConnection struct.
// Callers must return their database resources. Use of QueryRow() or Exec() is encouraged.
// Query() must have their rows.Close()'ed.
func (c *Client) Connect() (*DBConnection, error) {
	dbRegistryLock.Lock()
	defer dbRegistryLock.Unlock()

	dsn := c.config.connStr(c.databaseName)
	conn, found := dbRegistry[dsn]
	if !found {
		db, err := sql.Open(proxyDriverName, dsn)
		if err != nil {
			return nil, fmt.Errorf("Error connecting to PostgreSQL server %s: %w", c.config.Host, err)
		}

		// We don't want to retain connection
		// So when we connect on a specific database which might be managed by terraform,
		// we don't keep opened connection in case of the db has to be dopped in the plan.
		db.SetMaxIdleConns(0)
		db.SetMaxOpenConns(c.config.MaxConns)

		conn = &DBConnection{
			db,
			c,
		}
		dbRegistry[dsn] = conn
	}

	return conn, nil
}

func (c *Config) connStr(database string) string {
	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?%s",
		url.QueryEscape(c.Username),
		url.QueryEscape(c.Password),
		c.Host,
		c.Port,
		database,
		strings.Join(c.connParams(), "&"),
	)

	return connStr
}

func (c *Config) connParams() []string {
	params := map[string]string{}

	params["sslmode"] = c.SSLMode
	params["connect_timeout"] = "180"

	paramsArray := []string{}
	for key, value := range params {
		paramsArray = append(paramsArray, fmt.Sprintf("%s=%s", key, url.QueryEscape(value)))
	}

	return paramsArray
}

// New redshift client
func (c *Config) Client() (*Client, error) {

	conninfo := fmt.Sprintf("sslmode=%v user=%v password=%v host=%v port=%v dbname=%v",
		c.SSLMode,
		c.Username,
		c.Password,
		c.Host,
		c.Port,
		c.Database)

	db, err := sql.Open(proxyDriverName, conninfo)
	if err != nil {
		db.Close()
		return nil, err
	}

	client := Client{
		config: *c,
		db:     db,
	}

	return &client, nil
}

func (c *Client) Close() {
	if c.db != nil {
		c.db.Close()
	}
}
