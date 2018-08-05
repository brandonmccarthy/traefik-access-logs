package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"os"
)

var (
	logFile  = flag.String("log_file", "", "Path to the traefik log file.")
	sql_db   = flag.String("sql_db", "", "Path to the sqlite database.")
	truncate = flag.Bool("truncate", false, "Truncate the log file after reading.")
)

type logEntry struct {
	BackendAddr                     int            `json:"BackendAddr"`
	BackendName                     string         `json:"BackendName"`
	BackendURL                      backendTraefik `json:"BackendURL"`
	ClientAddr                      string         `json:"ClientAddr"`
	ClientHost                      string         `json:"ClientHost"`
	ClientPort                      string         `json:"ClientPort"`
	ClientUsername                  string         `json:"ClientUsername"`
	DownstreamContentSize           int            `json:"DownstreamContentSize"`
	DownstreamStatus                int            `json:"DownstreamStatus"`
	DownstreamStatusLine            string         `json:"DownstreamStatusLine"`
	Duration                        int            `json:"Duration"`
	FrontendName                    string         `json:"FrontendName"`
	OriginContentSize               int            `json:"OriginContentSize"`
	OriginDuration                  int            `json:"OriginDuration"`
	OriginStatus                    int            `json:"OriginStatus"`
	OriginStatusLine                string         `json:"OriginStatusLine"`
	Overhead                        int            `json:"Overhead"`
	RequestAddr                     string         `json:"RequestAddr"`
	RequestContentSize              int            `json:"RequestContentSize"`
	RequestCount                    int            `json:"RequestCount"`
	RequestHost                     string         `json:"RequestHost"`
	RequestLine                     string         `json:"RequestLine"`
	RequestMethod                   string         `json:"RequestMethod"`
	RequestPath                     string         `json:"RequestPath"`
	RequestPort                     string         `json:"RequestPort"`
	RequestProtocol                 string         `json:"RequestProtocol"`
	RetryAttempts                   int            `json:"RetryAttempts"`
	StartLocal                      string         `json:"StartLocal"`
	StartUTC                        string         `json:"StartUTC"`
	DownstreamContentType           string         `json:"downstream_Content-Type"`
	DownstreamDate                  string         `json:"downstream_Date"`
	Level                           string         `json:"level"`
	Msg                             string         `json:"msg"`
	OriginContentType               string         `json:"origin_Content-Type"`
	OriginDate                      string         `json:"origin_Date"`
	RequestAccept                   string         `json:"request_Accept"`
	RequestAcceptEncoding           string         `json:"request_Accept-Encoding"`
	RequestAcceptLanguage           string         `json:"request_Accept-Language"`
	RequestAccessControlAllowOrigin string         `json:"request_Access-Control-Allow-Origin"`
	RequestAuthorization            string         `json:"request_Authorization"`
	RequestDnt                      string         `json:"request_Dnt"`
	RequestReferer                  string         `json:"request_Referer"`
	RequestUserAgent                string         `json:"request_User-Agent"`
	Time                            string         `json:"time"`
}

type backendTraefik struct {
	Scheme     string `json:"Scheme"`
	Opaque     string `json:"Opaque"`
	User       string `json:"User"`
	Host       string `json:"Host"`
	Path       string `json:"Path"`
	RawPath    string `json:"RawPath"`
	ForceQuery string `json:"ForceQuery"`
	RawQuery   string `json:"RawQuery"`
	Fragment   string `json:"Fragment"`
}

func parseAccessLog(accessLog string, truncate bool) ([]logEntry, error) {
	// Open the json file
	jsonFile, err := os.Open(accessLog)
	if err != nil {
		return []logEntry{}, fmt.Errorf("unable to open traefik log file %s: %q", accessLog, err)
	}
	defer jsonFile.Close()

	// Iterate over the file, decode each line as json
	// since it's technically not in a list.
	scanner := bufio.NewScanner(jsonFile)
	var logs []logEntry
	for scanner.Scan() {
		logBytes := scanner.Bytes()
		if !json.Valid(logBytes) {
			return []logEntry{}, fmt.Errorf("line contains invalid json: %q", logBytes)
		}
		var logLine logEntry
		json.Unmarshal(logBytes, &logLine)
		logs = append(logs, logLine)
	}
	if truncate {
		jsonFile.Seek(0, 0)
		jsonFile.Truncate(0)
	}
	return logs, err
}

func insertLogs(logs []logEntry, db_path string) error {
	db, err := sql.Open("sqlite3", db_path)
	defer db.Close()
	if err != nil {
		return fmt.Errorf("unable to open sqlite database: %q", err)
	}
	stmt, err := db.Prepare(`CREATE TABLE IF NOT EXISTS access_logs (id INTEGER PRIMARY KEY, BackendName TEXT, BackendURLScheme TEXT, 
	BackendURLHost TEXT, ClientAddr TEXT, ClientHost TEXT, ClientPort TEXT, ClientUsername TEXT, DownstreamStatus INTEGER, DownstreamContentSize INTEGER,
	Duration INTEGER, FrontendName TEXT, OriginContentSize INTEGER, OriginDuration INTEGER, RequestAddr TEXT, RequestContentSize INTEGER, 
	RequestCount INTEGER, RequestHost TEXT, RequestMethod TEXT, RequestPath TEXT, RequestPort TEXT, RequestProtocol TEXT, StartUTC TEXT, RequestReferer TEXT,
	RequestUserAgent TEXT, Time TEXT)`)
	if err != nil {
		return fmt.Errorf("unable to prepare SQL statement: %q", err)
	}
	stmt.Exec()

	for _, log := range logs {
		stmt, err := db.Prepare(`INSERT INTO access_logs (BackendName, BackendURLScheme, BackendURLHost, ClientAddr, ClientHost, ClientPort,
		ClientUsername, DownstreamStatus, DownstreamContentSize, Duration, FrontendName, OriginContentSize, OriginDuration, RequestAddr, RequestContentSize,
		RequestCount, RequestHost, RequestMethod, RequestPath, RequestPort, RequestProtocol, StartUTC, RequestReferer, RequestUserAgent, Time) VALUES
		(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
		if err != nil {
			return fmt.Errorf("unable to prepare SQL statement: %q", err)
		}
		stmt.Exec(log.BackendName, log.BackendURL.Scheme, log.BackendURL.Host, log.ClientAddr, log.ClientHost, log.ClientPort, log.ClientUsername,
			log.DownstreamStatus, log.DownstreamContentSize, log.Duration, log.FrontendName, log.OriginContentSize, log.OriginDuration, log.RequestAddr,
			log.RequestContentSize, log.RequestCount, log.RequestHost, log.RequestMethod, log.RequestPath, log.RequestPort, log.RequestProtocol, log.StartUTC,
			log.RequestReferer, log.RequestUserAgent, log.Time)
	}
	return nil
}

func main() {
	flag.Parse()

	// Verify the flags have input
	if *logFile == "" {
		fmt.Printf("No log file specified, exiting.\n")
		os.Exit(1)
	}
	if *sql_db == "" {
		fmt.Printf("No sql DB specified, exiting.\n")
		os.Exit(1)
	}

	// Read and decode json line by line, add to logs slice
	fmt.Printf("Parsing access logs from %s", *logFile)
	logs, err := parseAccessLog(*logFile, *truncate)
	if err != nil {
		fmt.Printf("Unable to parse log file: %q\n", err)
		os.Exit(1)
	}

	fmt.Println("Inserting logs to sql database")
	err = insertLogs(logs, *sql_db)
	if err != nil {
		fmt.Printf("Error inserting logs to database: %q\n", err)
	}
}
