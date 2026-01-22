// Package main provides the entry point for the pgqueue-dash TUI.
// It handles CLI flag parsing, database connection pooling, and
// program initialization.
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	_ "github.com/joho/godotenv/autoload"
	_ "github.com/lib/pq"
)

func main() {
	dsnFlag := flag.String("dsn", "", "PostgreSQL connection string")
	pollFlag := flag.Duration("poll", 5*time.Second, "Database polling interval")
	flag.Parse()

	connStr := *dsnFlag
	if connStr == "" {
		connStr = os.Getenv(envConnString)
	}

	if connStr == "" {
		fmt.Printf("❌ Error: No database connection string provided.\n\n")
		fmt.Println("Usage:")
		fmt.Println("  pgqueue-dash --dsn='postgres://user:pass@localhost:5432/app_db'")
		fmt.Println("  OR set the PG_CONN_STRING environment variable.")
		os.Exit(1)
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("DB Open Failed: ", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(time.Hour)

	if err := db.Ping(); err != nil {
		log.Fatal("DB Ping Failed: ", err)
	}

	p := tea.NewProgram(initialModel(db, *pollFlag), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		log.Fatal(err)
	}
}
