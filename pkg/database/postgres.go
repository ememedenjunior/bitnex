package database

import (
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

var DB *sql.DB

func ConnectDB() {
	// Load env (only for local dev)
	_ = godotenv.Load()

	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		log.Fatal("❌ DB_URL is not set")
	}

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatal("❌ Failed to open DB connection:", err)
	}

	// Verify connection
	if err := db.Ping(); err != nil {
		log.Fatal("❌ Failed to connect to database:", err)
	}

	// Connection pool tuning (good for production)
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(time.Minute * 5)
	db.SetConnMaxIdleTime(time.Minute * 2)

	DB = db

	log.Println("✅ Connected to PostgreSQL")
}
