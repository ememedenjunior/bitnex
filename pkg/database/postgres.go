package database

import (
	"database/sql"
	"log"
	"os"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

var DB *sql.DB

func ConnectDB() {

	// Load .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatal("❌ Error loading .env file")
	}

	// Get DB URL from environment
	dsn := os.Getenv("DB_URL")

	if dsn == "" {
		log.Fatal("❌ DB_URL is missing in .env")
	}

	// Open database connection
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal("❌ Failed to open DB connection:", err)
	}

	// Verify database connection
	err = db.Ping()
	if err != nil {
		log.Fatal("❌ Failed to connect to database:", err)
	}

	// Production connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)

	DB = db

	log.Println("✅ Connected to PostgreSQL")
}
