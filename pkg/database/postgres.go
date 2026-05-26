package database

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/lib/pq"
)

var DB *sql.DB

func ConnectDB() {

	// Open database connection
	db, err := sql.Open("postgres", "postgres://avnadmin:AVNS_USP_xk4VuxF7BcKF5Jy@pg-3e869258-ememeden6-ec13.a.aivencloud.com:12319/bitnex?sslmode=require")
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
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(time.Minute * 5)
	db.SetConnMaxIdleTime(time.Minute * 2)

	DB = db

	log.Println("✅ Connected to PostgreSQL")
}
