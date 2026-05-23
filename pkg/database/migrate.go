package database

import (
	"log"
)

// ============================
// 🗄️ MIGRATIONS
// ============================
func MigrateUser() {

	query := `
	CREATE TABLE IF NOT EXISTS users (	
		user_uid BIGINT PRIMARY KEY,
		email TEXT UNIQUE NOT NULL,
		username TEXT UNIQUE NOT NULL,
		password_hash TEXT NOT NULL,
		is_verified BOOLEAN DEFAULT FALSE,
		role TEXT DEFAULT 'user',
		created_at TIMESTAMP DEFAULT NOW(),
		updated_at TIMESTAMP DEFAULT NOW()
	);
	`

	_, err := DB.Exec(query)
	if err != nil {
		log.Fatal("❌ Migration failed:", err)
	}

	log.Println("✅ Migration completed")
}

func MigrateAccounts() {

	query := `
	CREATE TABLE IF NOT EXISTS accounts (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		user_uid BIGINT NOT NULL,
		asset TEXT NOT NULL,
		balance NUMERIC(36,18) DEFAULT 0,
		created_at TIMESTAMP DEFAULT NOW(),

		UNIQUE(user_uid, asset)
	);
	`

	_, err := DB.Exec(query)
	if err != nil {
		log.Fatal("❌ Accounts migration failed:", err)
	}

	log.Println("✅ Accounts migration completed")
}

func MigrateWallets() {

	query := `
	CREATE TABLE IF NOT EXISTS wallets (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

		user_uid BIGINT NOT NULL UNIQUE,

		address TEXT NOT NULL UNIQUE,
		asset TEXT NOT NULL,

		encrypted_private_key TEXT NOT NULL,

		created_at TIMESTAMP DEFAULT NOW()
	);
	`

	_, err := DB.Exec(query)
	if err != nil {
		log.Fatal("❌ Wallets migration failed:", err)
	}

	log.Println("✅ Wallets migration completed")
}

func MigrateSessions() {

	query := `
	CREATE TABLE IF NOT EXISTS refresh_tokens (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		user_uid BIGINT NOT NULL,
		refresh_token TEXT NOT NULL,
		expires_at TIMESTAMP NOT NULL,
		revoked BOOLEAN DEFAULT FALSE,
		created_at TIMESTAMP DEFAULT NOW()
	);
	`

	_, err := DB.Exec(query)
	if err != nil {
		log.Fatal("❌ Sessions migration failed:", err)
	}

	log.Println("✅ Sessions migration completed")
}

func MigrateVerificationTokens() {

	query := `
	CREATE TABLE IF NOT EXISTS verification_tokens (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		email TEXT UNIQUE NOT NULL,
		token TEXT UNIQUE NOT NULL,
		expires_at TIMESTAMP NOT NULL,
		used BOOLEAN DEFAULT FALSE,
		created_at TIMESTAMP DEFAULT NOW()
	);
	`

	_, err := DB.Exec(query)
	if err != nil {
		log.Fatal("❌ Verification tokens migration failed:", err)
	}

	log.Println("✅ Verification tokens migration completed")
}

func MigrateTokenAccounts() {

	query := `
	CREATE TABLE IF NOT EXISTS token_accounts (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

		user_id UUID NOT NULL,

		wallet_address TEXT NOT NULL,

		token_mint TEXT NOT NULL,

		token_account_address TEXT NOT NULL UNIQUE,

		symbol TEXT,

		created_at TIMESTAMP DEFAULT NOW()
	);
	`

	_, err := DB.Exec(query)
	if err != nil {
		log.Fatal(err)
	}
}
