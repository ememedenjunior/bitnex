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
		daily_open_equity NUMERIC(36,18) DEFAULT 0,
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
		id BIGSERIAL PRIMARY KEY,
		user_uid BIGINT NOT NULL,
		chain VARCHAR(50) NOT NULL,
		address VARCHAR(255) NOT NULL UNIQUE,
		private_key_encrypted TEXT NOT NULL,
		derivation_path VARCHAR(255) NOT NULL,
		derivation_index BIGINT NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		is_active BOOLEAN DEFAULT true
	);
	`

	_, err := DB.Exec(query)
	if err != nil {
		log.Fatal("❌ Wallets migration failed:", err)
	}

	log.Println("✅ Wallets migration completed")
}

func MigrateHotWallets() {

	query := `
	CREATE TABLE IF NOT EXISTS hot_wallets (
		id BIGSERIAL PRIMARY KEY,
		chain VARCHAR(50) NOT NULL,
		mnemonic_encrypted TEXT NOT NULL,
		address VARCHAR(255) NOT NULL UNIQUE,
		private_key_encrypted TEXT NOT NULL,
		balance DECIMAL(40, 18) DEFAULT 0,
		derivation_path VARCHAR(255) NOT NULL,
		wallet_index INTEGER DEFAULT 0,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		is_active BOOLEAN DEFAULT true
	);
	`

	_, err := DB.Exec(query)
	if err != nil {
		log.Fatal("❌ Wallets migration failed:", err)
	}

	log.Println("✅ Wallets migration completed")
}

func MigrateVerificationTokens() {

	query := `
	CREATE TABLE IF NOT EXISTS verification_tokens (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		email TEXT NOT NULL,
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

// func MigrateSessions() {

// 	query := `
// 	CREATE TABLE IF NOT EXISTS refresh_tokens (
// 		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
// 		user_uid BIGINT NOT NULL,
// 		refresh_token TEXT NOT NULL,
// 		expires_at TIMESTAMP NOT NULL,
// 		revoked BOOLEAN DEFAULT FALSE,
// 		created_at TIMESTAMP DEFAULT NOW()
// 	);
// 	`

// 	_, err := DB.Exec(query)
// 	if err != nil {
// 		log.Fatal("❌ Sessions migration failed:", err)
// 	}

// 	log.Println("✅ Sessions migration completed")
// }

// func MigrateTokenAccounts() {

// 	query := `
// 	CREATE TABLE IF NOT EXISTS token_accounts (
// 		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

// 		user_id UUID NOT NULL,

// 		wallet_address TEXT NOT NULL,

// 		token_mint TEXT NOT NULL,

// 		token_account_address TEXT NOT NULL UNIQUE,

// 		symbol TEXT,

// 		created_at TIMESTAMP DEFAULT NOW()
// 	);
// 	`

// 	_, err := DB.Exec(query)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// }
