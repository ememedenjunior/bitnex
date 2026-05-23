package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

type Ledger struct {
	db *sql.DB
}

type Account struct {
	ID        string
	UserID    string
	Asset     string
	Balance   float64
	CreatedAt time.Time
}

type LedgerEntry struct {
	ID        string
	UserID    string
	Asset     string
	Debit     float64
	Credit    float64
	Reference string
	Type      string
	CreatedAt time.Time
}

// initSchema creates the required tables
func (l *Ledger) InitSchema() error {
	// Create accounts table
	accountsTable := `
	CREATE TABLE IF NOT EXISTS accounts (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		user_id UUID NOT NULL,
		asset TEXT NOT NULL,
		balance NUMERIC(36,18) DEFAULT 0,
		created_at TIMESTAMP DEFAULT NOW(),
		updated_at TIMESTAMP DEFAULT NOW(),
		UNIQUE(user_id, asset)
	);`

	if _, err := l.db.Exec(accountsTable); err != nil {
		return fmt.Errorf("failed to create accounts table: %w", err)
	}

	// Create ledger_entries table
	entriesTable := `
	CREATE TABLE IF NOT EXISTS ledger_entries (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		user_id UUID,
		asset TEXT NOT NULL,
		debit NUMERIC(36,18) DEFAULT 0,
		credit NUMERIC(36,18) DEFAULT 0,
		reference TEXT,
		type TEXT,
		created_at TIMESTAMP DEFAULT NOW()
	);`

	if _, err := l.db.Exec(entriesTable); err != nil {
		return fmt.Errorf("failed to create ledger_entries table: %w", err)
	}

	// Create indexes for better performance
	indexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_accounts_user_id ON accounts(user_id);",
		"CREATE INDEX IF NOT EXISTS idx_accounts_asset ON accounts(asset);",
		"CREATE INDEX IF NOT EXISTS idx_ledger_user_id ON ledger_entries(user_id);",
		"CREATE INDEX IF NOT EXISTS idx_ledger_reference ON ledger_entries(reference);",
		"CREATE INDEX IF NOT EXISTS idx_ledger_created_at ON ledger_entries(created_at);",
	}

	for _, idx := range indexes {
		if _, err := l.db.Exec(idx); err != nil {
			log.Printf("Warning: failed to create index: %v", err)
		}
	}

	return nil
}

// CreateAccount creates a new account for a user with a specific asset
func (l *Ledger) CreateAccount(ctx context.Context, userID, asset string) (*Account, error) {
	query := `
	INSERT INTO accounts (user_id, asset, balance)
	VALUES ($1, $2, 0)
	ON CONFLICT (user_id, asset) DO NOTHING
	RETURNING id, user_id, asset, balance, created_at;
	`

	var account Account
	err := l.db.QueryRowContext(ctx, query, userID, asset).Scan(
		&account.ID,
		&account.UserID,
		&account.Asset,
		&account.Balance,
		&account.CreatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			// Account already exists, fetch it
			return l.GetAccount(ctx, userID, asset)
		}
		return nil, fmt.Errorf("failed to create account: %w", err)
	}

	log.Printf("✅ Created account: user=%s, asset=%s", userID, asset)
	return &account, nil
}

// FindUserByWallet finds a user ID by wallet address
func (l *Ledger) FindUserByWallet(ctx context.Context, walletAddress string) (string, error) {
	query := `
	SELECT user_id 
	FROM wallets 
	WHERE address = $1 
	LIMIT 1;
	`

	var userID string
	err := l.db.QueryRowContext(ctx, query, walletAddress).Scan(&userID)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil // No user found for this wallet address
		}
		return "", fmt.Errorf("failed to find user by wallet: %w", err)
	}

	return userID, nil
}

// GetAccount retrieves an account by user ID and asset
func (l *Ledger) GetAccount(ctx context.Context, userID, asset string) (*Account, error) {
	query := `
	SELECT id, user_id, asset, balance, created_at
	FROM accounts
	WHERE user_id = $1 AND asset = $2;
	`

	var account Account
	err := l.db.QueryRowContext(ctx, query, userID, asset).Scan(
		&account.ID,
		&account.UserID,
		&account.Asset,
		&account.Balance,
		&account.CreatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("account not found for user %s with asset %s", userID, asset)
		}
		return nil, fmt.Errorf("failed to get account: %w", err)
	}

	return &account, nil
}

// GetBalance returns the balance for a user's asset
func (l *Ledger) GetBalance(ctx context.Context, userID, asset string) (float64, error) {
	account, err := l.GetAccount(ctx, userID, asset)
	if err != nil {
		return 0, err
	}
	return account.Balance, nil
}

// GetAllAccounts returns all accounts for a user
func (l *Ledger) GetAllAccounts(ctx context.Context, userID string) ([]Account, error) {
	query := `
	SELECT id, user_id, asset, balance, created_at
	FROM accounts
	WHERE user_id = $1
	ORDER BY created_at DESC;
	`

	rows, err := l.db.QueryContext(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get accounts: %w", err)
	}
	defer rows.Close()

	var accounts []Account
	for rows.Next() {
		var account Account
		err := rows.Scan(
			&account.ID,
			&account.UserID,
			&account.Asset,
			&account.Balance,
			&account.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan account: %w", err)
		}
		accounts = append(accounts, account)
	}

	return accounts, nil
}

// CreditAccount adds funds to an account (for deposits)
func (l *Ledger) CreditAccount(ctx context.Context, userID, asset string, amount float64, reference, txType string) error {
	if amount <= 0 {
		return errors.New("amount must be positive")
	}

	// Start a transaction
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Ensure account exists
	_, err = l.CreateAccount(ctx, userID, asset)
	if err != nil {
		return fmt.Errorf("failed to ensure account exists: %w", err)
	}

	// Update account balance
	updateQuery := `
	UPDATE accounts
	SET balance = balance + $1, updated_at = NOW()
	WHERE user_id = $2 AND asset = $3
	RETURNING balance;
	`

	var newBalance float64
	err = tx.QueryRowContext(ctx, updateQuery, amount, userID, asset).Scan(&newBalance)
	if err != nil {
		return fmt.Errorf("failed to update balance: %w", err)
	}

	// Insert ledger entry
	entryQuery := `
	INSERT INTO ledger_entries (user_id, asset, credit, reference, type)
	VALUES ($1, $2, $3, $4, $5)
	RETURNING id;
	`

	var entryID string
	err = tx.QueryRowContext(ctx, entryQuery, userID, asset, amount, reference, txType).Scan(&entryID)
	if err != nil {
		return fmt.Errorf("failed to insert ledger entry: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("💰 Credited %f %s to user %s (tx: %s, balance: %f)", amount, asset, userID, reference, newBalance)
	return nil
}

// DebitAccount subtracts funds from an account (for withdrawals)
func (l *Ledger) DebitAccount(ctx context.Context, userID, asset string, amount float64, reference, txType string) error {
	if amount <= 0 {
		return errors.New("amount must be positive")
	}

	// Start a transaction
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Check if account has sufficient balance
	var currentBalance float64
	balanceQuery := `SELECT balance FROM accounts WHERE user_id = $1 AND asset = $2 FOR UPDATE;`
	err = tx.QueryRowContext(ctx, balanceQuery, userID, asset).Scan(&currentBalance)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("account not found for user %s with asset %s", userID, asset)
		}
		return fmt.Errorf("failed to get balance: %w", err)
	}

	if currentBalance < amount {
		return fmt.Errorf("insufficient balance: have %f, need %f", currentBalance, amount)
	}

	// Update account balance
	updateQuery := `
	UPDATE accounts
	SET balance = balance - $1, updated_at = NOW()
	WHERE user_id = $2 AND asset = $3
	RETURNING balance;
	`

	var newBalance float64
	err = tx.QueryRowContext(ctx, updateQuery, amount, userID, asset).Scan(&newBalance)
	if err != nil {
		return fmt.Errorf("failed to update balance: %w", err)
	}

	// Insert ledger entry
	entryQuery := `
	INSERT INTO ledger_entries (user_id, asset, debit, reference, type)
	VALUES ($1, $2, $3, $4, $5)
	RETURNING id;
	`

	var entryID string
	err = tx.QueryRowContext(ctx, entryQuery, userID, asset, amount, reference, txType).Scan(&entryID)
	if err != nil {
		return fmt.Errorf("failed to insert ledger entry: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("💸 Debited %f %s from user %s (tx: %s, balance: %f)", amount, asset, userID, reference, newBalance)
	return nil
}

// InsertTransaction inserts a transaction record (generic method)
func (l *Ledger) InsertTransaction(ctx context.Context, userID, asset string, debit, credit float64, reference, txType string) (*LedgerEntry, error) {
	// Start a transaction
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// If credit > 0, update account balance
	if credit > 0 {
		_, err = l.CreateAccount(ctx, userID, asset)
		if err != nil {
			return nil, err
		}

		updateQuery := `
		UPDATE accounts
		SET balance = balance + $1, updated_at = NOW()
		WHERE user_id = $2 AND asset = $3;
		`
		_, err = tx.ExecContext(ctx, updateQuery, credit, userID, asset)
		if err != nil {
			return nil, fmt.Errorf("failed to update balance: %w", err)
		}
	}

	// If debit > 0, update account balance
	if debit > 0 {
		updateQuery := `
		UPDATE accounts
		SET balance = balance - $1, updated_at = NOW()
		WHERE user_id = $2 AND asset = $3;
		`
		_, err = tx.ExecContext(ctx, updateQuery, debit, userID, asset)
		if err != nil {
			return nil, fmt.Errorf("failed to update balance: %w", err)
		}
	}

	// Insert ledger entry
	entryQuery := `
	INSERT INTO ledger_entries (user_id, asset, debit, credit, reference, type)
	VALUES ($1, $2, $3, $4, $5, $6)
	RETURNING id, created_at;
	`

	var entry LedgerEntry
	entry.UserID = userID
	entry.Asset = asset
	entry.Debit = debit
	entry.Credit = credit
	entry.Reference = reference
	entry.Type = txType

	err = tx.QueryRowContext(ctx, entryQuery, userID, asset, debit, credit, reference, txType).Scan(
		&entry.ID,
		&entry.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert ledger entry: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Printf("📝 Inserted transaction: user=%s, asset=%s, debit=%f, credit=%f, ref=%s, type=%s",
		userID, asset, debit, credit, reference, txType)

	return &entry, nil
}

// GetTransactionHistory returns all transactions for a user
func (l *Ledger) GetTransactionHistory(ctx context.Context, userID string, limit, offset int) ([]LedgerEntry, error) {
	query := `
	SELECT id, user_id, asset, debit, credit, reference, type, created_at
	FROM ledger_entries
	WHERE user_id = $1
	ORDER BY created_at DESC
	LIMIT $2 OFFSET $3;
	`

	rows, err := l.db.QueryContext(ctx, query, userID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction history: %w", err)
	}
	defer rows.Close()

	var entries []LedgerEntry
	for rows.Next() {
		var entry LedgerEntry
		err := rows.Scan(
			&entry.ID,
			&entry.UserID,
			&entry.Asset,
			&entry.Debit,
			&entry.Credit,
			&entry.Reference,
			&entry.Type,
			&entry.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan entry: %w", err)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// GetTransactionByReference retrieves a transaction by its reference (tx hash)
func (l *Ledger) GetTransactionByReference(ctx context.Context, reference string) (*LedgerEntry, error) {
	query := `
	SELECT id, user_id, asset, debit, credit, reference, type, created_at
	FROM ledger_entries
	WHERE reference = $1;
	`

	var entry LedgerEntry
	err := l.db.QueryRowContext(ctx, query, reference).Scan(
		&entry.ID,
		&entry.UserID,
		&entry.Asset,
		&entry.Debit,
		&entry.Credit,
		&entry.Reference,
		&entry.Type,
		&entry.CreatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}

	return &entry, nil
}

// Close closes the database connection
func (l *Ledger) Close() error {
	return l.db.Close()
}
