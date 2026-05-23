package engine

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/gagliardetto/solana-go/programs/token"
	"github.com/gagliardetto/solana-go/rpc"
	confirm "github.com/gagliardetto/solana-go/rpc/sendAndConfirmTransaction"
	"github.com/gagliardetto/solana-go/rpc/ws"
)

type WithdrawalEngine struct {
	db        *sql.DB
	rpcClient *rpc.Client
	wsClient  *ws.Client
	hotWallet solana.PrivateKey
	config    WithdrawalConfig
}

type WithdrawalConfig struct {
	MinWithdrawalSOL       float64
	MaxWithdrawalSOL       float64
	MinWithdrawalToken     float64
	MaxWithdrawalToken     float64
	DailyLimitPerUserSOL   float64
	DailyLimitPerUserToken float64
	ProcessingInterval     time.Duration
	MaxRetries             int
	RetryDelay             time.Duration
	ConfirmationsRequired  int
	GasMultiplier          float64
}

type WithdrawalRequest struct {
	ID              string
	UserID          string
	WalletAddress   string
	Asset           string
	Mint            string
	Amount          float64
	Status          string
	TransactionHash string
	ErrorMessage    string
	CreatedAt       time.Time
	ProcessedAt     *time.Time
	RetryCount      int
}

type WithdrawalResult struct {
	RequestID       string
	TransactionHash string
	Status          string
	Error           string
	ProcessedAt     time.Time
}

// NewWithdrawalEngine creates a new withdrawal engine
func NewWithdrawalEngine(db *sql.DB, rpcURL, wsURL string, hotWalletPrivateKey string, config WithdrawalConfig) (*WithdrawalEngine, error) {
	// Decode hot wallet private key
	hotWallet, err := solana.PrivateKeyFromBase58(hotWalletPrivateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse hot wallet private key: %w", err)
	}

	// Create RPC client
	rpcClient := rpc.New(rpcURL)

	// Create WS client
	wsClient, err := ws.Connect(context.Background(), wsURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to websocket: %w", err)
	}

	engine := &WithdrawalEngine{
		db:        db,
		rpcClient: rpcClient,
		wsClient:  wsClient,
		hotWallet: hotWallet,
		config:    config,
	}

	// Initialize withdrawal tables
	if err := engine.initWithdrawalTables(); err != nil {
		return nil, fmt.Errorf("failed to init withdrawal tables: %w", err)
	}

	log.Println("✅ Withdrawal engine initialized")
	return engine, nil
}

// initWithdrawalTables creates necessary tables for withdrawals
func (e *WithdrawalEngine) initWithdrawalTables() error {
	// Create withdrawal_requests table
	requestsTable := `
	CREATE TABLE IF NOT EXISTS withdrawal_requests (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		user_id UUID NOT NULL,
		wallet_address TEXT NOT NULL,
		asset TEXT NOT NULL,
		mint TEXT,
		amount NUMERIC(36,18) NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending',
		transaction_hash TEXT,
		error_message TEXT,
		retry_count INT DEFAULT 0,
		created_at TIMESTAMP DEFAULT NOW(),
		processed_at TIMESTAMP,
		INDEX idx_withdrawal_user_id (user_id),
		INDEX idx_withdrawal_status (status),
		INDEX idx_withdrawal_created_at (created_at)
	);`

	if _, err := e.db.Exec(requestsTable); err != nil {
		return fmt.Errorf("failed to create withdrawal_requests table: %w", err)
	}

	return nil
}

// Start begins the withdrawal processing loop
func (e *WithdrawalEngine) Start(ctx context.Context) {
	log.Println("🔄 Withdrawal engine started")
	ticker := time.NewTicker(e.config.ProcessingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Withdrawal engine stopped")
			return
		case <-ticker.C:
			if err := e.processPendingWithdrawals(ctx); err != nil {
				log.Printf("❌ Error processing withdrawals: %v", err)
			}
		}
	}
}

// SubmitWithdrawal submits a new withdrawal request
func (e *WithdrawalEngine) SubmitWithdrawal(ctx context.Context, userID, walletAddress, asset, mint string, amount float64) (*WithdrawalRequest, error) {
	// Validate withdrawal
	if err := e.validateWithdrawal(ctx, userID, asset, amount); err != nil {
		return nil, fmt.Errorf("withdrawal validation failed: %w", err)
	}

	// Check user balance
	balance, err := e.getUserBalance(ctx, userID, asset)
	if err != nil {
		return nil, fmt.Errorf("failed to get user balance: %w", err)
	}

	if balance < amount {
		return nil, fmt.Errorf("insufficient balance: have %f, need %f", balance, amount)
	}

	// Create withdrawal request
	query := `
	INSERT INTO withdrawal_requests (user_id, wallet_address, asset, mint, amount, status)
	VALUES ($1, $2, $3, $4, $5, 'pending')
	RETURNING id, created_at;
	`

	var request WithdrawalRequest
	request.UserID = userID
	request.WalletAddress = walletAddress
	request.Asset = asset
	request.Mint = mint
	request.Amount = amount

	err = e.db.QueryRowContext(ctx, query, userID, walletAddress, asset, mint, amount).Scan(
		&request.ID, &request.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create withdrawal request: %w", err)
	}

	request.Status = "pending"
	log.Printf("📝 New withdrawal request: ID=%s, User=%s, Amount=%f %s",
		request.ID, userID, amount, asset)

	return &request, nil
}

// validateWithdrawal validates the withdrawal request
func (e *WithdrawalEngine) validateWithdrawal(ctx context.Context, userID, asset string, amount float64) error {
	// Check minimum withdrawal
	if asset == "SOL" {
		if amount < e.config.MinWithdrawalSOL {
			return fmt.Errorf("amount below minimum withdrawal: %f < %f", amount, e.config.MinWithdrawalSOL)
		}
		if amount > e.config.MaxWithdrawalSOL {
			return fmt.Errorf("amount exceeds maximum withdrawal: %f > %f", amount, e.config.MaxWithdrawalSOL)
		}
	} else {
		if amount < e.config.MinWithdrawalToken {
			return fmt.Errorf("amount below minimum withdrawal: %f < %f", amount, e.config.MinWithdrawalToken)
		}
		if amount > e.config.MaxWithdrawalToken {
			return fmt.Errorf("amount exceeds maximum withdrawal: %f > %f", amount, e.config.MaxWithdrawalToken)
		}
	}

	return nil
}

// processPendingWithdrawals processes all pending withdrawal requests
func (e *WithdrawalEngine) processPendingWithdrawals(ctx context.Context) error {
	// Get pending withdrawals
	withdrawals, err := e.getPendingWithdrawals(ctx)
	if err != nil {
		return fmt.Errorf("failed to get pending withdrawals: %w", err)
	}

	if len(withdrawals) == 0 {
		return nil
	}

	log.Printf("Processing %d pending withdrawals", len(withdrawals))

	for _, withdrawal := range withdrawals {
		if err := e.processWithdrawal(ctx, &withdrawal); err != nil {
			log.Printf("❌ Failed to process withdrawal %s: %v", withdrawal.ID, err)
			e.updateWithdrawalStatus(withdrawal.ID, "failed", "", err.Error())
		}
		// Add delay between withdrawals
		time.Sleep(2 * time.Second)
	}

	return nil
}

// getPendingWithdrawals retrieves pending withdrawal requests
func (e *WithdrawalEngine) getPendingWithdrawals(ctx context.Context) ([]WithdrawalRequest, error) {
	query := `
	SELECT id, user_id, wallet_address, asset, mint, amount, status, transaction_hash, error_message, retry_count, created_at, processed_at
	FROM withdrawal_requests
	WHERE status IN ('pending', 'failed')
	AND retry_count < $1
	ORDER BY created_at ASC
	LIMIT 50;
	`

	rows, err := e.db.QueryContext(ctx, query, e.config.MaxRetries)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var withdrawals []WithdrawalRequest
	for rows.Next() {
		var w WithdrawalRequest
		var processedAt sql.NullTime
		var txHash sql.NullString
		var errorMsg sql.NullString

		err := rows.Scan(
			&w.ID, &w.UserID, &w.WalletAddress, &w.Asset, &w.Mint,
			&w.Amount, &w.Status, &txHash, &errorMsg, &w.RetryCount,
			&w.CreatedAt, &processedAt,
		)
		if err != nil {
			log.Printf("Error scanning withdrawal: %v", err)
			continue
		}

		if txHash.Valid {
			w.TransactionHash = txHash.String
		}
		if errorMsg.Valid {
			w.ErrorMessage = errorMsg.String
		}
		if processedAt.Valid {
			w.ProcessedAt = &processedAt.Time
		}

		withdrawals = append(withdrawals, w)
	}

	return withdrawals, nil
}

// processWithdrawal processes a single withdrawal
func (e *WithdrawalEngine) processWithdrawal(ctx context.Context, withdrawal *WithdrawalRequest) error {
	log.Printf("Processing withdrawal %s: %f %s to %s",
		withdrawal.ID, withdrawal.Amount, withdrawal.Asset, withdrawal.WalletAddress)

	// Debit user account first (atomic operation)
	if err := e.debitUserBalance(ctx, withdrawal); err != nil {
		return fmt.Errorf("failed to debit user balance: %w", err)
	}

	// Process on-chain transaction
	var txHash string
	var err error

	if withdrawal.Asset == "SOL" {
		txHash, err = e.processSOLWithdrawal(ctx, withdrawal)
	} else {
		txHash, err = e.processTokenWithdrawal(ctx, withdrawal)
	}

	if err != nil {
		// Refund user balance if transaction failed
		if refundErr := e.refundUserBalance(ctx, withdrawal); refundErr != nil {
			log.Printf("⚠️ Failed to refund user %s: %v", withdrawal.ID, refundErr)
		}
		return fmt.Errorf("failed to process on-chain transaction: %w", err)
	}

	// Update withdrawal status
	if err := e.updateWithdrawalStatus(withdrawal.ID, "completed", txHash, ""); err != nil {
		return fmt.Errorf("failed to update withdrawal status: %w", err)
	}

	log.Printf("✅ Withdrawal %s completed: TX=%s", withdrawal.ID, txHash)
	return nil
}

// processSOLWithdrawal processes a SOL withdrawal
func (e *WithdrawalEngine) processSOLWithdrawal(ctx context.Context, withdrawal *WithdrawalRequest) (string, error) {
	// Parse destination wallet
	destWallet, err := solana.PublicKeyFromBase58(withdrawal.WalletAddress)
	if err != nil {
		return "", fmt.Errorf("invalid destination wallet address: %w", err)
	}

	// Get recent blockhash
	recent, err := e.rpcClient.GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
	if err != nil {
		return "", fmt.Errorf("failed to get blockhash: %w", err)
	}

	// Calculate amount in lamports
	lamports := uint64(withdrawal.Amount * float64(solana.LAMPORTS_PER_SOL))

	// Create transfer instruction
	instruction := system.NewTransferInstruction(
		lamports,
		e.hotWallet.PublicKey(),
		destWallet,
	).Build()

	// Create transaction
	tx, err := solana.NewTransaction(
		[]solana.Instruction{instruction},
		recent.Value.Blockhash,
		solana.TransactionPayer(e.hotWallet.PublicKey()),
	)
	if err != nil {
		return "", fmt.Errorf("failed to create transaction: %w", err)
	}

	// Sign transaction
	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key.Equals(e.hotWallet.PublicKey()) {
			return &e.hotWallet
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("failed to sign transaction: %w", err)
	}

	// Send and confirm with retries
	var signature solana.Signature
	for i := 0; i <= e.config.MaxRetries; i++ {
		signature, err = confirm.SendAndConfirmTransaction(
			ctx,
			e.rpcClient,
			e.wsClient,
			tx,
		)
		if err == nil {
			break
		}

		log.Printf("Retry %d/%d: Failed to send SOL transaction: %v", i+1, e.config.MaxRetries, err)
		time.Sleep(e.config.RetryDelay)
	}

	if err != nil {
		return "", fmt.Errorf("failed after %d retries: %w", e.config.MaxRetries, err)
	}

	return signature.String(), nil
}

// processTokenWithdrawal processes an SPL token withdrawal
func (e *WithdrawalEngine) processTokenWithdrawal(ctx context.Context, withdrawal *WithdrawalRequest) (string, error) {
	// Parse mint address
	mint, err := solana.PublicKeyFromBase58(withdrawal.Mint)
	if err != nil {
		return "", fmt.Errorf("invalid mint address: %w", err)
	}

	// Parse destination wallet
	destWallet, err := solana.PublicKeyFromBase58(withdrawal.WalletAddress)
	if err != nil {
		return "", fmt.Errorf("invalid destination wallet address: %w", err)
	}

	// Find or create token account for destination
	destTokenAccount, err := e.findOrCreateTokenAccount(ctx, destWallet, mint)
	if err != nil {
		return "", fmt.Errorf("failed to get destination token account: %w", err)
	}

	// Get hot wallet's token account
	hotTokenAccount, err := e.findUserTokenAccount(ctx, e.hotWallet.PublicKey(), mint)
	if err != nil {
		return "", fmt.Errorf("failed to get hot wallet token account: %w", err)
	}

	// Calculate amount in base units (get decimals from token)
	decimals, err := e.getTokenDecimals(ctx, mint)
	if err != nil {
		return "", fmt.Errorf("failed to get token decimals: %w", err)
	}
	amount := uint64(withdrawal.Amount * float64(10^decimals))

	// Get recent blockhash
	recent, err := e.rpcClient.GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
	if err != nil {
		return "", fmt.Errorf("failed to get blockhash: %w", err)
	}

	// Create transfer instruction
	instruction := token.NewTransferInstruction(
		amount,
		hotTokenAccount,
		destTokenAccount,
		e.hotWallet.PublicKey(),
		[]solana.PublicKey{},
	).Build()

	// Create transaction
	tx, err := solana.NewTransaction(
		[]solana.Instruction{instruction},
		recent.Value.Blockhash,
		solana.TransactionPayer(e.hotWallet.PublicKey()),
	)
	if err != nil {
		return "", fmt.Errorf("failed to create transaction: %w", err)
	}

	// Sign transaction
	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key.Equals(e.hotWallet.PublicKey()) {
			return &e.hotWallet
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("failed to sign transaction: %w", err)
	}

	// Send and confirm with retries
	var signature solana.Signature
	for i := 0; i <= e.config.MaxRetries; i++ {
		signature, err = confirm.SendAndConfirmTransaction(
			ctx,
			e.rpcClient,
			e.wsClient,
			tx,
		)
		if err == nil {
			break
		}

		log.Printf("Retry %d/%d: Failed to send token transaction: %v", i+1, e.config.MaxRetries, err)
		time.Sleep(e.config.RetryDelay)
	}

	if err != nil {
		return "", fmt.Errorf("failed after %d retries: %w", e.config.MaxRetries, err)
	}

	return signature.String(), nil
}

// debitUserBalance debits the user's account
func (e *WithdrawalEngine) debitUserBalance(ctx context.Context, withdrawal *WithdrawalRequest) error {
	query := `
	UPDATE accounts
	SET balance = balance - $1, updated_at = NOW()
	WHERE user_id = $2 AND asset = $3 AND balance >= $1
	RETURNING balance;
	`

	var newBalance float64
	err := e.db.QueryRowContext(ctx, query, withdrawal.Amount, withdrawal.UserID, withdrawal.Asset).Scan(&newBalance)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("insufficient balance")
		}
		return fmt.Errorf("failed to debit balance: %w", err)
	}

	// Record ledger entry
	entryQuery := `
	INSERT INTO ledger_entries (user_id, asset, debit, reference, type)
	VALUES ($1, $2, $3, $4, 'withdrawal')
	`

	_, err = e.db.ExecContext(ctx, entryQuery, withdrawal.UserID, withdrawal.Asset, withdrawal.Amount, withdrawal.ID)
	if err != nil {
		return fmt.Errorf("failed to record ledger entry: %w", err)
	}

	log.Printf("💰 Debited %f %s from user %s", withdrawal.Amount, withdrawal.Asset, withdrawal.UserID)
	return nil
}

// refundUserBalance refunds the user's balance if transaction fails
func (e *WithdrawalEngine) refundUserBalance(ctx context.Context, withdrawal *WithdrawalRequest) error {
	query := `
	UPDATE accounts
	SET balance = balance + $1, updated_at = NOW()
	WHERE user_id = $2 AND asset = $3
	`

	_, err := e.db.ExecContext(ctx, query, withdrawal.Amount, withdrawal.UserID, withdrawal.Asset)
	if err != nil {
		return fmt.Errorf("failed to refund balance: %w", err)
	}

	log.Printf("💰 Refunded %f %s to user %s", withdrawal.Amount, withdrawal.Asset, withdrawal.UserID)
	return nil
}

// getUserBalance gets user's balance for an asset
func (e *WithdrawalEngine) getUserBalance(ctx context.Context, userID, asset string) (float64, error) {
	query := `
	SELECT COALESCE(balance, 0)
	FROM accounts
	WHERE user_id = $1 AND asset = $2
	`

	var balance float64
	err := e.db.QueryRowContext(ctx, query, userID, asset).Scan(&balance)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get balance: %w", err)
	}

	return balance, nil
}

// updateWithdrawalStatus updates the status of a withdrawal request
func (e *WithdrawalEngine) updateWithdrawalStatus(id, status, txHash, errorMsg string) error {
	query := `
	UPDATE withdrawal_requests
	SET status = $1, transaction_hash = $2, error_message = $3, processed_at = NOW(), retry_count = retry_count + 1
	WHERE id = $4
	`

	_, err := e.db.Exec(query, status, txHash, errorMsg, id)
	return err
}

// findOrCreateTokenAccount finds or creates a token account for a wallet
func (e *WithdrawalEngine) findOrCreateTokenAccount(ctx context.Context, owner solana.PublicKey, mint solana.PublicKey) (solana.PublicKey, error) {
	ata, err := GetAssociatedTokenAddress(owner, mint)
	if err != nil {
		return solana.PublicKey{}, fmt.Errorf("failed to get associated token address: %w", err)
	}

	// Check if the token account exists
	accountInfo, err := e.rpcClient.GetAccountInfo(ctx, ata)
	if err != nil || accountInfo.Value == nil {
		// Account doesn't exist, attempt to create it (only for hot wallet)
		if owner.Equals(e.hotWallet.PublicKey()) {
			return e.createTokenAccount(ctx, owner, mint, ata)
		}
		return ata, nil
	}

	return ata, nil
}

// createTokenAccount creates a new token account
func (e *WithdrawalEngine) createTokenAccount(ctx context.Context, owner solana.PublicKey, mint solana.PublicKey, ata solana.PublicKey) (solana.PublicKey, error) {
	// Get recent blockhash
	recent, err := e.rpcClient.GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
	if err != nil {
		return solana.PublicKey{}, fmt.Errorf("failed to get blockhash: %w", err)
	}

	// Create instruction using our local function
	createIx := CreateAssociatedTokenAccountInstruction(owner, owner, mint)

	// Create transaction
	tx, err := solana.NewTransaction(
		[]solana.Instruction{createIx},
		recent.Value.Blockhash,
		solana.TransactionPayer(owner),
	)
	if err != nil {
		return solana.PublicKey{}, fmt.Errorf("failed to create transaction: %w", err)
	}

	// Sign transaction
	var signer *solana.PrivateKey
	if owner.Equals(e.hotWallet.PublicKey()) {
		signer = &e.hotWallet
	} else {
		return ata, fmt.Errorf("cannot create token account: private key not available")
	}

	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key.Equals(owner) {
			return signer
		}
		return nil
	})
	if err != nil {
		return solana.PublicKey{}, fmt.Errorf("failed to sign transaction: %w", err)
	}

	// Send and confirm transaction
	signature, err := confirm.SendAndConfirmTransaction(
		ctx,
		e.rpcClient,
		e.wsClient,
		tx,
	)
	if err != nil {
		return solana.PublicKey{}, fmt.Errorf("failed to create token account: %w", err)
	}

	log.Printf("✅ Created token account %s for mint %s (TX: %s)", ata.String(), mint.String(), signature.String())
	time.Sleep(2 * time.Second)

	return ata, nil
}

// findUserTokenAccount finds the user's token account
func (e *WithdrawalEngine) findUserTokenAccount(ctx context.Context, owner solana.PublicKey, mint solana.PublicKey) (solana.PublicKey, error) {
	ata, err := GetAssociatedTokenAddress(owner, mint)
	if err != nil {
		return solana.PublicKey{}, fmt.Errorf("failed to get associated token address: %w", err)
	}
	return ata, nil
}

// getTokenDecimals fetches token decimals from the blockchain
func (e *WithdrawalEngine) getTokenDecimals(ctx context.Context, mint solana.PublicKey) (int, error) {
	accountInfo, err := e.rpcClient.GetAccountInfo(ctx, mint)
	if err != nil {
		return 9, fmt.Errorf("failed to get mint account: %w", err)
	}

	if accountInfo.Value == nil {
		return 9, fmt.Errorf("mint account not found")
	}

	var mintAccount token.Mint
	if err := mintAccount.Decode(accountInfo.Value.Data.GetBinary()); err != nil {
		return 9, fmt.Errorf("failed to decode mint account: %w", err)
	}

	return int(mintAccount.Decimals), nil
}

// GetWithdrawalStatus gets the status of a withdrawal request
func (e *WithdrawalEngine) GetWithdrawalStatus(ctx context.Context, withdrawalID string) (*WithdrawalRequest, error) {
	query := `
	SELECT id, user_id, wallet_address, asset, mint, amount, status, transaction_hash, error_message, created_at, processed_at
	FROM withdrawal_requests
	WHERE id = $1
	`

	var w WithdrawalRequest
	var processedAt sql.NullTime
	var txHash sql.NullString
	var errorMsg sql.NullString

	err := e.db.QueryRowContext(ctx, query, withdrawalID).Scan(
		&w.ID, &w.UserID, &w.WalletAddress, &w.Asset, &w.Mint,
		&w.Amount, &w.Status, &txHash, &errorMsg, &w.CreatedAt, &processedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("withdrawal not found")
		}
		return nil, err
	}

	if txHash.Valid {
		w.TransactionHash = txHash.String
	}
	if errorMsg.Valid {
		w.ErrorMessage = errorMsg.String
	}
	if processedAt.Valid {
		w.ProcessedAt = &processedAt.Time
	}

	return &w, nil
}

// GetUserWithdrawals gets all withdrawals for a user
func (e *WithdrawalEngine) GetUserWithdrawals(ctx context.Context, userID string, limit, offset int) ([]WithdrawalRequest, error) {
	query := `
	SELECT id, user_id, wallet_address, asset, mint, amount, status, transaction_hash, error_message, created_at, processed_at
	FROM withdrawal_requests
	WHERE user_id = $1
	ORDER BY created_at DESC
	LIMIT $2 OFFSET $3
	`

	rows, err := e.db.QueryContext(ctx, query, userID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var withdrawals []WithdrawalRequest
	for rows.Next() {
		var w WithdrawalRequest
		var processedAt sql.NullTime
		var txHash sql.NullString
		var errorMsg sql.NullString

		err := rows.Scan(
			&w.ID, &w.UserID, &w.WalletAddress, &w.Asset, &w.Mint,
			&w.Amount, &w.Status, &txHash, &errorMsg, &w.CreatedAt, &processedAt,
		)
		if err != nil {
			continue
		}

		if txHash.Valid {
			w.TransactionHash = txHash.String
		}
		if errorMsg.Valid {
			w.ErrorMessage = errorMsg.String
		}
		if processedAt.Valid {
			w.ProcessedAt = &processedAt.Time
		}

		withdrawals = append(withdrawals, w)
	}

	return withdrawals, nil
}

// CancelWithdrawal cancels a pending withdrawal request
func (e *WithdrawalEngine) CancelWithdrawal(ctx context.Context, withdrawalID, userID string) error {
	// Check if withdrawal is pending
	var status string
	query := `SELECT status FROM withdrawal_requests WHERE id = $1 AND user_id = $2`
	err := e.db.QueryRowContext(ctx, query, withdrawalID, userID).Scan(&status)
	if err != nil {
		return fmt.Errorf("withdrawal not found")
	}

	if status != "pending" {
		return fmt.Errorf("cannot cancel withdrawal with status: %s", status)
	}

	// Update status to cancelled
	updateQuery := `UPDATE withdrawal_requests SET status = 'cancelled' WHERE id = $1`
	_, err = e.db.ExecContext(ctx, updateQuery, withdrawalID)
	return err
}

// Close closes the withdrawal engine connections
func (e *WithdrawalEngine) Close() error {
	if e.wsClient != nil {
		e.wsClient.Close()
	}
	return nil
}
