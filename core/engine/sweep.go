package engine

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/gagliardetto/solana-go/programs/token"
	"github.com/gagliardetto/solana-go/rpc"
	confirm "github.com/gagliardetto/solana-go/rpc/sendAndConfirmTransaction"
	"github.com/gagliardetto/solana-go/rpc/ws"
)

// Associated Token Account Program ID
const AssociatedTokenAccountProgramID = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"

// GetAssociatedTokenAddress derives the associated token account address
func GetAssociatedTokenAddress(wallet, mint solana.PublicKey) (solana.PublicKey, error) {
	// The seeds for ATA derivation: wallet address, token program ID, mint address
	seeds := [][]byte{
		wallet.Bytes(),
		token.ProgramID.Bytes(),
		mint.Bytes(),
	}

	ataProgramID := solana.MustPublicKeyFromBase58(AssociatedTokenAccountProgramID)

	// FindProgramAddress derives the PDA using the seeds
	ata, _, err := solana.FindProgramAddress(seeds, ataProgramID)
	if err != nil {
		return solana.PublicKey{}, err
	}

	return ata, nil
}

// CreateAssociatedTokenAccountInstruction creates an instruction to create an ATA
func CreateAssociatedTokenAccountInstruction(
	payer, wallet, mint solana.PublicKey,
) solana.Instruction {
	ata, _ := GetAssociatedTokenAddress(wallet, mint)

	// Build the account metas for the create instruction
	accounts := solana.AccountMetaSlice{
		solana.NewAccountMeta(payer, true, true),                     // [0] payer (signer, writable)
		solana.NewAccountMeta(ata, false, true),                      // [1] associated token account (writable)
		solana.NewAccountMeta(wallet, false, false),                  // [2] wallet (read-only)
		solana.NewAccountMeta(mint, false, false),                    // [3] mint (read-only)
		solana.NewAccountMeta(solana.SystemProgramID, false, false),  // [4] system program
		solana.NewAccountMeta(token.ProgramID, false, false),         // [5] token program
		solana.NewAccountMeta(solana.SysVarRentPubkey, false, false), // [6] rent sysvar
	}

	// Create the instruction using solana.NewInstruction
	programID := solana.MustPublicKeyFromBase58(AssociatedTokenAccountProgramID)

	return solana.NewInstruction(
		programID,
		accounts,
		[]byte{0}, // Create instruction discriminator
	)
}

type SweepEngine struct {
	db         *sql.DB
	rpcClient  *rpc.Client
	wsClient   *ws.Client
	hotWallet  solana.PrivateKey
	config     SweepConfig
	tokenCache *TokenCache
}

type SweepConfig struct {
	MinBalanceSOL   float64 // Minimum SOL balance before sweeping (in SOL)
	MinBalanceToken float64 // Minimum token balance before sweeping
	SweepInterval   time.Duration
	BatchSize       int
	MaxRetries      int
	RetryDelay      time.Duration
	GasMultiplier   float64 // Multiplier for gas fees (e.g., 1.1 = 10% extra)
}

type WalletToSweep struct {
	ID            string
	UserID        string
	Address       string
	PrivateKey    string // Encrypted private key
	SOLBalance    float64
	TokenBalances []TokenBalance
}

type TokenBalance struct {
	Mint         string
	Balance      float64
	TokenAccount string
}

// TokenInfo caches token decimals and other metadata
type TokenInfo struct {
	Mint     solana.PublicKey
	Decimals uint8
	Name     string
	Symbol   string
}

// TokenCache provides caching for token metadata
type TokenCache struct {
	mu        sync.RWMutex
	tokens    map[string]*TokenInfo
	expiry    time.Duration
	lastFetch map[string]time.Time
}

// NewTokenCache creates a new token cache
func NewTokenCache(expiry time.Duration) *TokenCache {
	return &TokenCache{
		tokens:    make(map[string]*TokenInfo),
		expiry:    expiry,
		lastFetch: make(map[string]time.Time),
	}
}

type SweepResult struct {
	WalletID      string
	UserID        string
	TransactionID string
	SOLAmount     float64
	TokenAmounts  map[string]float64
	Status        string
	Error         string
	Timestamp     time.Time
}

// NewSweepEngine creates a new sweep engine
func NewSweepEngine(db *sql.DB, rpcURL, wsURL string, hotWalletPrivateKey string, config SweepConfig) (*SweepEngine, error) {
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

	engine := &SweepEngine{
		db:         db,
		rpcClient:  rpcClient,
		wsClient:   wsClient,
		hotWallet:  hotWallet,
		config:     config,
		tokenCache: NewTokenCache(1 * time.Hour),
	}

	// Initialize sweep tracking table
	if err := engine.initSweepTable(); err != nil {
		return nil, fmt.Errorf("failed to init sweep table: %w", err)
	}

	log.Println("✅ Sweep engine initialized")
	return engine, nil
}

// initSweepTable creates tables for tracking sweeps
func (s *SweepEngine) initSweepTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS sweeps (
		id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
		wallet_id UUID NOT NULL,
		user_id UUID NOT NULL,
		transaction_signature TEXT NOT NULL,
		sol_amount NUMERIC(36,18) DEFAULT 0,
		token_amounts JSONB DEFAULT '{}',
		status TEXT NOT NULL,
		error_message TEXT,
		created_at TIMESTAMP DEFAULT NOW(),
		completed_at TIMESTAMP,
		INDEX idx_sweeps_wallet_id (wallet_id),
		INDEX idx_sweeps_status (status),
		INDEX idx_sweeps_created_at (created_at)
	);`

	_, err := s.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create sweeps table: %w", err)
	}

	// Add last_sweep_at column to wallets table
	alterQuery := `
	ALTER TABLE wallets 
	ADD COLUMN IF NOT EXISTS last_sweep_at TIMESTAMP,
	ADD COLUMN IF NOT EXISTS last_sweep_tx TEXT;
	`
	_, err = s.db.Exec(alterQuery)
	if err != nil {
		log.Printf("Warning: failed to alter wallets table: %v", err)
	}

	return nil
}

// Start begins the sweep engine loop
func (s *SweepEngine) Start(ctx context.Context) {
	log.Println("🔄 Sweep engine started")
	ticker := time.NewTicker(s.config.SweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Sweep engine stopped")
			return
		case <-ticker.C:
			if err := s.processWallets(ctx); err != nil {
				log.Printf("❌ Error processing wallets: %v", err)
			}
		}
	}
}

// processWallets finds and sweeps eligible wallets
func (s *SweepEngine) processWallets(ctx context.Context) error {
	// Get wallets that need sweeping
	wallets, err := s.getWalletsToSweep(ctx)
	if err != nil {
		return fmt.Errorf("failed to get wallets to sweep: %w", err)
	}

	if len(wallets) == 0 {
		log.Println("No wallets to sweep")
		return nil
	}

	log.Printf("Found %d wallets to sweep", len(wallets))

	// Process wallets in batches
	for i := 0; i < len(wallets); i += s.config.BatchSize {
		end := i + s.config.BatchSize
		if end > len(wallets) {
			end = len(wallets)
		}

		batch := wallets[i:end]
		if err := s.sweepBatch(ctx, batch); err != nil {
			log.Printf("❌ Error sweeping batch: %v", err)
		}

		// Add delay between batches to avoid rate limiting
		time.Sleep(2 * time.Second)
	}

	return nil
}

// getWalletsToSweep retrieves wallets with balances above thresholds
func (s *SweepEngine) getWalletsToSweep(ctx context.Context) ([]WalletToSweep, error) {
	query := `
	SELECT 
		w.id,
		w.user_id,
		w.address,
		w.encrypted_private_key,
		COALESCE(a.balance, 0) as sol_balance
	FROM wallets w
	LEFT JOIN accounts a ON w.user_id = a.user_id AND a.asset = 'SOL'
	WHERE 
		(w.last_sweep_at IS NULL OR w.last_sweep_at < NOW() - INTERVAL '1 day')
		AND COALESCE(a.balance, 0) > $1
	ORDER BY COALESCE(a.balance, 0) DESC
	LIMIT $2;
	`

	rows, err := s.db.QueryContext(ctx, query, s.config.MinBalanceSOL, 100)
	if err != nil {
		return nil, fmt.Errorf("failed to query wallets: %w", err)
	}
	defer rows.Close()

	var wallets []WalletToSweep
	for rows.Next() {
		var w WalletToSweep
		var encryptedKey string

		err := rows.Scan(&w.ID, &w.UserID, &w.Address, &encryptedKey, &w.SOLBalance)
		if err != nil {
			log.Printf("Error scanning wallet: %v", err)
			continue
		}

		// Decrypt private key (implement your decryption logic)
		w.PrivateKey, err = s.decryptPrivateKey(encryptedKey)
		if err != nil {
			log.Printf("Failed to decrypt private key for wallet %s: %v", w.ID, err)
			continue
		}

		// Get token balances
		tokenBalances, err := s.getTokenBalances(ctx, w.Address)
		if err != nil {
			log.Printf("Failed to get token balances for wallet %s: %v", w.ID, err)
		}
		w.TokenBalances = tokenBalances

		wallets = append(wallets, w)
	}

	return wallets, nil
}

// sweepBatch sweeps a batch of wallets
func (s *SweepEngine) sweepBatch(ctx context.Context, wallets []WalletToSweep) error {
	for _, wallet := range wallets {
		if err := s.sweepWallet(ctx, &wallet); err != nil {
			log.Printf("❌ Failed to sweep wallet %s: %v", wallet.ID, err)
			s.recordSweepFailure(&wallet, err)
		}
		time.Sleep(1 * time.Second) // Rate limiting
	}
	return nil
}

// sweepWallet sweeps a single wallet
func (s *SweepEngine) sweepWallet(ctx context.Context, wallet *WalletToSweep) error {
	log.Printf("💰 Sweeping wallet %s (User: %s, Balance: %f SOL)",
		wallet.Address, wallet.UserID, wallet.SOLBalance)

	// Parse wallet private key
	userWallet, err := solana.PrivateKeyFromBase58(wallet.PrivateKey)
	if err != nil {
		return fmt.Errorf("invalid private key: %w", err)
	}

	// Get recent blockhash
	recent, err := s.rpcClient.GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
	if err != nil {
		return fmt.Errorf("failed to get blockhash: %w", err)
	}

	var instructions []solana.Instruction

	// Add SOL transfer instruction
	if wallet.SOLBalance > s.config.MinBalanceSOL {
		// Calculate amount to sweep (leave some for gas if needed)
		amountToSweep := wallet.SOLBalance - 0.001 // Leave 0.001 SOL for future gas
		if amountToSweep < 0 {
			amountToSweep = wallet.SOLBalance
		}

		lamports := uint64(amountToSweep * float64(solana.LAMPORTS_PER_SOL))
		solInstruction := system.NewTransferInstruction(
			lamports,
			userWallet.PublicKey(),
			s.hotWallet.PublicKey(),
		).Build()
		instructions = append(instructions, solInstruction)
	}

	// Add token transfer instructions
	tokenInstructions, err := s.buildTokenTransferInstructions(ctx, wallet, userWallet.PublicKey())
	if err != nil {
		log.Printf("Failed to build token instructions: %v", err)
	} else {
		instructions = append(instructions, tokenInstructions...)
	}

	if len(instructions) == 0 {
		log.Printf("No instructions to sweep for wallet %s", wallet.Address)
		return nil
	}

	// Create and sign transaction
	tx, err := solana.NewTransaction(
		instructions,
		recent.Value.Blockhash,
		solana.TransactionPayer(userWallet.PublicKey()),
	)
	if err != nil {
		return fmt.Errorf("failed to create transaction: %w", err)
	}

	// Sign transaction
	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key.Equals(userWallet.PublicKey()) {
			return &userWallet
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to sign transaction: %w", err)
	}

	// Send and confirm transaction with retries
	var signature solana.Signature
	for i := 0; i <= s.config.MaxRetries; i++ {
		signature, err = confirm.SendAndConfirmTransaction(
			ctx,
			s.rpcClient,
			s.wsClient,
			tx,
		)
		if err == nil {
			break
		}

		log.Printf("Retry %d/%d: Failed to send transaction: %v", i+1, s.config.MaxRetries, err)
		time.Sleep(s.config.RetryDelay)
	}

	if err != nil {
		return fmt.Errorf("failed to send transaction after %d retries: %w", s.config.MaxRetries, err)
	}

	log.Printf("✅ Successfully swept wallet %s: TX %s", wallet.Address, signature.String())

	// Record successful sweep
	return s.recordSweepSuccess(wallet, signature.String(), wallet.SOLBalance, nil)
}

// buildTokenTransferInstructions creates token transfer instructions
func (s *SweepEngine) buildTokenTransferInstructions(ctx context.Context, wallet *WalletToSweep, userPublicKey solana.PublicKey) ([]solana.Instruction, error) {
	var instructions []solana.Instruction

	for _, tokenBalance := range wallet.TokenBalances {
		if tokenBalance.Balance <= s.config.MinBalanceToken {
			continue
		}

		// Parse mint public key
		mint, err := solana.PublicKeyFromBase58(tokenBalance.Mint)
		if err != nil {
			log.Printf("Invalid mint address %s: %v", tokenBalance.Mint, err)
			continue
		}

		// Find or create token account for hot wallet
		hotTokenAccount, err := s.findOrCreateTokenAccount(ctx, s.hotWallet.PublicKey(), mint)
		if err != nil {
			log.Printf("Failed to get token account for hot wallet: %v", err)
			continue
		}

		// Get user's token account
		userTokenAccount, err := s.findUserTokenAccount(ctx, userPublicKey, mint)
		if err != nil {
			log.Printf("Failed to find user token account: %v", err)
			continue
		}

		// Calculate amount in base units (consider decimals)
		amount := s.calculateTokenAmount(tokenBalance.Balance, tokenBalance.Mint)

		// Create transfer instruction
		transferIx := token.NewTransferInstruction(
			amount,
			userTokenAccount,
			hotTokenAccount,
			userPublicKey,
			[]solana.PublicKey{},
		).Build()

		instructions = append(instructions, transferIx)
	}

	return instructions, nil
}

// findOrCreateTokenAccount finds or creates a token account for a wallet
func (s *SweepEngine) findOrCreateTokenAccount(ctx context.Context, owner solana.PublicKey, mint solana.PublicKey) (solana.PublicKey, error) {
	ata, err := GetAssociatedTokenAddress(owner, mint)
	if err != nil {
		return solana.PublicKey{}, fmt.Errorf("failed to get associated token address: %w", err)
	}

	// Check if the token account exists
	accountInfo, err := s.rpcClient.GetAccountInfo(ctx, ata)
	if err != nil || accountInfo.Value == nil {
		// Account doesn't exist, attempt to create it (only for hot wallet)
		if owner.Equals(s.hotWallet.PublicKey()) {
			return s.createTokenAccount(ctx, owner, mint, ata)
		}
		return ata, nil
	}

	return ata, nil
}

// createTokenAccount creates a new token account
func (s *SweepEngine) createTokenAccount(ctx context.Context, owner solana.PublicKey, mint solana.PublicKey, ata solana.PublicKey) (solana.PublicKey, error) {
	// Get recent blockhash
	recent, err := s.rpcClient.GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
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
	if owner.Equals(s.hotWallet.PublicKey()) {
		signer = &s.hotWallet
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
		s.rpcClient,
		s.wsClient,
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
func (s *SweepEngine) findUserTokenAccount(ctx context.Context, owner solana.PublicKey, mint solana.PublicKey) (solana.PublicKey, error) {
	ata, err := GetAssociatedTokenAddress(owner, mint)
	if err != nil {
		return solana.PublicKey{}, fmt.Errorf("failed to get associated token address: %w", err)
	}
	return ata, nil
}

// calculateTokenAmount calculates token amount in base units
func (s *SweepEngine) calculateTokenAmount(amount float64, mint string) uint64 {
	// Get token decimals from cache
	decimals := s.getTokenDecimals(mint)

	// Calculate amount in base units using math.Pow
	multiplier := 1.0
	for i := 0; i < int(decimals); i++ {
		multiplier *= 10
	}
	baseAmount := amount * multiplier

	return uint64(baseAmount)
}

// getTokenDecimals retrieves token decimals from cache or RPC
func (s *SweepEngine) getTokenDecimals(mint string) uint8 {
	// Check cache first
	if s.tokenCache != nil {
		if info := s.tokenCache.Get(mint); info != nil {
			return info.Decimals
		}
	}

	// Fetch from RPC
	decimals, err := s.fetchTokenDecimals(mint)
	if err != nil {
		log.Printf("Failed to fetch decimals for mint %s, using default 9: %v", mint, err)
		return 9 // Default to 9 decimals
	}

	// Cache the result
	if s.tokenCache != nil {
		s.tokenCache.Set(mint, &TokenInfo{
			Mint:     solana.MustPublicKeyFromBase58(mint),
			Decimals: decimals,
		})
	}

	return decimals
}

// fetchTokenDecimals fetches token decimals from the blockchain
func (s *SweepEngine) fetchTokenDecimals(mint string) (uint8, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mintPubkey, err := solana.PublicKeyFromBase58(mint)
	if err != nil {
		return 0, fmt.Errorf("invalid mint address: %w", err)
	}

	// Get token mint account info
	accountInfo, err := s.rpcClient.GetAccountInfo(ctx, mintPubkey)
	if err != nil {
		return 0, fmt.Errorf("failed to get mint account: %w", err)
	}

	if accountInfo.Value == nil {
		return 0, fmt.Errorf("mint account not found")
	}

	// Decode mint account
	var mintAccount token.Mint
	if err := mintAccount.Decode(accountInfo.Value.Data.GetBinary()); err != nil {
		return 0, fmt.Errorf("failed to decode mint account: %w", err)
	}

	return mintAccount.Decimals, nil
}

// getTokenBalances retrieves token balances for a wallet
func (s *SweepEngine) getTokenBalances(ctx context.Context, walletAddress string) ([]TokenBalance, error) {
	// Query token accounts from database or RPC
	query := `
	SELECT t.mint, t.balance, t.token_account
	FROM token_balances t
	WHERE t.wallet_address = $1 AND t.balance > 0
	`

	rows, err := s.db.QueryContext(ctx, query, walletAddress)
	if err != nil {
		// If no table, return empty slice
		return []TokenBalance{}, nil
	}
	defer rows.Close()

	var balances []TokenBalance
	for rows.Next() {
		var tb TokenBalance
		err := rows.Scan(&tb.Mint, &tb.Balance, &tb.TokenAccount)
		if err != nil {
			continue
		}
		balances = append(balances, tb)
	}

	return balances, nil
}

// recordSweepSuccess records a successful sweep
func (s *SweepEngine) recordSweepSuccess(wallet *WalletToSweep, txSignature string, solAmount float64, tokenAmounts map[string]float64) error {
	// Update sweeps table
	query := `
	INSERT INTO sweeps (wallet_id, user_id, transaction_signature, sol_amount, token_amounts, status, completed_at)
	VALUES ($1, $2, $3, $4, $5, $6, NOW())
	`

	_, err := s.db.Exec(query, wallet.ID, wallet.UserID, txSignature, solAmount, tokenAmounts, "completed")
	if err != nil {
		return fmt.Errorf("failed to record sweep: %w", err)
	}

	// Update wallet's last sweep info
	updateQuery := `
	UPDATE wallets 
	SET last_sweep_at = NOW(), last_sweep_tx = $1 
	WHERE id = $2
	`
	_, err = s.db.Exec(updateQuery, txSignature, wallet.ID)
	if err != nil {
		log.Printf("Warning: failed to update wallet last_sweep: %v", err)
	}

	return nil
}

// recordSweepFailure records a failed sweep
func (s *SweepEngine) recordSweepFailure(wallet *WalletToSweep, err error) {
	query := `
	INSERT INTO sweeps (wallet_id, user_id, transaction_signature, status, error_message, completed_at)
	VALUES ($1, $2, $3, $4, $5, NOW())
	`

	_, _ = s.db.Exec(query, wallet.ID, wallet.UserID, "", "failed", err.Error())
}

// decryptPrivateKey decrypts an encrypted private key
func (s *SweepEngine) decryptPrivateKey(encryptedKey string) (string, error) {
	// Implement your decryption logic here
	// This could use AES, AWS KMS, HashiCorp Vault, etc.
	// For now, return as-is (not secure for production!)
	return encryptedKey, nil
}

// GetSweepHistory retrieves sweep history for a user
func (s *SweepEngine) GetSweepHistory(ctx context.Context, userID string, limit, offset int) ([]SweepResult, error) {
	query := `
	SELECT 
		wallet_id,
		user_id,
		transaction_signature,
		sol_amount,
		token_amounts,
		status,
		error_message,
		completed_at
	FROM sweeps
	WHERE user_id = $1
	ORDER BY completed_at DESC
	LIMIT $2 OFFSET $3
	`

	rows, err := s.db.QueryContext(ctx, query, userID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to get sweep history: %w", err)
	}
	defer rows.Close()

	var results []SweepResult
	for rows.Next() {
		var result SweepResult
		err := rows.Scan(
			&result.WalletID,
			&result.UserID,
			&result.TransactionID,
			&result.SOLAmount,
			&result.TokenAmounts,
			&result.Status,
			&result.Error,
			&result.Timestamp,
		)
		if err != nil {
			log.Printf("Error scanning sweep result: %v", err)
			continue
		}
		results = append(results, result)
	}

	return results, nil
}

// GetPendingSweeps returns sweeps that need to be retried
func (s *SweepEngine) GetPendingSweeps(ctx context.Context) ([]SweepResult, error) {
	query := `
	SELECT 
		wallet_id,
		user_id,
		transaction_signature,
		sol_amount,
		token_amounts,
		status,
		error_message,
		created_at
	FROM sweeps
	WHERE status = 'failed' 
	AND created_at > NOW() - INTERVAL '24 hours'
	ORDER BY created_at ASC
	LIMIT 10
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending sweeps: %w", err)
	}
	defer rows.Close()

	var results []SweepResult
	for rows.Next() {
		var result SweepResult
		err := rows.Scan(
			&result.WalletID,
			&result.UserID,
			&result.TransactionID,
			&result.SOLAmount,
			&result.TokenAmounts,
			&result.Status,
			&result.Error,
			&result.Timestamp,
		)
		if err != nil {
			log.Printf("Error scanning sweep result: %v", err)
			continue
		}
		results = append(results, result)
	}

	return results, nil
}

// Close closes the sweep engine connections
func (s *SweepEngine) Close() error {
	if s.wsClient != nil {
		s.wsClient.Close()
	}
	return nil
}

// TokenCache methods
func (c *TokenCache) Get(mint string) *TokenInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	info, exists := c.tokens[mint]
	if !exists {
		return nil
	}

	// Check if expired
	if lastFetch, ok := c.lastFetch[mint]; ok {
		if time.Since(lastFetch) > c.expiry {
			return nil
		}
	}

	return info
}

func (c *TokenCache) Set(mint string, info *TokenInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.tokens[mint] = info
	c.lastFetch[mint] = time.Now()
}
