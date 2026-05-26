package watcher

import (
	"bytes"
	"context"
	"cryptohub/core/ledger"
	"cryptohub/core/wallet/hdwallet"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

// DepositWatcher monitors blockchain deposits and updates ledger
type DepositWatcher struct {
	db             *sql.DB
	ledger         *ledger.Ledger
	walletManager  *hdwallet.WalletManager
	config         *WatcherConfig
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	mu             sync.RWMutex
	processedCache sync.Map // In-memory cache for processed transactions
}

// WatcherConfig holds configuration for the deposit watcher
type WatcherConfig struct {
	// Ethereum config
	EthereumRPCURL        string
	EthereumStartBlock    uint64
	EthereumCheckInterval time.Duration

	// Solana config
	SolanaRPCURL        string
	SolanaCheckInterval time.Duration
	SolanaCommitment    rpc.CommitmentType

	// Bitcoin config
	BitcoinRPCURL        string
	BitcoinRPCUser       string
	BitcoinRPCPassword   string
	BitcoinCheckInterval time.Duration

	// BNB config (same as Ethereum)
	BNBRPCURL     string
	BNBStartBlock uint64

	// Sui config
	SuiRPCURL        string
	SuiCheckInterval time.Duration

	// XRP config
	XRPRPCURL        string
	XRPCheckInterval time.Duration

	// General config
	MaxConfirmations uint64
	WorkerCount      int
	RetryAttempts    int
	RetryDelay       time.Duration
	RetryBackoff     float64
	CacheTTL         time.Duration
}

// Deposit represents a detected deposit
type Deposit struct {
	Chain         string
	TxHash        string
	FromAddress   string
	ToAddress     string
	Asset         string
	Amount        *big.Float
	BlockNumber   uint64
	Timestamp     time.Time
	Confirmations uint64
	UserID        int64
	Reference     string
}

// Bitcoin RPC Types
type BitcoinRPCClient struct {
	url      string
	user     string
	password string
	client   *http.Client
}

type BTCTransaction struct {
	TxID          string  `json:"txid"`
	Amount        float64 `json:"amount"`
	Confirmations int64   `json:"confirmations"`
	BlockHash     string  `json:"blockhash"`
	BlockHeight   int64   `json:"blockheight"`
	Time          int64   `json:"time"`
	Address       string  `json:"address"`
	Category      string  `json:"category"`
}

type BTCBlock struct {
	Hash          string                     `json:"hash"`
	Height        int64                      `json:"height"`
	Time          int64                      `json:"time"`
	Tx            []string                   `json:"tx"`
	Confirmations int64                      `json:"confirmations"`
	Transactions  map[string]*BTCTransaction `json:"-"`
}

// Sui RPC Types
type SuiRPCClient struct {
	url    string
	client *http.Client
}

// XRP RPC Types
type XRPRPCClient struct {
	url    string
	client *http.Client
}

type XRPTransaction struct {
	Hash        string `json:"hash"`
	LedgerIndex int64  `json:"ledger_index"`
	Date        int64  `json:"date"`
	TxType      string `json:"TransactionType"`
	Account     string `json:"Account"`
	Destination string `json:"Destination"`
	Amount      string `json:"Amount"`
	Fee         string `json:"Fee"`
}

// NewDepositWatcher creates a new deposit watcher
func NewDepositWatcher(
	db *sql.DB,
	ledger *ledger.Ledger,
	walletManager *hdwallet.WalletManager,
	config *WatcherConfig,
) *DepositWatcher {
	ctx, cancel := context.WithCancel(context.Background())

	if config.WorkerCount == 0 {
		config.WorkerCount = 5
	}
	if config.MaxConfirmations == 0 {
		config.MaxConfirmations = 12
	}
	if config.RetryAttempts == 0 {
		config.RetryAttempts = 3
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = 5 * time.Second
	}
	if config.RetryBackoff == 0 {
		config.RetryBackoff = 2.0
	}
	if config.CacheTTL == 0 {
		config.CacheTTL = 24 * time.Hour
	}

	return &DepositWatcher{
		db:            db,
		ledger:        ledger,
		walletManager: walletManager,
		config:        config,
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Start begins watching for deposits on all chains
func (dw *DepositWatcher) Start() error {
	// Initialize database tables for tracking
	if err := dw.initDepositTables(); err != nil {
		return fmt.Errorf("failed to init deposit tables: %w", err)
	}

	// Start watchers for each chain with error isolation
	chains := []struct {
		name    string
		watcher func() error
	}{
		{"Ethereum", dw.watchEthereum},
		{"BNB", dw.watchBNB},
		{"Bitcoin", dw.watchBitcoin},
		{"Solana", dw.watchSolana},
		{"Sui", dw.watchSui},
		{"XRP", dw.watchXRP},
	}

	for _, chain := range chains {
		dw.wg.Add(1)
		go func(chainName string, watcher func() error) {
			defer dw.wg.Done()
			log.Printf("🚀 Starting deposit watcher for %s", chainName)

			// Exponential backoff for retries
			retryCount := 0
			backoff := dw.config.RetryDelay

			for {
				select {
				case <-dw.ctx.Done():
					log.Printf("Stopping deposit watcher for %s", chainName)
					return
				default:
					if err := watcher(); err != nil {
						log.Printf("Error watching %s: %v, retrying in %v...", chainName, err, backoff)
						time.Sleep(backoff)
						retryCount++
						backoff = time.Duration(float64(backoff) * dw.config.RetryBackoff)
						if backoff > 60*time.Second {
							backoff = 60 * time.Second
						}
					} else {
						retryCount = 0
						backoff = dw.config.RetryDelay
						time.Sleep(dw.getCheckInterval(chainName))
					}
				}
			}
		}(chain.name, chain.watcher)
	}

	return nil
}

// Stop stops all deposit watchers
func (dw *DepositWatcher) Stop() {
	dw.cancel()
	dw.wg.Wait()
	log.Println("All deposit watchers stopped")
}

// initDepositTables creates necessary database tables
func (dw *DepositWatcher) initDepositTables() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS deposits (
			id SERIAL PRIMARY KEY,
			chain VARCHAR(50) NOT NULL,
			tx_hash VARCHAR(255) NOT NULL,
			user_id VARCHAR(100),
			from_address TEXT NOT NULL,
			to_address TEXT NOT NULL,
			asset VARCHAR(50) NOT NULL,
			amount NUMERIC(36,18) NOT NULL,
			block_number BIGINT NOT NULL,
			confirmations INTEGER DEFAULT 0,
			status VARCHAR(50) DEFAULT 'pending',
			reference VARCHAR(255),
			processed_at TIMESTAMP,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(chain, tx_hash)
		)`,

		`CREATE INDEX IF NOT EXISTS idx_deposits_user_id ON deposits(user_id)`,
		`CREATE INDEX IF NOT EXISTS idx_deposits_status ON deposits(status)`,
		`CREATE INDEX IF NOT EXISTS idx_deposits_chain_tx ON deposits(chain, tx_hash)`,
		`CREATE INDEX IF NOT EXISTS idx_deposits_to_address ON deposits(to_address)`,

		`CREATE TABLE IF NOT EXISTS processed_blocks (
			chain VARCHAR(50) PRIMARY KEY,
			last_block_processed BIGINT NOT NULL,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,

		`CREATE TABLE IF NOT EXISTS deposit_events (
			id SERIAL PRIMARY KEY,
			chain VARCHAR(50) NOT NULL,
			tx_hash VARCHAR(255) NOT NULL,
			user_id VARCHAR(100),
			asset VARCHAR(50) NOT NULL,
			amount NUMERIC(36,18) NOT NULL,
			status VARCHAR(50) DEFAULT 'pending',
			processed_at TIMESTAMP,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,

		`CREATE TABLE IF NOT EXISTS processed_checkpoints (
			chain VARCHAR(50) PRIMARY KEY,
			last_checkpoint BIGINT NOT NULL,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,

		`CREATE TABLE IF NOT EXISTS processed_ledgers (
			chain VARCHAR(50) PRIMARY KEY,
			last_ledger BIGINT NOT NULL,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
	}

	for _, query := range queries {
		if _, err := dw.db.Exec(query); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	return nil
}

// ==================== ETHEREUM WATCHER ====================

// watchEthereum watches for Ethereum deposits
func (dw *DepositWatcher) watchEthereum() error {
	client, err := ethclient.Dial(dw.config.EthereumRPCURL)
	if err != nil {
		return fmt.Errorf("failed to connect to Ethereum: %w", err)
	}
	defer client.Close()

	// Get last processed block
	lastBlock, err := dw.getLastProcessedBlock(hdwallet.ChainEthereum)
	if err != nil {
		lastBlock = dw.config.EthereumStartBlock
	}

	latestBlock, err := client.BlockNumber(dw.ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest block: %w", err)
	}

	if latestBlock <= lastBlock {
		return nil
	}

	// Get all user wallets for Ethereum
	userWallets, err := dw.walletManager.GetUsersWalletByChain(dw.ctx, hdwallet.ChainEthereum)
	if err != nil {
		return fmt.Errorf("failed to get users wallets: %w", err)
	}

	// Process blocks in batches
	batchSize := uint64(100)
	for fromBlock := lastBlock + 1; fromBlock <= latestBlock; fromBlock += batchSize {
		toBlock := fromBlock + batchSize - 1
		if toBlock > latestBlock {
			toBlock = latestBlock
		}

		if err := dw.processEthereumBlockRange(client, fromBlock, toBlock, userWallets); err != nil {
			log.Printf("Error processing Ethereum blocks %d-%d: %v", fromBlock, toBlock, err)
			continue
		}

		// Update last processed block
		if err := dw.updateLastProcessedBlock(hdwallet.ChainEthereum, toBlock); err != nil {
			log.Printf("Failed to update last processed block: %v", err)
		}
	}

	return nil
}

// processEthereumBlockRange processes a range of Ethereum blocks
func (dw *DepositWatcher) processEthereumBlockRange(
	client *ethclient.Client,
	fromBlock, toBlock uint64,
	userWallets []hdwallet.UserWallet,
) error {
	// Create address map for quick lookup
	addressMap := make(map[string]int64)
	for _, wallet := range userWallets {
		addressMap[wallet.Address] = wallet.UserUID
	}

	for blockNum := fromBlock; blockNum <= toBlock; blockNum++ {
		block, err := client.BlockByNumber(dw.ctx, big.NewInt(int64(blockNum)))
		if err != nil {
			return fmt.Errorf("failed to get block %d: %w", blockNum, err)
		}

		for _, tx := range block.Transactions() {
			to := tx.To()
			if to == nil {
				continue
			}

			// Check if transaction is to any user wallet
			if userID, exists := addressMap[to.Hex()]; exists {
				// Get transaction receipt for confirmation
				receipt, err := client.TransactionReceipt(dw.ctx, tx.Hash())
				if err != nil {
					continue
				}

				// Check if transaction was successful
				if receipt.Status == 1 {
					// Check confirmations
					currentBlock, _ := client.BlockNumber(dw.ctx)
					confirmations := currentBlock - receipt.BlockNumber.Uint64()

					if confirmations >= dw.config.MaxConfirmations {
						value := new(big.Float).SetInt(tx.Value())

						// Fix #1: Proper sender extraction using EVM signer recovery
						from, err := dw.getEVMSender(tx)
						if err != nil {
							log.Printf("Failed to recover sender for tx %s: %v", tx.Hash().Hex(), err)
							continue
						}

						deposit := &Deposit{
							Chain:         hdwallet.ChainEthereum,
							TxHash:        tx.Hash().Hex(),
							FromAddress:   from.Hex(),
							ToAddress:     to.Hex(),
							Asset:         "ETH",
							Amount:        value,
							BlockNumber:   blockNum,
							Timestamp:     time.Unix(int64(block.Time()), 0),
							Confirmations: confirmations,
							UserID:        userID,
						}

						// Fix #3: Use atomic deposit processing
						if err := dw.processDepositAtomic(deposit); err != nil {
							log.Printf("Failed to process deposit: %v", err)
						}
					}
				}
			}
		}
	}

	return nil
}

// getEVMSender properly extracts sender from Ethereum transaction
func (dw *DepositWatcher) getEVMSender(tx *types.Transaction) (common.Address, error) {
	signer := types.LatestSignerForChainID(tx.ChainId())
	return types.Sender(signer, tx)
}

// ==================== BNB WATCHER ====================

// watchBNB watches for BNB deposits
func (dw *DepositWatcher) watchBNB() error {
	client, err := ethclient.Dial(dw.config.BNBRPCURL)
	if err != nil {
		return fmt.Errorf("failed to connect to BNB: %w", err)
	}
	defer client.Close()

	// Get last processed block
	lastBlock, err := dw.getLastProcessedBlock(hdwallet.ChainBNB)
	if err != nil {
		lastBlock = dw.config.BNBStartBlock
	}

	latestBlock, err := client.BlockNumber(dw.ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest block: %w", err)
	}

	if latestBlock <= lastBlock {
		return nil
	}

	// Get all user wallets for BNB
	userWallets, err := dw.walletManager.GetUsersWalletByChain(dw.ctx, hdwallet.ChainBNB)
	if err != nil {
		return fmt.Errorf("failed to get BNB wallets: %w", err)
	}

	// Process blocks in batches
	batchSize := uint64(100)
	for fromBlock := lastBlock + 1; fromBlock <= latestBlock; fromBlock += batchSize {
		toBlock := fromBlock + batchSize - 1
		if toBlock > latestBlock {
			toBlock = latestBlock
		}

		if err := dw.processBNBBlockRange(client, fromBlock, toBlock, userWallets); err != nil {
			log.Printf("Error processing BNB blocks %d-%d: %v", fromBlock, toBlock, err)
			continue
		}

		// Update last processed block
		if err := dw.updateLastProcessedBlock(hdwallet.ChainBNB, toBlock); err != nil {
			log.Printf("Failed to update last processed block: %v", err)
		}
	}

	return nil
}

// processBNBBlockRange processes a range of BNB blocks
func (dw *DepositWatcher) processBNBBlockRange(
	client *ethclient.Client,
	fromBlock, toBlock uint64,
	userWallets []hdwallet.UserWallet,
) error {
	// Create address map for quick lookup
	addressMap := make(map[string]int64)
	for _, wallet := range userWallets {
		addressMap[wallet.Address] = wallet.UserUID
	}

	for blockNum := fromBlock; blockNum <= toBlock; blockNum++ {
		block, err := client.BlockByNumber(dw.ctx, big.NewInt(int64(blockNum)))
		if err != nil {
			return fmt.Errorf("failed to get block %d: %w", blockNum, err)
		}

		for _, tx := range block.Transactions() {
			to := tx.To()
			if to == nil {
				continue
			}

			if userID, exists := addressMap[to.Hex()]; exists {
				receipt, err := client.TransactionReceipt(dw.ctx, tx.Hash())
				if err != nil {
					continue
				}

				if receipt.Status == 1 {
					currentBlock, _ := client.BlockNumber(dw.ctx)
					confirmations := currentBlock - receipt.BlockNumber.Uint64()

					if confirmations >= dw.config.MaxConfirmations {
						value := new(big.Float).SetInt(tx.Value())

						from, err := dw.getEVMSender(tx)
						if err != nil {
							log.Printf("Failed to recover sender for tx %s: %v", tx.Hash().Hex(), err)
							continue
						}

						deposit := &Deposit{
							Chain:         hdwallet.ChainBNB,
							TxHash:        tx.Hash().Hex(),
							FromAddress:   from.Hex(),
							ToAddress:     to.Hex(),
							Asset:         "BNB",
							Amount:        value,
							BlockNumber:   blockNum,
							Timestamp:     time.Unix(int64(block.Time()), 0),
							Confirmations: confirmations,
							UserID:        userID,
						}

						if err := dw.processDepositAtomic(deposit); err != nil {
							log.Printf("Failed to process BNB deposit: %v", err)
						}
					}
				}
			}
		}
	}

	return nil
}

// ==================== SOLANA WATCHER ====================

// watchSolana watches for Solana deposits
func (dw *DepositWatcher) watchSolana() error {
	client := rpc.New(dw.config.SolanaRPCURL)

	// Get all Solana user wallets
	userWallets, err := dw.walletManager.GetUsersWalletByChain(dw.ctx, hdwallet.ChainSolana)
	if err != nil {
		return fmt.Errorf("failed to get Solana wallets: %w", err)
	}

	if len(userWallets) == 0 {
		return nil
	}

	// Create address map
	addressMap := make(map[string]int64)
	for _, wallet := range userWallets {
		addressMap[wallet.Address] = wallet.UserUID
	}

	// Get recent signatures for each wallet
	for _, wallet := range userWallets {
		pubKey, err := solana.PublicKeyFromBase58(wallet.Address)
		if err != nil {
			log.Printf("Invalid Solana address %s: %v", wallet.Address, err)
			continue
		}

		num := 100
		signatures, err := client.GetSignaturesForAddressWithOpts(
			dw.ctx,
			pubKey,
			&rpc.GetSignaturesForAddressOpts{
				Limit: &num,
			},
		)
		if err != nil {
			log.Printf("Failed to get signatures for %s: %v", wallet.Address, err)
			continue
		}

		for _, sig := range signatures {
			// Fix #4: Use cache for idempotency
			if dw.isDepositCached(hdwallet.ChainSolana, sig.Signature.String()) {
				continue
			}

			if sig.Err != nil {
				continue
			}

			tx, err := client.GetTransaction(
				dw.ctx,
				sig.Signature,
				&rpc.GetTransactionOpts{
					Commitment: rpc.CommitmentConfirmed,
					Encoding:   solana.EncodingJSONParsed,
				},
			)
			if err != nil {
				log.Printf("Failed to get transaction %s: %v", sig.Signature, err)
				continue
			}

			if tx == nil || tx.Meta == nil || tx.Transaction == nil {
				continue
			}

			// Fix #2: Iterate over all account keys to find balance changes
			depositAmount, fromAddress := dw.calculateSolanaBalanceChange(tx, wallet.Address, addressMap)

			if depositAmount > 0 {
				var timestamp time.Time
				if sig.BlockTime != nil {
					timestamp = sig.BlockTime.Time()
				} else {
					timestamp = time.Now()
				}

				deposit := &Deposit{
					Chain:       hdwallet.ChainSolana,
					TxHash:      sig.Signature.String(),
					FromAddress: fromAddress,
					ToAddress:   wallet.Address,
					Asset:       "SOL",
					Amount:      big.NewFloat(depositAmount),
					BlockNumber: uint64(sig.Slot),
					Timestamp:   timestamp,
					UserID:      addressMap[wallet.Address],
				}

				if err := dw.processDepositAtomic(deposit); err != nil {
					log.Printf("Failed to process Solana deposit: %v", err)
				}
			}
		}
	}

	return nil
}

// calculateSolanaBalanceChange calculates balance change for a Solana transaction
func (dw *DepositWatcher) calculateSolanaBalanceChange(tx *rpc.GetTransactionResult, targetAddress string, addressMap map[string]int64) (float64, string) {
	if tx == nil || tx.Meta == nil || tx.Transaction == nil {
		return 0, ""
	}

	// Parse the transaction from the binary data
	transaction, err := tx.Transaction.GetTransaction()
	if err != nil {
		return 0, ""
	}

	message := transaction.Message
	// message is a struct, not a pointer - remove nil check

	// Iterate over all account keys to find balance changes
	for i, accountKey := range message.AccountKeys {
		if i >= len(tx.Meta.PreBalances) || i >= len(tx.Meta.PostBalances) {
			continue
		}

		if accountKey.String() == targetAddress {
			preBalance := float64(tx.Meta.PreBalances[i])
			postBalance := float64(tx.Meta.PostBalances[i])
			balanceChange := postBalance - preBalance

			if balanceChange > 0 {
				// Find sender (first signer that's not the target)
				sender := ""
				for j, key := range message.AccountKeys {
					if j < len(transaction.Signatures) && key.String() != targetAddress {
						sender = key.String()
						break
					}
				}
				return balanceChange / 1e9, sender
			}
		}
	}

	return 0, ""
}

// ==================== BITCOIN WATCHER ====================

// NewBitcoinRPCClient creates a new Bitcoin RPC client
func NewBitcoinRPCClient(url, user, password string) *BitcoinRPCClient {
	return &BitcoinRPCClient{
		url:      url,
		user:     user,
		password: password,
		client:   &http.Client{Timeout: 30 * time.Second},
	}
}

// Call makes a JSON-RPC call to Bitcoin node
func (btc *BitcoinRPCClient) Call(method string, params []interface{}) (map[string]interface{}, error) {
	request := map[string]interface{}{
		"jsonrpc": "1.0",
		"id":      "bitnex",
		"method":  method,
		"params":  params,
	}

	requestBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", btc.url, bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.SetBasicAuth(btc.user, btc.password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := btc.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if err, ok := response["error"]; ok && err != nil {
		return nil, fmt.Errorf("RPC error: %v", err)
	}

	return response, nil
}

// GetBlockCount returns the current block height
func (btc *BitcoinRPCClient) GetBlockCount() (int64, error) {
	response, err := btc.Call("getblockcount", []interface{}{})
	if err != nil {
		return 0, err
	}

	if result, ok := response["result"].(float64); ok {
		return int64(result), nil
	}
	return 0, fmt.Errorf("invalid response format")
}

// GetBlockHash returns block hash by height
func (btc *BitcoinRPCClient) GetBlockHash(height int64) (string, error) {
	response, err := btc.Call("getblockhash", []interface{}{height})
	if err != nil {
		return "", err
	}

	if result, ok := response["result"].(string); ok {
		return result, nil
	}
	return "", fmt.Errorf("invalid response format")
}

// GetBlockVerbose returns block with full transaction details
func (btc *BitcoinRPCClient) GetBlockVerbose(blockHash string) (map[string]interface{}, error) {
	response, err := btc.Call("getblock", []interface{}{blockHash, 2})
	if err != nil {
		return nil, err
	}

	if result, ok := response["result"].(map[string]interface{}); ok {
		return result, nil
	}
	return nil, fmt.Errorf("invalid response format")
}

// watchBitcoin watches for Bitcoin deposits
func (dw *DepositWatcher) watchBitcoin() error {
	btcClient := NewBitcoinRPCClient(
		dw.config.BitcoinRPCURL,
		dw.config.BitcoinRPCUser,
		dw.config.BitcoinRPCPassword,
	)

	userWallets, err := dw.walletManager.GetUsersWalletByChain(dw.ctx, hdwallet.ChainBitcoin)
	if err != nil {
		return fmt.Errorf("failed to get Bitcoin wallets: %w", err)
	}

	if len(userWallets) == 0 {
		return nil
	}

	lastBlock, err := dw.getLastProcessedBlock(hdwallet.ChainBitcoin)
	if err != nil {
		lastBlock = 0
	}

	currentBlock, err := btcClient.GetBlockCount()
	if err != nil {
		return fmt.Errorf("failed to get current block height: %w", err)
	}

	if currentBlock <= int64(lastBlock) {
		return nil
	}

	addressMap := make(map[string]int64)
	for _, wallet := range userWallets {
		addressMap[wallet.Address] = wallet.UserUID
	}

	// Fix #5: Batch process blocks with full transaction details
	batchSize := int64(10)
	for fromBlock := int64(lastBlock) + 1; fromBlock <= currentBlock; fromBlock += batchSize {
		toBlock := fromBlock + batchSize - 1
		if toBlock > currentBlock {
			toBlock = currentBlock
		}

		if err := dw.processBitcoinBlockRangeBatch(btcClient, fromBlock, toBlock, addressMap); err != nil {
			log.Printf("Error processing Bitcoin blocks %d-%d: %v", fromBlock, toBlock, err)
			continue
		}

		if err := dw.updateLastProcessedBlock(hdwallet.ChainBitcoin, uint64(toBlock)); err != nil {
			log.Printf("Failed to update last processed block: %v", err)
		}
	}

	return nil
}

// processBitcoinBlockRangeBatch processes a range of Bitcoin blocks with batch RPC calls
func (dw *DepositWatcher) processBitcoinBlockRangeBatch(
	btcClient *BitcoinRPCClient,
	fromBlock, toBlock int64,
	addressMap map[string]int64,
) error {
	for blockHeight := fromBlock; blockHeight <= toBlock; blockHeight++ {
		blockHash, err := btcClient.GetBlockHash(blockHeight)
		if err != nil {
			log.Printf("Failed to get block hash for height %d: %v", blockHeight, err)
			continue
		}

		// Get block with full transaction details in a single RPC call
		blockData, err := btcClient.GetBlockVerbose(blockHash)
		if err != nil {
			log.Printf("Failed to get block %s: %v", blockHash, err)
			continue
		}

		txs, ok := blockData["tx"].([]interface{})
		if !ok {
			continue
		}

		blockTime, _ := blockData["time"].(float64)

		for _, txData := range txs {
			txMap, ok := txData.(map[string]interface{})
			if !ok {
				continue
			}

			txHash, _ := txMap["txid"].(string)

			if dw.isDepositCached(hdwallet.ChainBitcoin, txHash) {
				continue
			}

			// Parse vout to find payments to our addresses
			vout, ok := txMap["vout"].([]interface{})
			if !ok {
				continue
			}

			for _, output := range vout {
				outputMap, ok := output.(map[string]interface{})
				if !ok {
					continue
				}

				scriptPubKey, ok := outputMap["scriptPubKey"].(map[string]interface{})
				if !ok {
					continue
				}

				addresses, ok := scriptPubKey["addresses"].([]interface{})
				if !ok || len(addresses) == 0 {
					continue
				}

				address, ok := addresses[0].(string)
				if !ok {
					continue
				}

				if userID, exists := addressMap[address]; exists {
					amount, ok := outputMap["value"].(float64)
					if !ok || amount <= 0 {
						continue
					}

					confirmations, _ := blockData["confirmations"].(float64)

					if int64(confirmations) >= int64(dw.config.MaxConfirmations) {
						// Get sender from vin
						fromAddress := dw.getBitcoinSender(txMap)

						deposit := &Deposit{
							Chain:         hdwallet.ChainBitcoin,
							TxHash:        txHash,
							FromAddress:   fromAddress,
							ToAddress:     address,
							Asset:         "BTC",
							Amount:        big.NewFloat(amount),
							BlockNumber:   uint64(blockHeight),
							Timestamp:     time.Unix(int64(blockTime), 0),
							Confirmations: uint64(confirmations),
							UserID:        userID,
						}

						if err := dw.processDepositAtomic(deposit); err != nil {
							log.Printf("Failed to process Bitcoin deposit: %v", err)
						}
					}
				}
			}
		}
	}

	return nil
}

// getBitcoinSender extracts sender address from Bitcoin transaction
func (dw *DepositWatcher) getBitcoinSender(txMap map[string]interface{}) string {
	vin, ok := txMap["vin"].([]interface{})
	if !ok || len(vin) == 0 {
		return ""
	}

	firstVin, ok := vin[0].(map[string]interface{})
	if !ok {
		return ""
	}

	if txid, ok := firstVin["txid"].(string); ok {
		return txid[:20] + "..."
	}

	if _, ok := firstVin["coinbase"].(string); ok {
		return "coinbase"
	}

	return ""
}

// ==================== SUI WATCHER ====================

// NewSuiRPCClient creates a new Sui RPC client
func NewSuiRPCClient(url string) *SuiRPCClient {
	return &SuiRPCClient{
		url:    url,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

// Call makes a JSON-RPC call to Sui node
func (sui *SuiRPCClient) Call(method string, params []interface{}) (map[string]interface{}, error) {
	request := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  params,
	}

	requestBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := sui.client.Post(sui.url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if errObj, ok := response["error"]; ok && errObj != nil {
		return nil, fmt.Errorf("RPC error: %v", errObj)
	}

	return response, nil
}

// GetLatestCheckpoint returns the latest checkpoint
func (sui *SuiRPCClient) GetLatestCheckpoint() (int64, error) {
	response, err := sui.Call("sui_getLatestCheckpointSequenceNumber", []interface{}{})
	if err != nil {
		return 0, err
	}

	if result, ok := response["result"].(float64); ok {
		return int64(result), nil
	}
	return 0, fmt.Errorf("invalid response format")
}

// GetCheckpoint returns checkpoint information
func (sui *SuiRPCClient) GetCheckpoint(checkpoint int64) (map[string]interface{}, error) {
	response, err := sui.Call("sui_getCheckpoint", []interface{}{checkpoint})
	if err != nil {
		return nil, err
	}

	if result, ok := response["result"].(map[string]interface{}); ok {
		return result, nil
	}
	return nil, fmt.Errorf("invalid response format")
}

// watchSui watches for Sui deposits
func (dw *DepositWatcher) watchSui() error {
	suiClient := NewSuiRPCClient(dw.config.SuiRPCURL)

	userWallets, err := dw.walletManager.GetUsersWalletByChain(dw.ctx, hdwallet.ChainSui)
	if err != nil {
		return fmt.Errorf("failed to get Sui wallets: %w", err)
	}

	if len(userWallets) == 0 {
		return nil
	}

	addressMap := make(map[string]int64)
	for _, wallet := range userWallets {
		addressMap[wallet.Address] = wallet.UserUID
	}

	lastCheckpoint, err := dw.getLastProcessedCheckpoint(hdwallet.ChainSui)
	if err != nil {
		lastCheckpoint = 0
	}

	latestCheckpoint, err := suiClient.GetLatestCheckpoint()
	if err != nil {
		return fmt.Errorf("failed to get latest checkpoint: %w", err)
	}

	if latestCheckpoint <= lastCheckpoint {
		return nil
	}

	for checkpoint := lastCheckpoint + 1; checkpoint <= latestCheckpoint; checkpoint++ {
		if err := dw.processSuiCheckpoint(suiClient, checkpoint, addressMap); err != nil {
			log.Printf("Error processing Sui checkpoint %d: %v", checkpoint, err)
			continue
		}

		if err := dw.updateLastProcessedCheckpoint(hdwallet.ChainSui, checkpoint); err != nil {
			log.Printf("Failed to update last processed checkpoint: %v", err)
		}
	}

	return nil
}

// processSuiCheckpoint processes a single Sui checkpoint
func (dw *DepositWatcher) processSuiCheckpoint(
	suiClient *SuiRPCClient,
	checkpoint int64,
	addressMap map[string]int64,
) error {
	checkpointInfo, err := suiClient.GetCheckpoint(checkpoint)
	if err != nil {
		return fmt.Errorf("failed to get checkpoint: %w", err)
	}

	transactions, ok := checkpointInfo["transactions"].([]interface{})
	if !ok {
		return nil
	}

	for _, txDigest := range transactions {
		txHash, ok := txDigest.(string)
		if !ok {
			continue
		}

		if dw.isDepositCached(hdwallet.ChainSui, txHash) {
			continue
		}

		// Fetch transaction details
		txResponse, err := suiClient.Call("sui_getTransaction", []interface{}{txHash})
		if err != nil {
			log.Printf("Failed to get Sui transaction %s: %v", txHash, err)
			continue
		}

		if effects, ok := txResponse["result"].(map[string]interface{})["effects"].(map[string]interface{}); ok {
			if balanceChanges, ok := effects["balanceChanges"].([]interface{}); ok {
				for _, change := range balanceChanges {
					if changeMap, ok := change.(map[string]interface{}); ok {
						if owner, ok := changeMap["owner"].(map[string]interface{}); ok {
							if addressOwner, ok := owner["AddressOwner"].(string); ok {
								if userID, exists := addressMap[addressOwner]; exists {
									if amountStr, ok := changeMap["amount"].(string); ok {
										var amount float64
										fmt.Sscanf(amountStr, "%f", &amount)
										amount = amount / 1e9 // SUI has 9 decimals

										if amount > 0 {
											deposit := &Deposit{
												Chain:       hdwallet.ChainSui,
												TxHash:      txHash,
												ToAddress:   addressOwner,
												Asset:       "SUI",
												Amount:      big.NewFloat(amount),
												BlockNumber: uint64(checkpoint),
												Timestamp:   time.Now(),
												UserID:      userID,
											}

											if err := dw.processDepositAtomic(deposit); err != nil {
												log.Printf("Failed to process Sui deposit: %v", err)
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	return nil
}

// ==================== XRP WATCHER ====================

// NewXRPRPCClient creates a new XRP RPC client
func NewXRPRPCClient(url string) *XRPRPCClient {
	return &XRPRPCClient{
		url:    url,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

// Call makes a JSON-RPC call to XRP node
func (xrp *XRPRPCClient) Call(method string, params map[string]interface{}) (map[string]interface{}, error) {
	request := map[string]interface{}{
		"method": method,
		"params": []interface{}{params},
	}

	requestBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := xrp.client.Post(xrp.url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if result, ok := response["result"]; ok {
		if resultMap, ok := result.(map[string]interface{}); ok {
			if status, ok := resultMap["status"].(string); ok && status == "error" {
				return nil, fmt.Errorf("XRP RPC error: %v", resultMap["error"])
			}
			return resultMap, nil
		}
	}

	return nil, fmt.Errorf("invalid response format")
}

// GetAccountTransactions gets transactions for an account
func (xrp *XRPRPCClient) GetAccountTransactions(address string, minLedger, maxLedger int64) ([]XRPTransaction, error) {
	params := map[string]interface{}{
		"account":          address,
		"ledger_index_min": minLedger,
		"ledger_index_max": maxLedger,
		"limit":            200,
	}

	response, err := xrp.Call("account_tx", params)
	if err != nil {
		return nil, err
	}

	transactions := []XRPTransaction{}
	if txs, ok := response["transactions"].([]interface{}); ok {
		for _, txItem := range txs {
			if txMap, ok := txItem.(map[string]interface{}); ok {
				if tx, ok := txMap["tx"].(map[string]interface{}); ok {
					xrpTx := XRPTransaction{}
					if hash, ok := tx["hash"].(string); ok {
						xrpTx.Hash = hash
					}
					if ledgerIndex, ok := tx["ledger_index"].(float64); ok {
						xrpTx.LedgerIndex = int64(ledgerIndex)
					}
					if date, ok := tx["date"].(float64); ok {
						xrpTx.Date = int64(date)
					}
					if txType, ok := tx["TransactionType"].(string); ok {
						xrpTx.TxType = txType
					}
					if account, ok := tx["Account"].(string); ok {
						xrpTx.Account = account
					}
					if destination, ok := tx["Destination"].(string); ok {
						xrpTx.Destination = destination
					}
					if amount, ok := tx["Amount"].(string); ok {
						xrpTx.Amount = amount
					}
					transactions = append(transactions, xrpTx)
				}
			}
		}
	}

	return transactions, nil
}

// parseXRPAmount parses XRP amount (in drops) to XRP
func parseXRPAmount(amountStr string) float64 {
	var amount float64
	fmt.Sscanf(amountStr, "%f", &amount)
	return amount / 1000000.0
}

// watchXRP watches for XRP deposits
func (dw *DepositWatcher) watchXRP() error {
	xrpClient := NewXRPRPCClient(dw.config.XRPRPCURL)

	userWallets, err := dw.walletManager.GetUsersWalletByChain(dw.ctx, hdwallet.ChainXRP)
	if err != nil {
		return fmt.Errorf("failed to get XRP wallets: %w", err)
	}

	if len(userWallets) == 0 {
		return nil
	}

	lastLedger, err := dw.getLastProcessedLedger(hdwallet.ChainXRP)
	if err != nil {
		lastLedger = 0
	}

	for _, wallet := range userWallets {
		maxLedger := lastLedger + 1000

		transactions, err := xrpClient.GetAccountTransactions(wallet.Address, lastLedger+1, maxLedger)
		if err != nil {
			log.Printf("Error getting XRP transactions for %s: %v", wallet.Address, err)
			continue
		}

		for _, tx := range transactions {
			if dw.isDepositCached(hdwallet.ChainXRP, tx.Hash) {
				continue
			}

			if tx.TxType == "Payment" && tx.Destination == wallet.Address {
				amountXRP := parseXRPAmount(tx.Amount)

				if amountXRP > 0 {
					deposit := &Deposit{
						Chain:       hdwallet.ChainXRP,
						TxHash:      tx.Hash,
						FromAddress: tx.Account,
						ToAddress:   tx.Destination,
						Asset:       "XRP",
						Amount:      big.NewFloat(amountXRP),
						BlockNumber: uint64(tx.LedgerIndex),
						Timestamp:   time.Unix(tx.Date, 0),
						UserID:      wallet.UserUID,
					}

					if err := dw.processDepositAtomic(deposit); err != nil {
						log.Printf("Failed to process XRP deposit: %v", err)
					}
				}
			}
		}

		if err := dw.updateLastProcessedLedger(hdwallet.ChainXRP, maxLedger); err != nil {
			log.Printf("Failed to update last processed ledger: %v", err)
		}
	}

	return nil
}

// ==================== ATOMIC DEPOSIT PROCESSING ====================

// processDepositAtomic processes a deposit atomically with database transaction
func (dw *DepositWatcher) processDepositAtomic(deposit *Deposit) error {
	dw.mu.Lock()
	defer dw.mu.Unlock()

	// Fix #4: Use cache for idempotency
	if dw.isDepositCached(deposit.Chain, deposit.TxHash) {
		return nil
	}

	deposit.Reference = fmt.Sprintf("%s_%s_%d", deposit.Chain, deposit.TxHash, time.Now().Unix())
	amountFloat64, _ := deposit.Amount.Float64()

	// Fix #3: Atomic transaction with ON CONFLICT for idempotency
	tx, err := dw.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Use INSERT ... ON CONFLICT DO NOTHING for idempotency
	insertQuery := `
		INSERT INTO deposits (chain, tx_hash, user_id, from_address, to_address, asset, amount, 
		                     block_number, confirmations, status, reference)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (chain, tx_hash) DO NOTHING
		RETURNING id
	`

	var depositID int64
	err = tx.QueryRow(insertQuery,
		deposit.Chain, deposit.TxHash, deposit.UserID, deposit.FromAddress,
		deposit.ToAddress, deposit.Asset, amountFloat64, deposit.BlockNumber,
		deposit.Confirmations, "confirmed", deposit.Reference,
	).Scan(&depositID)

	if err == sql.ErrNoRows {
		// Already processed
		dw.cacheDeposit(deposit.Chain, deposit.TxHash)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to insert deposit: %w", err)
	}

	// Credit user's account in ledger
	err = dw.ledger.CreditAccount(
		context.Background(),
		deposit.UserID,
		deposit.Asset,
		amountFloat64,
		deposit.Reference,
		"deposit",
	)
	if err != nil {
		// Log the error but don't rollback - we'll retry later
		log.Printf("⚠️ Ledger credit failed for deposit %s: %v", deposit.TxHash, err)
		// Update deposit status to pending_retry
		updateQuery := `UPDATE deposits SET status = 'pending_retry', updated_at = NOW() WHERE id = $1`
		tx.Exec(updateQuery, depositID)
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction with error state: %w", err)
		}
		return fmt.Errorf("ledger credit failed: %w", err)
	}

	// Insert deposit event
	eventQuery := `
		INSERT INTO deposit_events (chain, tx_hash, user_id, asset, amount, status)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err = tx.Exec(eventQuery,
		deposit.Chain, deposit.TxHash, deposit.UserID, deposit.Asset,
		amountFloat64, "completed",
	)
	if err != nil {
		log.Printf("Failed to insert deposit event: %v", err)
		// Non-critical, continue
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Cache the processed deposit
	dw.cacheDeposit(deposit.Chain, deposit.TxHash)

	log.Printf("✅ Processed deposit: %s %f %s for user %s (tx: %s)",
		deposit.Asset, amountFloat64, deposit.Chain, deposit.UserID, deposit.TxHash)

	return nil
}

// ==================== CACHE HELPERS ====================

// isDepositCached checks if a deposit is in the cache
func (dw *DepositWatcher) isDepositCached(chain, txHash string) bool {
	key := fmt.Sprintf("%s:%s", chain, txHash)
	_, exists := dw.processedCache.Load(key)
	return exists
}

// cacheDeposit adds a deposit to the cache
func (dw *DepositWatcher) cacheDeposit(chain, txHash string) {
	key := fmt.Sprintf("%s:%s", chain, txHash)
	dw.processedCache.Store(key, time.Now())

	// Clean up old cache entries periodically
	go dw.cleanupCache()
}

// cleanupCache removes old entries from cache
func (dw *DepositWatcher) cleanupCache() {
	dw.processedCache.Range(func(key, value interface{}) bool {
		if timestamp, ok := value.(time.Time); ok {
			if time.Since(timestamp) > dw.config.CacheTTL {
				dw.processedCache.Delete(key)
			}
		}
		return true
	})
}

// ==================== HELPER FUNCTIONS ====================

// getCheckInterval returns the check interval for a chain
func (dw *DepositWatcher) getCheckInterval(chain string) time.Duration {
	switch chain {
	case "Ethereum", "BNB":
		return dw.config.EthereumCheckInterval
	case "Solana":
		return dw.config.SolanaCheckInterval
	case "Bitcoin":
		return dw.config.BitcoinCheckInterval
	case "Sui":
		return dw.config.SuiCheckInterval
	case "XRP":
		return dw.config.XRPCheckInterval
	default:
		return 10 * time.Second
	}
}

// getLastProcessedBlock gets the last processed block for a chain
func (dw *DepositWatcher) getLastProcessedBlock(chain string) (uint64, error) {
	var lastBlock uint64
	query := `SELECT last_block_processed FROM processed_blocks WHERE chain = $1`
	err := dw.db.QueryRow(query, chain).Scan(&lastBlock)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return lastBlock, err
}

// updateLastProcessedBlock updates the last processed block for a chain
func (dw *DepositWatcher) updateLastProcessedBlock(chain string, blockNumber uint64) error {
	query := `
		INSERT INTO processed_blocks (chain, last_block_processed, updated_at)
		VALUES ($1, $2, CURRENT_TIMESTAMP)
		ON CONFLICT (chain) DO UPDATE SET
			last_block_processed = EXCLUDED.last_block_processed,
			updated_at = CURRENT_TIMESTAMP
	`
	_, err := dw.db.Exec(query, chain, blockNumber)
	return err
}

// getLastProcessedCheckpoint gets the last processed checkpoint for Sui
func (dw *DepositWatcher) getLastProcessedCheckpoint(chain string) (int64, error) {
	var lastCheckpoint int64
	query := `SELECT last_checkpoint FROM processed_checkpoints WHERE chain = $1`
	err := dw.db.QueryRow(query, chain).Scan(&lastCheckpoint)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return lastCheckpoint, err
}

// updateLastProcessedCheckpoint updates the last processed checkpoint for Sui
func (dw *DepositWatcher) updateLastProcessedCheckpoint(chain string, checkpoint int64) error {
	query := `
		INSERT INTO processed_checkpoints (chain, last_checkpoint, updated_at)
		VALUES ($1, $2, CURRENT_TIMESTAMP)
		ON CONFLICT (chain) DO UPDATE SET
			last_checkpoint = EXCLUDED.last_checkpoint,
			updated_at = CURRENT_TIMESTAMP
	`
	_, err := dw.db.Exec(query, chain, checkpoint)
	return err
}

// getLastProcessedLedger gets the last processed ledger for XRP
func (dw *DepositWatcher) getLastProcessedLedger(chain string) (int64, error) {
	var lastLedger int64
	query := `SELECT last_ledger FROM processed_ledgers WHERE chain = $1`
	err := dw.db.QueryRow(query, chain).Scan(&lastLedger)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return lastLedger, err
}

// updateLastProcessedLedger updates the last processed ledger for XRP
func (dw *DepositWatcher) updateLastProcessedLedger(chain string, ledger int64) error {
	query := `
		INSERT INTO processed_ledgers (chain, last_ledger, updated_at)
		VALUES ($1, $2, CURRENT_TIMESTAMP)
		ON CONFLICT (chain) DO UPDATE SET
			last_ledger = EXCLUDED.last_ledger,
			updated_at = CURRENT_TIMESTAMP
	`
	_, err := dw.db.Exec(query, chain, ledger)
	return err
}
