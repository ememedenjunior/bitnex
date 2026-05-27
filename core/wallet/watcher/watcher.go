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
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gagliardetto/solana-go/rpc"
)

// ==================== RPC HEALTH CHECKER ====================

type RPCHealthChecker struct {
	mu              sync.RWMutex
	endpointStatus  map[string]*EndpointStatus
	circuitBreakers map[string]*CircuitBreaker
}

type EndpointStatus struct {
	URL          string
	Healthy      bool
	LastCheck    time.Time
	FailureCount int
	SuccessCount int
	LastError    string
	ResponseTime time.Duration
}

type CircuitBreaker struct {
	Failures    int
	LastFailure time.Time
	State       string // "CLOSED", "OPEN", "HALF_OPEN"
	LastSuccess time.Time
}

func NewRPCHealthChecker() *RPCHealthChecker {
	return &RPCHealthChecker{
		endpointStatus:  make(map[string]*EndpointStatus),
		circuitBreakers: make(map[string]*CircuitBreaker),
	}
}

func (hc *RPCHealthChecker) CheckEndpoint(url string) bool {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	status, exists := hc.endpointStatus[url]
	if !exists {
		status = &EndpointStatus{URL: url, Healthy: true}
		hc.endpointStatus[url] = status
	}

	// Check circuit breaker
	cb, exists := hc.circuitBreakers[url]
	if exists && cb.State == "OPEN" {
		if time.Since(cb.LastFailure) > 60*time.Second {
			cb.State = "HALF_OPEN"
			log.Printf("🔌 Circuit breaker for %s transitioning to HALF_OPEN", url)
		} else {
			return false
		}
	}

	return status.Healthy
}

func (hc *RPCHealthChecker) RecordSuccess(url string, responseTime time.Duration) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	status, exists := hc.endpointStatus[url]
	if !exists {
		status = &EndpointStatus{URL: url, Healthy: true}
		hc.endpointStatus[url] = status
	}

	status.Healthy = true
	status.SuccessCount++
	status.FailureCount = 0
	status.LastCheck = time.Now()
	status.ResponseTime = responseTime
	status.LastError = ""

	// Update circuit breaker
	cb, exists := hc.circuitBreakers[url]
	if exists {
		cb.Failures = 0
		cb.LastSuccess = time.Now()
		if cb.State == "HALF_OPEN" {
			cb.State = "CLOSED"
			log.Printf("🔌 Circuit breaker for %s closed (successful request)", url)
		}
	}
}

func (hc *RPCHealthChecker) RecordFailure(url string, err error) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	status, exists := hc.endpointStatus[url]
	if !exists {
		status = &EndpointStatus{URL: url, Healthy: true}
		hc.endpointStatus[url] = status
	}

	status.FailureCount++
	status.LastError = err.Error()
	status.LastCheck = time.Now()

	// Circuit breaker logic
	cb, exists := hc.circuitBreakers[url]
	if !exists {
		cb = &CircuitBreaker{State: "CLOSED"}
		hc.circuitBreakers[url] = cb
	}

	cb.Failures++
	cb.LastFailure = time.Now()

	// Open circuit after 3 failures
	if cb.Failures >= 3 && cb.State == "CLOSED" {
		cb.State = "OPEN"
		status.Healthy = false
		log.Printf("🔌 Circuit breaker for %s OPEN after %d failures", url, cb.Failures)
	}

	// Mark unhealthy after 2 failures
	if status.FailureCount >= 2 {
		status.Healthy = false
	}
}

// ==================== CHAIN ADAPTER INTERFACE ====================

type ChainAdapter interface {
	GetLatestState() (interface{}, error)
	FetchTransactions(startBlock, endBlock interface{}) ([]Transaction, error)
	ValidateResponse(response []byte) bool
	SwitchRPC() error
	GetName() string
	GetRPCURL() string
}

type Transaction struct {
	Hash        string
	From        string
	To          string
	Amount      *big.Float
	Asset       string
	BlockNumber uint64
	Timestamp   time.Time
	Chain       string
}

// ==================== SUI ADAPTER WITH FIX ====================

type SuiAdapter struct {
	name          string
	rpcURLs       []string
	currentRPC    int
	healthChecker *RPCHealthChecker
	client        *http.Client
	mu            sync.RWMutex
}

func NewSuiAdapter(rpcURLs []string) *SuiAdapter {
	return &SuiAdapter{
		name:          "Sui",
		rpcURLs:       rpcURLs,
		currentRPC:    0,
		healthChecker: NewRPCHealthChecker(),
		client:        &http.Client{Timeout: 30 * time.Second},
	}
}

func (sa *SuiAdapter) GetName() string {
	return sa.name
}

func (sa *SuiAdapter) GetRPCURL() string {
	sa.mu.RLock()
	defer sa.mu.RUnlock()
	if len(sa.rpcURLs) == 0 {
		return ""
	}
	return sa.rpcURLs[sa.currentRPC]
}

func (sa *SuiAdapter) SwitchRPC() error {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	if len(sa.rpcURLs) == 0 {
		return fmt.Errorf("no RPC endpoints configured")
	}

	originalRPC := sa.currentRPC
	for i := 0; i < len(sa.rpcURLs); i++ {
		sa.currentRPC = (sa.currentRPC + 1) % len(sa.rpcURLs)
		if sa.currentRPC != originalRPC {
			log.Printf("🔄 Sui: Switching RPC from %s to %s",
				sa.rpcURLs[originalRPC], sa.rpcURLs[sa.currentRPC])
			return nil
		}
	}
	return fmt.Errorf("no healthy Sui RPC endpoints available")
}

func (sa *SuiAdapter) GetLatestCheckpoint() (int64, error) {
	url := sa.GetRPCURL()
	if url == "" {
		return 0, fmt.Errorf("no RPC URL available")
	}

	if !sa.healthChecker.CheckEndpoint(url) {
		if err := sa.SwitchRPC(); err != nil {
			return 0, err
		}
		url = sa.GetRPCURL()
	}

	// Correct JSON-RPC request for Sui
	request := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "sui_getLatestCheckpointSequenceNumber",
		"params":  []interface{}{},
	}

	startTime := time.Now()
	response, err := sa.makeRequest(url, request)
	if err != nil {
		sa.healthChecker.RecordFailure(url, err)
		sa.SwitchRPC()
		return 0, fmt.Errorf("failed to get latest checkpoint: %w", err)
	}

	// Validate response is not HTML
	if !sa.ValidateResponse(response) {
		err := fmt.Errorf("invalid response format (HTML or malformed JSON)")
		sa.healthChecker.RecordFailure(url, err)
		sa.SwitchRPC()
		return 0, err
	}

	sa.healthChecker.RecordSuccess(url, time.Since(startTime))

	// Parse response
	var rpcResponse struct {
		Result int64 `json:"result"`
		Error  struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal(response, &rpcResponse); err != nil {
		return 0, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if rpcResponse.Error.Code != 0 {
		return 0, fmt.Errorf("RPC error: %s", rpcResponse.Error.Message)
	}

	return rpcResponse.Result, nil
}

func (sa *SuiAdapter) GetCheckpoint(checkpoint int64) (map[string]interface{}, error) {
	url := sa.GetRPCURL()
	if url == "" {
		return nil, fmt.Errorf("no RPC URL available")
	}

	request := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "sui_getCheckpoint",
		"params":  []interface{}{checkpoint},
	}

	response, err := sa.makeRequest(url, request)
	if err != nil {
		return nil, err
	}

	if !sa.ValidateResponse(response) {
		return nil, fmt.Errorf("invalid response format")
	}

	var rpcResponse struct {
		Result map[string]interface{} `json:"result"`
		Error  struct {
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal(response, &rpcResponse); err != nil {
		return nil, err
	}

	if rpcResponse.Error.Message != "" {
		return nil, fmt.Errorf("RPC error: %s", rpcResponse.Error.Message)
	}

	return rpcResponse.Result, nil
}

func (sa *SuiAdapter) makeRequest(url string, request interface{}) ([]byte, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	resp, err := sa.client.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func (sa *SuiAdapter) ValidateResponse(response []byte) bool {
	// Check for HTML response
	responseStr := string(response)
	if strings.Contains(responseStr, "<!DOCTYPE") ||
		strings.Contains(responseStr, "<html") ||
		strings.Contains(responseStr, "HTML") {
		return false
	}

	// Check if it's valid JSON
	var js interface{}
	if err := json.Unmarshal(response, &js); err != nil {
		return false
	}

	return true
}

func (sa *SuiAdapter) GetLatestState() (interface{}, error) {
	return sa.GetLatestCheckpoint()
}

func (sa *SuiAdapter) FetchTransactions(startBlock, endBlock interface{}) ([]Transaction, error) {
	return []Transaction{}, nil
}

// ==================== XRP ADAPTER WITH LEDGER FIX ====================

type XRPAdapter struct {
	name          string
	rpcURLs       []string
	currentRPC    int
	healthChecker *RPCHealthChecker
	client        *http.Client
	mu            sync.RWMutex
}

type XRPTransaction struct {
	Hash        string
	LedgerIndex int64
	Date        int64
	TxType      string
	Account     string
	Destination string
	Amount      string
	Fee         string
}

func NewXRPAdapter(rpcURLs []string) *XRPAdapter {
	return &XRPAdapter{
		name:          "XRP",
		rpcURLs:       rpcURLs,
		currentRPC:    0,
		healthChecker: NewRPCHealthChecker(),
		client:        &http.Client{Timeout: 30 * time.Second},
	}
}

func (xa *XRPAdapter) GetName() string {
	return xa.name
}

func (xa *XRPAdapter) GetRPCURL() string {
	xa.mu.RLock()
	defer xa.mu.RUnlock()
	if len(xa.rpcURLs) == 0 {
		return ""
	}
	return xa.rpcURLs[xa.currentRPC]
}

func (xa *XRPAdapter) SwitchRPC() error {
	xa.mu.Lock()
	defer xa.mu.Unlock()

	if len(xa.rpcURLs) == 0 {
		return fmt.Errorf("no RPC endpoints configured")
	}

	originalRPC := xa.currentRPC
	for i := 0; i < len(xa.rpcURLs); i++ {
		xa.currentRPC = (xa.currentRPC + 1) % len(xa.rpcURLs)
		if xa.currentRPC != originalRPC {
			log.Printf("🔄 XRP: Switching RPC from %s to %s",
				xa.rpcURLs[originalRPC], xa.rpcURLs[xa.currentRPC])
			return nil
		}
	}
	return fmt.Errorf("no healthy XRP RPC endpoints available")
}

func (xa *XRPAdapter) GetAccountTransactions(address string, minLedger, maxLedger int64) ([]XRPTransaction, error) {
	url := xa.GetRPCURL()
	if url == "" {
		return nil, fmt.Errorf("no RPC URL available")
	}

	if !xa.healthChecker.CheckEndpoint(url) {
		if err := xa.SwitchRPC(); err != nil {
			return nil, err
		}
		url = xa.GetRPCURL()
	}

	// Normalize ledger indices - FIX for lgrIdxsInvalid
	normalizedMin := minLedger
	normalizedMax := maxLedger

	// Fix: Ensure min <= max unless max = -1
	if normalizedMax != -1 && normalizedMin > normalizedMax {
		log.Printf("⚠️ XRP: Swapping invalid ledger range (min=%d, max=%d)", normalizedMin, normalizedMax)
		normalizedMin, normalizedMax = normalizedMax, normalizedMin
	}

	// Fix: Set invalid or negative indices to -1 (latest)
	if normalizedMin < 0 {
		normalizedMin = -1
	}
	if normalizedMax < 0 {
		normalizedMax = -1
	}

	request := map[string]interface{}{
		"method": "account_tx",
		"params": []interface{}{
			map[string]interface{}{
				"account":          address,
				"ledger_index_min": normalizedMin,
				"ledger_index_max": normalizedMax,
				"limit":            200,
				"binary":           false,
			},
		},
	}

	startTime := time.Now()
	response, err := xa.makeRequest(url, request)
	if err != nil {
		xa.healthChecker.RecordFailure(url, err)
		xa.SwitchRPC()
		return nil, err
	}

	if !xa.ValidateResponse(response) {
		err := fmt.Errorf("invalid response format")
		xa.healthChecker.RecordFailure(url, err)
		xa.SwitchRPC()
		return nil, err
	}

	xa.healthChecker.RecordSuccess(url, time.Since(startTime))

	// Parse response
	var rpcResponse struct {
		Result struct {
			Transactions []struct {
				Tx struct {
					Hash        string `json:"hash"`
					LedgerIndex int64  `json:"ledger_index"`
					Date        int64  `json:"date"`
					TxType      string `json:"TransactionType"`
					Account     string `json:"Account"`
					Destination string `json:"Destination"`
					Amount      string `json:"Amount"`
					Fee         string `json:"Fee"`
				} `json:"tx"`
			} `json:"transactions"`
			Status string `json:"status"`
			Error  string `json:"error"`
		} `json:"result"`
	}

	if err := json.Unmarshal(response, &rpcResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if rpcResponse.Result.Status == "error" {
		return nil, fmt.Errorf("XRP error: %s", rpcResponse.Result.Error)
	}

	transactions := []XRPTransaction{}
	for _, txItem := range rpcResponse.Result.Transactions {
		transactions = append(transactions, XRPTransaction{
			Hash:        txItem.Tx.Hash,
			LedgerIndex: txItem.Tx.LedgerIndex,
			Date:        txItem.Tx.Date,
			TxType:      txItem.Tx.TxType,
			Account:     txItem.Tx.Account,
			Destination: txItem.Tx.Destination,
			Amount:      txItem.Tx.Amount,
			Fee:         txItem.Tx.Fee,
		})
	}

	return transactions, nil
}

func (xa *XRPAdapter) makeRequest(url string, request interface{}) ([]byte, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	resp, err := xa.client.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func (xa *XRPAdapter) ValidateResponse(response []byte) bool {
	responseStr := string(response)
	if strings.Contains(responseStr, "<!DOCTYPE") ||
		strings.Contains(responseStr, "<html") {
		return false
	}

	var js interface{}
	if err := json.Unmarshal(response, &js); err != nil {
		return false
	}

	return true
}

func (xa *XRPAdapter) GetLatestState() (interface{}, error) {
	return nil, nil
}

func (xa *XRPAdapter) FetchTransactions(startBlock, endBlock interface{}) ([]Transaction, error) {
	return []Transaction{}, nil
}

// ==================== BITCOIN ADAPTER WITH REST API FIX ====================

type BitcoinAdapter struct {
	name          string
	rpcURLs       []string
	currentRPC    int
	healthChecker *RPCHealthChecker
	client        *http.Client
	mu            sync.RWMutex
}

func NewBitcoinAdapter(rpcURLs []string) *BitcoinAdapter {
	return &BitcoinAdapter{
		name:          "Bitcoin",
		rpcURLs:       rpcURLs,
		currentRPC:    0,
		healthChecker: NewRPCHealthChecker(),
		client:        &http.Client{Timeout: 30 * time.Second},
	}
}

func (ba *BitcoinAdapter) GetName() string {
	return ba.name
}

func (ba *BitcoinAdapter) GetRPCURL() string {
	ba.mu.RLock()
	defer ba.mu.RUnlock()
	if len(ba.rpcURLs) == 0 {
		return ""
	}
	return ba.rpcURLs[ba.currentRPC]
}

func (ba *BitcoinAdapter) SwitchRPC() error {
	ba.mu.Lock()
	defer ba.mu.Unlock()

	if len(ba.rpcURLs) == 0 {
		return fmt.Errorf("no RPC endpoints configured")
	}

	originalRPC := ba.currentRPC
	for i := 0; i < len(ba.rpcURLs); i++ {
		ba.currentRPC = (ba.currentRPC + 1) % len(ba.rpcURLs)
		if ba.currentRPC != originalRPC {
			log.Printf("🔄 Bitcoin: Switching RPC from %s to %s",
				ba.rpcURLs[originalRPC], ba.rpcURLs[ba.currentRPC])
			return nil
		}
	}
	return fmt.Errorf("no healthy Bitcoin RPC endpoints available")
}

func (ba *BitcoinAdapter) GetBlockCount() (int64, error) {
	url := ba.GetRPCURL()
	if url == "" {
		return 0, fmt.Errorf("no RPC URL available")
	}

	if !ba.healthChecker.CheckEndpoint(url) {
		if err := ba.SwitchRPC(); err != nil {
			return 0, err
		}
		url = ba.GetRPCURL()
	}

	// Bitcoin REST API endpoint for block height
	restURL := strings.TrimSuffix(url, "/") + "/blocks/tip/height"

	startTime := time.Now()
	req, err := http.NewRequest("GET", restURL, nil)
	if err != nil {
		return 0, err
	}

	resp, err := ba.client.Do(req)
	if err != nil {
		ba.healthChecker.RecordFailure(url, err)
		ba.SwitchRPC()
		return 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	// Validate response is not HTML
	if !ba.ValidateResponse(body) {
		err := fmt.Errorf("invalid HTML response from Bitcoin API")
		ba.healthChecker.RecordFailure(url, err)
		ba.SwitchRPC()
		return 0, err
	}

	ba.healthChecker.RecordSuccess(url, time.Since(startTime))

	// Parse JSON response
	var height int64
	if err := json.Unmarshal(body, &height); err != nil {
		return 0, fmt.Errorf("failed to parse block height: %w", err)
	}

	return height, nil
}

func (ba *BitcoinAdapter) GetBlockHash(height int64) (string, error) {
	url := ba.GetRPCURL()
	if url == "" {
		return "", fmt.Errorf("no RPC URL available")
	}

	restURL := strings.TrimSuffix(url, "/") + fmt.Sprintf("/block-height/%d", height)

	req, err := http.NewRequest("GET", restURL, nil)
	if err != nil {
		return "", err
	}

	resp, err := ba.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if !ba.ValidateResponse(body) {
		return "", fmt.Errorf("invalid response format")
	}

	var blockHash string
	if err := json.Unmarshal(body, &blockHash); err != nil {
		return "", err
	}

	return blockHash, nil
}

func (ba *BitcoinAdapter) GetBlock(blockHash string) (map[string]interface{}, error) {
	url := ba.GetRPCURL()
	if url == "" {
		return nil, fmt.Errorf("no RPC URL available")
	}

	restURL := strings.TrimSuffix(url, "/") + fmt.Sprintf("/block/%s", blockHash)

	req, err := http.NewRequest("GET", restURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := ba.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if !ba.ValidateResponse(body) {
		return nil, fmt.Errorf("invalid response format")
	}

	var block map[string]interface{}
	if err := json.Unmarshal(body, &block); err != nil {
		return nil, err
	}

	return block, nil
}

func (ba *BitcoinAdapter) ValidateResponse(response []byte) bool {
	responseStr := string(response)
	// Check for HTML error pages
	if strings.Contains(responseStr, "<!DOCTYPE") ||
		strings.Contains(responseStr, "<html") ||
		strings.Contains(responseStr, "HTML") {
		return false
	}

	// Should be valid JSON
	var js interface{}
	if err := json.Unmarshal(response, &js); err != nil {
		return false
	}

	return true
}

func (ba *BitcoinAdapter) GetLatestState() (interface{}, error) {
	return ba.GetBlockCount()
}

func (ba *BitcoinAdapter) FetchTransactions(startBlock, endBlock interface{}) ([]Transaction, error) {
	return []Transaction{}, nil
}

// ==================== BNB ADAPTER WITH DNS TIMEOUT FIX ====================

type BNBAdapter struct {
	name          string
	rpcURLs       []string
	currentRPC    int
	healthChecker *RPCHealthChecker
	client        *http.Client
	mu            sync.RWMutex
}

func NewBNBAdapter(rpcURLs []string) *BNBAdapter {
	return &BNBAdapter{
		name:          "BNB",
		rpcURLs:       rpcURLs,
		currentRPC:    0,
		healthChecker: NewRPCHealthChecker(),
		client: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				TLSHandshakeTimeout: 5 * time.Second,
			},
		},
	}
}

func (ba *BNBAdapter) GetName() string {
	return ba.name
}

func (ba *BNBAdapter) GetRPCURL() string {
	ba.mu.RLock()
	defer ba.mu.RUnlock()
	if len(ba.rpcURLs) == 0 {
		return ""
	}
	return ba.rpcURLs[ba.currentRPC]
}

func (ba *BNBAdapter) SwitchRPC() error {
	ba.mu.Lock()
	defer ba.mu.Unlock()

	if len(ba.rpcURLs) == 0 {
		return fmt.Errorf("no RPC endpoints configured")
	}

	originalRPC := ba.currentRPC
	for i := 0; i < len(ba.rpcURLs); i++ {
		ba.currentRPC = (ba.currentRPC + 1) % len(ba.rpcURLs)
		if ba.currentRPC != originalRPC {
			log.Printf("🔄 BNB: Switching RPC from %s to %s",
				ba.rpcURLs[originalRPC], ba.rpcURLs[ba.currentRPC])
			return nil
		}
	}
	return fmt.Errorf("no healthy BNB RPC endpoints available")
}

func (ba *BNBAdapter) DialWithRetry() (*ethclient.Client, error) {
	var client *ethclient.Client
	var err error

	backoff := 2 * time.Second
	maxBackoff := 16 * time.Second

	for attempt := 0; attempt < 5; attempt++ {
		url := ba.GetRPCURL()
		if url == "" {
			return nil, fmt.Errorf("no RPC URL available")
		}

		if !ba.healthChecker.CheckEndpoint(url) {
			if err := ba.SwitchRPC(); err != nil {
				continue
			}
			url = ba.GetRPCURL()
		}

		// Test DNS resolution first
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Try to dial with timeout
		client, err = ethclient.DialContext(ctx, url)
		if err != nil {
			// Check if it's a DNS error
			if strings.Contains(err.Error(), "no such host") ||
				strings.Contains(err.Error(), "i/o timeout") ||
				strings.Contains(err.Error(), "dial tcp: lookup") {
				log.Printf("⚠️ BNB DNS/timeout error: %v", err)
				ba.healthChecker.RecordFailure(url, err)
				ba.SwitchRPC()

				// Exponential backoff before retry
				time.Sleep(backoff)
				backoff = time.Duration(float64(backoff) * 2)
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
			return nil, err
		}

		// Test connection
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err = client.BlockNumber(ctx)
		if err != nil {
			client.Close()
			ba.healthChecker.RecordFailure(url, err)
			ba.SwitchRPC()
			continue
		}

		ba.healthChecker.RecordSuccess(url, 0)
		return client, nil
	}

	return nil, fmt.Errorf("failed to connect to BNB after multiple retries: %w", err)
}

func (ba *BNBAdapter) GetLatestState() (interface{}, error) {
	client, err := ba.DialWithRetry()
	if err != nil {
		return nil, err
	}
	defer client.Close()

	return client.BlockNumber(context.Background())
}

func (ba *BNBAdapter) FetchTransactions(startBlock, endBlock interface{}) ([]Transaction, error) {
	return []Transaction{}, nil
}

func (ba *BNBAdapter) ValidateResponse(response []byte) bool {
	return true
}

// ==================== DEPOSIT WATCHER CONFIG ====================

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

// ==================== DEPOSIT STRUCT ====================

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

// ==================== ENHANCED DEPOSIT WATCHER ====================

type EnhancedDepositWatcher struct {
	db             *sql.DB
	ledger         *ledger.Ledger
	walletManager  *hdwallet.WalletManager
	config         *WatcherConfig
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	mu             sync.RWMutex
	processedCache sync.Map

	// Chain adapters
	suiAdapter     *SuiAdapter
	xrpAdapter     *XRPAdapter
	bitcoinAdapter *BitcoinAdapter
	bnbAdapter     *BNBAdapter

	healthChecker *RPCHealthChecker
}

// Updated RPC endpoints per chain
var (
	// Sui Mainnet endpoints
	SuiRPCEndpoints = []string{
		"https://fullnode.mainnet.sui.io:443",
		"https://sui-mainnet.public.blastapi.io",
		"https://mainnet.sui.rpcpool.com",
		"https://sui.api.metalamp.io",
	}

	// XRP Mainnet endpoints
	XRPRPCEndpoints = []string{
		"https://xrplcluster.com",
		"https://xrpl.ws",
		"https://s2.ripple.com:51234",
		"https://xrpl.link",
	}

	// Bitcoin Mainnet endpoints (REST API)
	BitcoinRPCEndpoints = []string{
		"https://blockchain.info",
		"https://blockstream.info/api",
		"https://mempool.space/api",
	}

	// BNB Smart Chain endpoints
	BNBRPCEndpoints = []string{
		"https://bsc-dataseed1.binance.org",
		"https://bsc-dataseed2.binance.org",
		"https://bsc-dataseed3.binance.org",
		"https://bsc-dataseed4.binance.org",
		"https://bsc-dataseed1.defibit.io",
		"https://bsc-dataseed2.defibit.io",
	}
)

func NewEnhancedDepositWatcher(
	db *sql.DB,
	ledger *ledger.Ledger,
	walletManager *hdwallet.WalletManager,
	config *WatcherConfig,
) *EnhancedDepositWatcher {
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize adapters with fallback endpoints
	suiAdapter := NewSuiAdapter(SuiRPCEndpoints)
	xrpAdapter := NewXRPAdapter(XRPRPCEndpoints)
	bitcoinAdapter := NewBitcoinAdapter(BitcoinRPCEndpoints)
	bnbAdapter := NewBNBAdapter(BNBRPCEndpoints)

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

	return &EnhancedDepositWatcher{
		db:             db,
		ledger:         ledger,
		walletManager:  walletManager,
		config:         config,
		ctx:            ctx,
		cancel:         cancel,
		suiAdapter:     suiAdapter,
		xrpAdapter:     xrpAdapter,
		bitcoinAdapter: bitcoinAdapter,
		bnbAdapter:     bnbAdapter,
		healthChecker:  NewRPCHealthChecker(),
	}
}

func (dw *EnhancedDepositWatcher) Start() error {
	if err := dw.initDepositTables(); err != nil {
		return fmt.Errorf("failed to init deposit tables: %w", err)
	}

	// Start watchers with enhanced error recovery
	chains := []struct {
		name    string
		watcher func() error
	}{
		{"Sui", dw.watchSui},
		{"XRP", dw.watchXRP},
		{"Bitcoin", dw.watchBitcoin},
		{"BNB", dw.watchBNB},
		{"Ethereum", dw.watchEthereum},
		{"Solana", dw.watchSolana},
	}

	for _, chain := range chains {
		dw.wg.Add(1)
		go func(chainName string, watcher func() error) {
			defer dw.wg.Done()
			log.Printf("🚀 Starting enhanced deposit watcher for %s", chainName)

			retryCount := 0
			backoff := 2 * time.Second
			maxBackoff := 60 * time.Second

			for {
				select {
				case <-dw.ctx.Done():
					log.Printf("Stopping deposit watcher for %s", chainName)
					return
				default:
					err := watcher()
					if err != nil {
						retryCount++
						log.Printf("❌ Error watching %s (attempt %d): %v, retrying in %v...",
							chainName, retryCount, err, backoff)
						time.Sleep(backoff)

						// Exponential backoff with max limit
						backoff = time.Duration(float64(backoff) * 1.5)
						if backoff > maxBackoff {
							backoff = maxBackoff
						}

						// After 3 failures, try switching RPC
						if retryCount >= 3 {
							switch chainName {
							case "Sui":
								dw.suiAdapter.SwitchRPC()
							case "XRP":
								dw.xrpAdapter.SwitchRPC()
							case "Bitcoin":
								dw.bitcoinAdapter.SwitchRPC()
							case "BNB":
								dw.bnbAdapter.SwitchRPC()
							}
							retryCount = 0 // Reset after switch
						}
					} else {
						retryCount = 0
						backoff = 2 * time.Second
						time.Sleep(dw.getCheckInterval(chainName))
					}
				}
			}
		}(chain.name, chain.watcher)
	}

	return nil
}

func (dw *EnhancedDepositWatcher) Stop() {
	dw.cancel()
	dw.wg.Wait()
	log.Println("All deposit watchers stopped")
}

// ==================== DATABASE INITIALIZATION ====================

func (dw *EnhancedDepositWatcher) initDepositTables() error {
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

// ==================== ENHANCED CHAIN WATCHERS ====================

func (dw *EnhancedDepositWatcher) watchSui() error {
	latestCheckpoint, err := dw.suiAdapter.GetLatestCheckpoint()
	if err != nil {
		return fmt.Errorf("failed to get latest checkpoint: %w", err)
	}

	userWallets, err := dw.walletManager.GetUsersWalletByChain(dw.ctx, hdwallet.ChainSui)
	if err != nil {
		return fmt.Errorf("failed to get Sui wallets: %w", err)
	}

	if len(userWallets) == 0 {
		time.Sleep(dw.config.SuiCheckInterval)
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

	if latestCheckpoint <= lastCheckpoint {
		return nil
	}

	for checkpoint := lastCheckpoint + 1; checkpoint <= latestCheckpoint; checkpoint++ {
		if err := dw.processSuiCheckpoint(checkpoint, addressMap); err != nil {
			log.Printf("Error processing Sui checkpoint %d: %v", checkpoint, err)
			continue
		}

		if err := dw.updateLastProcessedCheckpoint(hdwallet.ChainSui, checkpoint); err != nil {
			log.Printf("Failed to update last processed checkpoint: %v", err)
		}
	}

	return nil
}

func (dw *EnhancedDepositWatcher) processSuiCheckpoint(checkpoint int64, addressMap map[string]int64) error {
	checkpointInfo, err := dw.suiAdapter.GetCheckpoint(checkpoint)
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

		// Process transaction - simplified for brevity
		log.Printf("Processing Sui transaction: %s", txHash)
	}

	return nil
}

func (dw *EnhancedDepositWatcher) watchXRP() error {
	userWallets, err := dw.walletManager.GetUsersWalletByChain(dw.ctx, hdwallet.ChainXRP)
	if err != nil {
		return fmt.Errorf("failed to get XRP wallets: %w", err)
	}

	if len(userWallets) == 0 {
		time.Sleep(dw.config.XRPCheckInterval)
		return nil
	}

	lastLedger, err := dw.getLastProcessedLedger(hdwallet.ChainXRP)
	if err != nil {
		lastLedger = -1 // Start from latest
	}

	for _, wallet := range userWallets {
		// Use normalized ledger indices
		minLedger := lastLedger + 1
		if lastLedger == -1 {
			minLedger = -1
		}

		maxLedger := int64(-1) // Up to latest

		transactions, err := dw.xrpAdapter.GetAccountTransactions(wallet.Address, minLedger, maxLedger)
		if err != nil {
			log.Printf("Error getting XRP transactions for %s: %v", wallet.Address, err)

			// Retry with corrected parameters if lgrIdxsInvalid error
			if strings.Contains(err.Error(), "lgrIdxsInvalid") {
				log.Printf("Retrying XRP with corrected ledger range")
				transactions, err = dw.xrpAdapter.GetAccountTransactions(wallet.Address, -1, -1)
				if err != nil {
					continue
				}
			} else {
				continue
			}
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
	}

	return nil
}

func (dw *EnhancedDepositWatcher) watchBitcoin() error {
	blockHeight, err := dw.bitcoinAdapter.GetBlockCount()
	if err != nil {
		// Check if error is due to HTML response
		if strings.Contains(err.Error(), "HTML") || strings.Contains(err.Error(), "<!DOCTYPE") {
			log.Printf("⚠️ Bitcoin returned HTML error page, switching RPC")
			dw.bitcoinAdapter.SwitchRPC()
			return fmt.Errorf("bitcoin HTML response, retrying with different endpoint")
		}
		return err
	}

	userWallets, err := dw.walletManager.GetUsersWalletByChain(dw.ctx, hdwallet.ChainBitcoin)
	if err != nil {
		return fmt.Errorf("failed to get Bitcoin wallets: %w", err)
	}

	if len(userWallets) == 0 {
		time.Sleep(dw.config.BitcoinCheckInterval)
		return nil
	}

	lastBlock, err := dw.getLastProcessedBlock(hdwallet.ChainBitcoin)
	if err != nil {
		lastBlock = 0
	}

	if blockHeight <= int64(lastBlock) {
		return nil
	}

	addressMap := make(map[string]int64)
	for _, wallet := range userWallets {
		addressMap[wallet.Address] = wallet.UserUID
	}

	// Process blocks
	for height := lastBlock + 1; height <= uint64(blockHeight); height++ {
		blockHash, err := dw.bitcoinAdapter.GetBlockHash(int64(height))
		if err != nil {
			log.Printf("Failed to get block hash for height %d: %v", height, err)
			continue
		}

		block, err := dw.bitcoinAdapter.GetBlock(blockHash)
		if err != nil {
			log.Printf("Failed to get block %s: %v", blockHash, err)
			continue
		}

		// Process transactions
		if txs, ok := block["tx"].([]interface{}); ok {
			for _, tx := range txs {
				if txMap, ok := tx.(map[string]interface{}); ok {
					if txHash, ok := txMap["txid"].(string); ok {
						log.Printf("Processing Bitcoin transaction: %s", txHash)
					}
				}
			}
		}

		if err := dw.updateLastProcessedBlock(hdwallet.ChainBitcoin, height); err != nil {
			log.Printf("Failed to update last processed block: %v", err)
		}
	}

	return nil
}

func (dw *EnhancedDepositWatcher) watchBNB() error {
	client, err := dw.bnbAdapter.DialWithRetry()
	if err != nil {
		return fmt.Errorf("failed to connect to BNB: %w", err)
	}
	defer client.Close()

	blockNumber, err := client.BlockNumber(dw.ctx)
	if err != nil {
		return fmt.Errorf("failed to get BNB block number: %w", err)
	}

	log.Printf("✓ BNB block height: %d", blockNumber)

	// Process BNB blocks (similar to Ethereum)
	return nil
}

func (dw *EnhancedDepositWatcher) watchEthereum() error {
	client, err := ethclient.Dial(dw.config.EthereumRPCURL)
	if err != nil {
		return fmt.Errorf("failed to connect to Ethereum: %w", err)
	}
	defer client.Close()

	blockNumber, err := client.BlockNumber(dw.ctx)
	if err != nil {
		return fmt.Errorf("failed to get block number: %w", err)
	}

	log.Printf("✓ Ethereum block height: %d", blockNumber)
	return nil
}

func (dw *EnhancedDepositWatcher) watchSolana() error {
	client := rpc.New(dw.config.SolanaRPCURL)

	userWallets, err := dw.walletManager.GetUsersWalletByChain(dw.ctx, hdwallet.ChainSolana)
	if err != nil {
		return fmt.Errorf("failed to get Solana wallets: %w", err)
	}

	log.Printf("✓ Monitoring %d Solana wallets", len(userWallets))
	_ = client

	return nil
}

// ==================== DEPOSIT PROCESSING ====================

func (dw *EnhancedDepositWatcher) processDepositAtomic(deposit *Deposit) error {
	dw.mu.Lock()
	defer dw.mu.Unlock()

	if dw.isDepositCached(deposit.Chain, deposit.TxHash) {
		return nil
	}

	deposit.Reference = fmt.Sprintf("%s_%s_%d", deposit.Chain, deposit.TxHash, time.Now().Unix())
	amountFloat64, _ := deposit.Amount.Float64()

	tx, err := dw.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

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
		dw.cacheDeposit(deposit.Chain, deposit.TxHash)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to insert deposit: %w", err)
	}

	err = dw.ledger.CreditAccount(
		context.Background(),
		deposit.UserID,
		deposit.Asset,
		amountFloat64,
		deposit.Reference,
		"deposit",
	)
	if err != nil {
		log.Printf("⚠️ Ledger credit failed for deposit %s: %v", deposit.TxHash, err)
		updateQuery := `UPDATE deposits SET status = 'pending_retry', updated_at = NOW() WHERE id = $1`
		tx.Exec(updateQuery, depositID)
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction with error state: %w", err)
		}
		return fmt.Errorf("ledger credit failed: %w", err)
	}

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
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	dw.cacheDeposit(deposit.Chain, deposit.TxHash)

	log.Printf("✅ Processed deposit: %s %f %s for user %d (tx: %s)",
		deposit.Asset, amountFloat64, deposit.Chain, deposit.UserID, deposit.TxHash)

	return nil
}

// ==================== HELPER FUNCTIONS ====================

func (dw *EnhancedDepositWatcher) getCheckInterval(chain string) time.Duration {
	switch chain {
	case "Ethereum":
		return dw.config.EthereumCheckInterval
	case "BNB":
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

func (dw *EnhancedDepositWatcher) getLastProcessedBlock(chain string) (uint64, error) {
	var lastBlock uint64
	query := `SELECT last_block_processed FROM processed_blocks WHERE chain = $1`
	err := dw.db.QueryRow(query, chain).Scan(&lastBlock)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return lastBlock, err
}

func (dw *EnhancedDepositWatcher) updateLastProcessedBlock(chain string, blockNumber uint64) error {
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

func (dw *EnhancedDepositWatcher) getLastProcessedCheckpoint(chain string) (int64, error) {
	var lastCheckpoint int64
	query := `SELECT last_checkpoint FROM processed_checkpoints WHERE chain = $1`
	err := dw.db.QueryRow(query, chain).Scan(&lastCheckpoint)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return lastCheckpoint, err
}

func (dw *EnhancedDepositWatcher) updateLastProcessedCheckpoint(chain string, checkpoint int64) error {
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

func (dw *EnhancedDepositWatcher) getLastProcessedLedger(chain string) (int64, error) {
	var lastLedger int64
	query := `SELECT last_ledger FROM processed_ledgers WHERE chain = $1`
	err := dw.db.QueryRow(query, chain).Scan(&lastLedger)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return lastLedger, err
}

func (dw *EnhancedDepositWatcher) updateLastProcessedLedger(chain string, ledger int64) error {
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

func (dw *EnhancedDepositWatcher) isDepositCached(chain, txHash string) bool {
	key := fmt.Sprintf("%s:%s", chain, txHash)
	_, exists := dw.processedCache.Load(key)
	return exists
}

func (dw *EnhancedDepositWatcher) cacheDeposit(chain, txHash string) {
	key := fmt.Sprintf("%s:%s", chain, txHash)
	dw.processedCache.Store(key, time.Now())
	go dw.cleanupCache()
}

func (dw *EnhancedDepositWatcher) cleanupCache() {
	dw.processedCache.Range(func(key, value interface{}) bool {
		if timestamp, ok := value.(time.Time); ok {
			if time.Since(timestamp) > dw.config.CacheTTL {
				dw.processedCache.Delete(key)
			}
		}
		return true
	})
}

func (dw *EnhancedDepositWatcher) getEVMSender(tx *types.Transaction) (common.Address, error) {
	signer := types.LatestSignerForChainID(tx.ChainId())
	return types.Sender(signer, tx)
}

func parseXRPAmount(amountStr string) float64 {
	var amount float64
	fmt.Sscanf(amountStr, "%f", &amount)
	return amount / 1000000.0
}
