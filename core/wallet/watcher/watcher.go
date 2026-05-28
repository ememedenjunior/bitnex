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

	"github.com/algorand/go-algorand-sdk/client/v2/algod"
	"github.com/algorand/go-algorand-sdk/client/v2/indexer"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/fbsobreira/gotron-sdk/pkg/address"
	"github.com/fbsobreira/gotron-sdk/pkg/client"
	tronCommon "github.com/fbsobreira/gotron-sdk/pkg/common"
	"github.com/fbsobreira/gotron-sdk/pkg/proto/api"
	"github.com/fbsobreira/gotron-sdk/pkg/proto/core"
	"github.com/gagliardetto/solana-go/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	// Open circuit after 5 failures
	if cb.Failures >= 5 && cb.State == "CLOSED" {
		cb.State = "OPEN"
		status.Healthy = false
		log.Printf("🔌 Circuit breaker for %s OPEN after %d failures", url, cb.Failures)
	}

	// Mark unhealthy after 3 failures
	if status.FailureCount >= 3 {
		status.Healthy = false
	}
}

// ==================== XRP ADAPTER ====================

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

	request := map[string]interface{}{
		"method": "account_tx",
		"params": []interface{}{
			map[string]interface{}{
				"account":          address,
				"ledger_index_min": minLedger,
				"ledger_index_max": maxLedger,
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

// ==================== BITCOIN ADAPTER ====================

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

	restURLs := []string{
		strings.TrimSuffix(url, "/") + "/blocks/tip/height",
		strings.TrimSuffix(url, "/") + "/api/blocks/tip/height",
		strings.TrimSuffix(url, "/") + "/q/getblockcount",
	}

	var lastErr error
	for _, restURL := range restURLs {
		startTime := time.Now()
		req, err := http.NewRequest("GET", restURL, nil)
		if err != nil {
			lastErr = err
			continue
		}

		resp, err := ba.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			lastErr = err
			continue
		}

		if ba.ValidateResponse(body) {
			var height int64
			if err := json.Unmarshal(body, &height); err == nil {
				ba.healthChecker.RecordSuccess(url, time.Since(startTime))
				return height, nil
			}
		}
	}

	ba.healthChecker.RecordFailure(url, lastErr)
	ba.SwitchRPC()
	return 0, fmt.Errorf("failed to get block height: %w", lastErr)
}

func (ba *BitcoinAdapter) ValidateResponse(response []byte) bool {
	responseStr := string(response)
	if strings.Contains(responseStr, "<!DOCTYPE") ||
		strings.Contains(responseStr, "<html") ||
		strings.Contains(responseStr, "HTML") {
		return false
	}

	var js interface{}
	if err := json.Unmarshal(response, &js); err != nil {
		return false
	}

	return true
}

// ==================== BNB ADAPTER ====================

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
	var ethClient *ethclient.Client
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		ethClient, err = ethclient.DialContext(ctx, url)
		cancel()

		if err != nil {
			if strings.Contains(err.Error(), "no such host") ||
				strings.Contains(err.Error(), "i/o timeout") ||
				strings.Contains(err.Error(), "dial tcp: lookup") {
				log.Printf("⚠️ BNB DNS/timeout error: %v", err)
				ba.healthChecker.RecordFailure(url, err)
				ba.SwitchRPC()

				time.Sleep(backoff)
				backoff = time.Duration(float64(backoff) * 2)
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
			return nil, err
		}

		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		_, err = ethClient.BlockNumber(ctx)
		cancel()

		if err != nil {
			ethClient.Close()
			ba.healthChecker.RecordFailure(url, err)
			ba.SwitchRPC()
			continue
		}

		ba.healthChecker.RecordSuccess(url, 0)
		return ethClient, nil
	}

	return nil, fmt.Errorf("failed to connect to BNB after multiple retries: %w", err)
}

// ==================== ALGORAND ADAPTER ====================

type AlgorandAdapter struct {
	name          string
	algodURLs     []string
	currentRPC    int
	healthChecker *RPCHealthChecker
	clients       []*algod.Client
	mu            sync.RWMutex
}

func NewAlgorandAdapter(algodURLs []string) *AlgorandAdapter {
	adapter := &AlgorandAdapter{
		name:          "Algorand",
		algodURLs:     algodURLs,
		currentRPC:    0,
		healthChecker: NewRPCHealthChecker(),
		clients:       make([]*algod.Client, len(algodURLs)),
	}

	for i, url := range algodURLs {
		client, err := algod.MakeClient(url, "")
		if err != nil {
			log.Printf("⚠️ Failed to create Algorand client for %s: %v", url, err)
			continue
		}
		adapter.clients[i] = client
	}

	return adapter
}

func (aa *AlgorandAdapter) GetName() string {
	return aa.name
}

func (aa *AlgorandAdapter) GetRPCURL() string {
	aa.mu.RLock()
	defer aa.mu.RUnlock()
	if len(aa.algodURLs) == 0 {
		return ""
	}
	return aa.algodURLs[aa.currentRPC]
}

func (aa *AlgorandAdapter) GetClient() *algod.Client {
	aa.mu.RLock()
	defer aa.mu.RUnlock()
	if len(aa.clients) == 0 || aa.currentRPC >= len(aa.clients) {
		return nil
	}
	return aa.clients[aa.currentRPC]
}

func (aa *AlgorandAdapter) SwitchRPC() error {
	aa.mu.Lock()
	defer aa.mu.Unlock()

	if len(aa.algodURLs) == 0 {
		return fmt.Errorf("no RPC endpoints configured")
	}

	originalRPC := aa.currentRPC
	for i := 0; i < len(aa.algodURLs); i++ {
		aa.currentRPC = (aa.currentRPC + 1) % len(aa.algodURLs)
		if aa.currentRPC != originalRPC && aa.clients[aa.currentRPC] != nil {
			log.Printf("🔄 Algorand: Switching RPC from %s to %s",
				aa.algodURLs[originalRPC], aa.algodURLs[aa.currentRPC])
			return nil
		}
	}
	return fmt.Errorf("no healthy Algorand RPC endpoints available")
}

func (aa *AlgorandAdapter) GetLatestBlock() (uint64, error) {
	client := aa.GetClient()
	if client == nil {
		return 0, fmt.Errorf("no client available")
	}

	if !aa.healthChecker.CheckEndpoint(aa.GetRPCURL()) {
		if err := aa.SwitchRPC(); err != nil {
			return 0, err
		}
		client = aa.GetClient()
	}

	startTime := time.Now()
	status, err := client.Status().Do(context.Background())
	if err != nil {
		aa.healthChecker.RecordFailure(aa.GetRPCURL(), err)
		aa.SwitchRPC()
		return 0, fmt.Errorf("failed to get status: %w", err)
	}

	aa.healthChecker.RecordSuccess(aa.GetRPCURL(), time.Since(startTime))
	return uint64(status.LastRound), nil
}

// ==================== TRON ADAPTER ====================

type TronAdapter struct {
	name          string
	grpcURLs      []string
	currentRPC    int
	healthChecker *RPCHealthChecker
	clients       []*client.GrpcClient
	mu            sync.RWMutex
}

func NewTronAdapter(grpcURLs []string) *TronAdapter {
	adapter := &TronAdapter{
		name:          "Tron",
		grpcURLs:      grpcURLs,
		currentRPC:    0,
		healthChecker: NewRPCHealthChecker(),
		clients:       make([]*client.GrpcClient, len(grpcURLs)),
	}

	for i, url := range grpcURLs {
		grpcClient := client.NewGrpcClient(url)
		err := grpcClient.Start(grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("⚠️ Failed to create Tron client for %s: %v", url, err)
			continue
		}
		adapter.clients[i] = grpcClient
	}

	return adapter
}

func (ta *TronAdapter) GetName() string {
	return ta.name
}

func (ta *TronAdapter) GetRPCURL() string {
	ta.mu.RLock()
	defer ta.mu.RUnlock()
	if len(ta.grpcURLs) == 0 {
		return ""
	}
	return ta.grpcURLs[ta.currentRPC]
}

func (ta *TronAdapter) GetClient() *client.GrpcClient {
	ta.mu.RLock()
	defer ta.mu.RUnlock()
	if len(ta.clients) == 0 || ta.currentRPC >= len(ta.clients) {
		return nil
	}
	return ta.clients[ta.currentRPC]
}

func (ta *TronAdapter) SwitchRPC() error {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	if len(ta.grpcURLs) == 0 {
		return fmt.Errorf("no RPC endpoints configured")
	}

	originalRPC := ta.currentRPC
	for i := 0; i < len(ta.grpcURLs); i++ {
		ta.currentRPC = (ta.currentRPC + 1) % len(ta.grpcURLs)
		if ta.currentRPC != originalRPC && ta.clients[ta.currentRPC] != nil {
			log.Printf("🔄 Tron: Switching RPC from %s to %s",
				ta.grpcURLs[originalRPC], ta.grpcURLs[ta.currentRPC])
			return nil
		}
	}
	return fmt.Errorf("no healthy Tron RPC endpoints available")
}

func (ta *TronAdapter) GetLatestBlock() (int64, error) {
	client := ta.GetClient()
	if client == nil {
		return 0, fmt.Errorf("no client available")
	}

	if !ta.healthChecker.CheckEndpoint(ta.GetRPCURL()) {
		if err := ta.SwitchRPC(); err != nil {
			return 0, err
		}
		client = ta.GetClient()
	}

	startTime := time.Now()
	block, err := client.GetNowBlock()
	if err != nil {
		ta.healthChecker.RecordFailure(ta.GetRPCURL(), err)
		ta.SwitchRPC()
		return 0, fmt.Errorf("failed to get latest block: %w", err)
	}

	ta.healthChecker.RecordSuccess(ta.GetRPCURL(), time.Since(startTime))
	return block.GetBlockHeader().GetRawData().GetNumber(), nil
}

func (ta *TronAdapter) GetBlockByNumber(blockNumber int64) (*api.BlockExtention, error) {
	client := ta.GetClient()
	if client == nil {
		return nil, fmt.Errorf("no client available")
	}

	block, err := client.GetBlockByNum(blockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	return block, nil
}

func (ta *TronAdapter) GetTransactionByID(txID string) (*core.Transaction, error) {
	client := ta.GetClient()
	if client == nil {
		return nil, fmt.Errorf("no client available")
	}

	tx, err := client.GetTransactionByID(txID)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}

	return tx, nil
}

// ==================== SUI ADAPTER ====================

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

	if !sa.ValidateResponse(response) {
		err := fmt.Errorf("invalid response format (HTML or malformed JSON)")
		sa.healthChecker.RecordFailure(url, err)
		sa.SwitchRPC()
		return 0, err
	}

	sa.healthChecker.RecordSuccess(url, time.Since(startTime))

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
	responseStr := string(response)
	if strings.Contains(responseStr, "<!DOCTYPE") ||
		strings.Contains(responseStr, "<html") ||
		strings.Contains(responseStr, "HTML") {
		return false
	}

	var js interface{}
	if err := json.Unmarshal(response, &js); err != nil {
		return false
	}

	return true
}

// ==================== DEPOSIT WATCHER CONFIG ====================

type WatcherConfig struct {
	EthereumRPCURL        string
	EthereumStartBlock    uint64
	EthereumCheckInterval time.Duration

	SolanaRPCURL        string
	SolanaCheckInterval time.Duration
	SolanaCommitment    rpc.CommitmentType

	BitcoinRPCURL        string
	BitcoinRPCUser       string
	BitcoinRPCPassword   string
	BitcoinCheckInterval time.Duration

	BNBRPCURL     string
	BNBStartBlock uint64

	SuiRPCURL        string
	SuiCheckInterval time.Duration

	XRPRPCURL        string
	XRPCheckInterval time.Duration

	AlgorandRPCURLs       []string
	AlgorandIndexerURLs   []string
	AlgorandCheckInterval time.Duration

	TronRPCURLs       []string
	TronCheckInterval time.Duration

	MaxConfirmations uint64
	WorkerCount      int
	RetryAttempts    int
	RetryDelay       time.Duration
	RetryBackoff     float64
	CacheTTL         time.Duration
}

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

	suiAdapter      *SuiAdapter
	xrpAdapter      *XRPAdapter
	bitcoinAdapter  *BitcoinAdapter
	bnbAdapter      *BNBAdapter
	algorandAdapter *AlgorandAdapter
	algorandIndexer *indexer.Client
	tronAdapter     *TronAdapter

	healthChecker *RPCHealthChecker
}

var (
	SuiRPCEndpoints = []string{
		"https://fullnode.mainnet.sui.io:443",
		"https://sui-mainnet.public.blastapi.io",
		"https://mainnet.sui.rpcpool.com",
		"https://sui.api.metalamp.io",
	}

	XRPRPCEndpoints = []string{
		"https://xrplcluster.com",
		"https://xrpl.ws",
		"https://s2.ripple.com:51234",
		"https://xrpl.link",
	}

	BitcoinRPCEndpoints = []string{
		"https://blockchain.info",
		"https://blockstream.info/api",
		"https://mempool.space/api",
	}

	BNBRPCEndpoints = []string{
		"https://bsc-dataseed1.binance.org",
		"https://bsc-dataseed2.binance.org",
		"https://bsc-dataseed3.binance.org",
		"https://bsc-dataseed4.binance.org",
		"https://bsc-dataseed1.defibit.io",
		"https://bsc-dataseed2.defibit.io",
	}

	AlgorandRPCEndpoints = []string{
		"https://mainnet-api.algonode.cloud",
		"https://algoexplorerapi.io",
		"https://mainnet.algorand.se",
		"https://mainnet-api.algorand.network",
	}

	AlgorandIndexerEndpoints = []string{
		"https://mainnet-idx.algonode.cloud",
		"https://algoexplorerapi.io/idx2",
		"https://indexer.algorand.se",
	}

	TronGRPCEndpoints = []string{
		"grpc.trongrid.io:50051",
		"grpc.tronstack.io:50051",
		"grpc.trxplaza.io:50051",
	}
)

func NewEnhancedDepositWatcher(
	db *sql.DB,
	ledger *ledger.Ledger,
	walletManager *hdwallet.WalletManager,
	config *WatcherConfig,
) *EnhancedDepositWatcher {
	ctx, cancel := context.WithCancel(context.Background())

	suiAdapter := NewSuiAdapter(SuiRPCEndpoints)
	xrpAdapter := NewXRPAdapter(XRPRPCEndpoints)
	bitcoinAdapter := NewBitcoinAdapter(BitcoinRPCEndpoints)
	bnbAdapter := NewBNBAdapter(BNBRPCEndpoints)
	algorandAdapter := NewAlgorandAdapter(AlgorandRPCEndpoints)
	tronAdapter := NewTronAdapter(TronGRPCEndpoints)

	// Initialize Algorand Indexer client
	var algorandIndexer *indexer.Client
	for _, idxURL := range AlgorandIndexerEndpoints {
		idxClient, err := indexer.MakeClient(idxURL, "")
		if err == nil {
			algorandIndexer = idxClient
			log.Printf("✅ Connected to Algorand Indexer at %s", idxURL)
			break
		}
		log.Printf("⚠️ Failed to create Algorand Indexer client for %s: %v", idxURL, err)
	}

	if algorandIndexer == nil {
		log.Printf("⚠️ No Algorand Indexer endpoints available, will use algod only for basic monitoring")
	}

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
	if config.AlgorandCheckInterval == 0 {
		config.AlgorandCheckInterval = 10 * time.Second
	}
	if config.TronCheckInterval == 0 {
		config.TronCheckInterval = 10 * time.Second
	}
	if config.EthereumCheckInterval == 0 {
		config.EthereumCheckInterval = 10 * time.Second
	}
	if config.SolanaCheckInterval == 0 {
		config.SolanaCheckInterval = 10 * time.Second
	}
	if config.BitcoinCheckInterval == 0 {
		config.BitcoinCheckInterval = 30 * time.Second
	}
	if config.SuiCheckInterval == 0 {
		config.SuiCheckInterval = 10 * time.Second
	}
	if config.XRPCheckInterval == 0 {
		config.XRPCheckInterval = 10 * time.Second
	}

	return &EnhancedDepositWatcher{
		db:              db,
		ledger:          ledger,
		walletManager:   walletManager,
		config:          config,
		ctx:             ctx,
		cancel:          cancel,
		suiAdapter:      suiAdapter,
		xrpAdapter:      xrpAdapter,
		bitcoinAdapter:  bitcoinAdapter,
		bnbAdapter:      bnbAdapter,
		algorandAdapter: algorandAdapter,
		algorandIndexer: algorandIndexer,
		tronAdapter:     tronAdapter,
		healthChecker:   NewRPCHealthChecker(),
	}
}

func (dw *EnhancedDepositWatcher) Start() error {
	if err := dw.initDepositTables(); err != nil {
		return fmt.Errorf("failed to init deposit tables: %w", err)
	}

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
		{"Algorand", dw.watchAlgorand},
		{"Tron", dw.watchTron},
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

						backoff = time.Duration(float64(backoff) * 1.5)
						if backoff > maxBackoff {
							backoff = maxBackoff
						}

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
							case "Algorand":
								dw.algorandAdapter.SwitchRPC()
							case "Tron":
								dw.tronAdapter.SwitchRPC()
							}
							retryCount = 0
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
		`CREATE TABLE IF NOT EXISTS processed_rounds (
			chain VARCHAR(50) PRIMARY KEY,
			last_round BIGINT NOT NULL,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS processed_tron_blocks (
			chain VARCHAR(50) PRIMARY KEY,
			last_block BIGINT NOT NULL,
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
		lastLedger = -1
	}

	for _, wallet := range userWallets {
		minLedger := lastLedger + 1
		if lastLedger == -1 {
			minLedger = -1
		}
		maxLedger := int64(-1)

		transactions, err := dw.xrpAdapter.GetAccountTransactions(wallet.Address, minLedger, maxLedger)
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
	}

	return nil
}

func (dw *EnhancedDepositWatcher) watchBitcoin() error {
	blockHeight, err := dw.bitcoinAdapter.GetBlockCount()
	if err != nil {
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

	log.Printf("✓ Bitcoin block height: %d, processing from %d", blockHeight, lastBlock+1)

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

// ==================== ALGORAND WATCHER WITH INDEXER ====================

func (dw *EnhancedDepositWatcher) watchAlgorand() error {
	if dw.algorandIndexer == nil {
		log.Printf("⚠️ Algorand Indexer not available, skipping Algorand monitoring")
		time.Sleep(dw.config.AlgorandCheckInterval)
		return nil
	}

	latestRound, err := dw.algorandAdapter.GetLatestBlock()
	if err != nil {
		return fmt.Errorf("failed to get latest round: %w", err)
	}

	userWallets, err := dw.walletManager.GetUsersWalletByChain(dw.ctx, hdwallet.ChainAlgorand)
	if err != nil {
		return fmt.Errorf("failed to get Algorand wallets: %w", err)
	}

	if len(userWallets) == 0 {
		time.Sleep(dw.config.AlgorandCheckInterval)
		return nil
	}

	lastRound, err := dw.getLastProcessedRound(hdwallet.ChainAlgorand)
	if err != nil {
		lastRound = 0
	}

	batchSize := uint64(20)
	for round := lastRound + 1; round <= uint64(latestRound); round++ {
		// Process each wallet address for this round
		for _, wallet := range userWallets {
			if err := dw.processAlgorandRoundWithIndexer(round, wallet.Address, wallet.UserUID); err != nil {
				log.Printf("Error processing Algorand round %d for address %s: %v", round, wallet.Address, err)
			}
		}

		if err := dw.updateLastProcessedRound(hdwallet.ChainAlgorand, round); err != nil {
			log.Printf("Failed to update last processed round: %v", err)
		}

		if round%batchSize == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	return nil
}

func (dw *EnhancedDepositWatcher) processAlgorandRoundWithIndexer(round uint64, address string, userID int64) error {
	ctx, cancel := context.WithTimeout(dw.ctx, 30*time.Second)
	defer cancel()

	// Look up transactions for this address at this specific round
	txnResults, err := dw.algorandIndexer.LookupAccountTransactions(address).
		MinRound(round).
		MaxRound(round).
		Do(ctx)

	if err != nil {
		return fmt.Errorf("failed to lookup transactions: %w", err)
	}

	for _, txn := range txnResults.Transactions {
		txID := txn.Id // Indexer provides the transaction ID directly

		if dw.isDepositCached(hdwallet.ChainAlgorand, txID) {
			continue
		}

		// Check if this is a payment transaction TO our address
		if txn.PaymentTransaction.Amount != 0 && txn.PaymentTransaction.Receiver == address {
			amount := float64(txn.PaymentTransaction.Amount) / 1_000_000

			if amount > 0 {
				deposit := &Deposit{
					Chain:       hdwallet.ChainAlgorand,
					TxHash:      txID,
					FromAddress: txn.Sender,
					ToAddress:   address,
					Asset:       "ALGO",
					Amount:      big.NewFloat(amount),
					BlockNumber: round,
					Timestamp:   time.Unix(int64(txn.RoundTime), 0),
					UserID:      userID,
				}

				if err := dw.processDepositAtomic(deposit); err != nil {
					log.Printf("Failed to process Algorand deposit: %v", err)
				}
			}
		}

		// Check for ASA (Algorand Standard Asset) transfers
		if txn.AssetTransferTransaction.Amount != 0 && txn.AssetTransferTransaction.Receiver == address {
			amount := float64(txn.AssetTransferTransaction.Amount) / 1_000_000
			assetID := txn.AssetTransferTransaction.AssetId

			if amount > 0 {
				deposit := &Deposit{
					Chain:       hdwallet.ChainAlgorand,
					TxHash:      txID,
					FromAddress: txn.Sender,
					ToAddress:   address,
					Asset:       fmt.Sprintf("ASA-%d", assetID),
					Amount:      big.NewFloat(amount),
					BlockNumber: round,
					Timestamp:   time.Unix(int64(txn.RoundTime), 0),
					UserID:      userID,
				}

				if err := dw.processDepositAtomic(deposit); err != nil {
					log.Printf("Failed to process Algorand ASA deposit: %v", err)
				}
			}
		}
	}

	return nil
}

// ==================== TRON WATCHER ====================

func (dw *EnhancedDepositWatcher) watchTron() error {
	latestBlock, err := dw.tronAdapter.GetLatestBlock()
	if err != nil {
		return fmt.Errorf("failed to get latest block: %w", err)
	}

	userWallets, err := dw.walletManager.GetUsersWalletByChain(dw.ctx, hdwallet.ChainTron)
	if err != nil {
		return fmt.Errorf("failed to get Tron wallets: %w", err)
	}

	if len(userWallets) == 0 {
		time.Sleep(dw.config.TronCheckInterval)
		return nil
	}

	addressMap := make(map[string]int64)
	for _, wallet := range userWallets {
		addressMap[wallet.Address] = wallet.UserUID
	}

	lastBlock, err := dw.getLastProcessedTronBlock(hdwallet.ChainTron)
	if err != nil {
		lastBlock = 0
	}

	for blockNum := lastBlock + 1; blockNum <= latestBlock; blockNum++ {
		if err := dw.processTronBlock(blockNum, addressMap); err != nil {
			log.Printf("Error processing Tron block %d: %v", blockNum, err)
			continue
		}
		if err := dw.updateLastProcessedTronBlock(hdwallet.ChainTron, blockNum); err != nil {
			log.Printf("Failed to update last processed block: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	return nil
}

func (dw *EnhancedDepositWatcher) processTronBlock(blockNumber int64, addressMap map[string]int64) error {
	block, err := dw.tronAdapter.GetBlockByNumber(blockNumber)
	if err != nil {
		return fmt.Errorf("failed to get block: %w", err)
	}

	if block == nil || len(block.Transactions) == 0 {
		return nil
	}

	for _, tx := range block.Transactions {
		txID := tronCommon.BytesToHexString(tx.GetTxid())

		if dw.isDepositCached(hdwallet.ChainTron, txID) {
			continue
		}

		txInfo, err := dw.tronAdapter.GetTransactionByID(txID)
		if err != nil {
			log.Printf("Failed to get transaction details for %s: %v", txID, err)
			continue
		}

		for _, contract := range txInfo.GetRawData().GetContract() {
			if contract.GetType() == core.Transaction_Contract_TransferContract {
				transferContract := &core.TransferContract{}
				if err := contract.GetParameter().UnmarshalTo(transferContract); err != nil {
					continue
				}

				toAddress := address.Address(transferContract.GetToAddress()).String()
				fromAddress := address.Address(transferContract.GetOwnerAddress()).String()

				if userID, exists := addressMap[toAddress]; exists {
					amount := float64(transferContract.GetAmount()) / 1_000_000
					if amount > 0 {
						deposit := &Deposit{
							Chain:       hdwallet.ChainTron,
							TxHash:      txID,
							FromAddress: fromAddress,
							ToAddress:   toAddress,
							Asset:       "TRX",
							Amount:      big.NewFloat(amount),
							BlockNumber: uint64(blockNumber),
							Timestamp:   time.Unix(block.GetBlockHeader().GetRawData().GetTimestamp()/1000, 0),
							UserID:      userID,
						}
						if err := dw.processDepositAtomic(deposit); err != nil {
							log.Printf("Failed to process Tron deposit: %v", err)
						}
					}
				}
			}
		}
	}

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

func (dw *EnhancedDepositWatcher) getLastProcessedRound(chain string) (uint64, error) {
	var lastRound uint64
	query := `SELECT last_round FROM processed_rounds WHERE chain = $1`
	err := dw.db.QueryRow(query, chain).Scan(&lastRound)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return lastRound, err
}

func (dw *EnhancedDepositWatcher) updateLastProcessedRound(chain string, round uint64) error {
	query := `
		INSERT INTO processed_rounds (chain, last_round, updated_at)
		VALUES ($1, $2, CURRENT_TIMESTAMP)
		ON CONFLICT (chain) DO UPDATE SET
			last_round = EXCLUDED.last_round,
			updated_at = CURRENT_TIMESTAMP
	`
	_, err := dw.db.Exec(query, chain, round)
	return err
}

func (dw *EnhancedDepositWatcher) getLastProcessedTronBlock(chain string) (int64, error) {
	var lastBlock int64
	query := `SELECT last_block FROM processed_tron_blocks WHERE chain = $1`
	err := dw.db.QueryRow(query, chain).Scan(&lastBlock)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return lastBlock, err
}

func (dw *EnhancedDepositWatcher) updateLastProcessedTronBlock(chain string, block int64) error {
	query := `
		INSERT INTO processed_tron_blocks (chain, last_block, updated_at)
		VALUES ($1, $2, CURRENT_TIMESTAMP)
		ON CONFLICT (chain) DO UPDATE SET
			last_block = EXCLUDED.last_block,
			updated_at = CURRENT_TIMESTAMP
	`
	_, err := dw.db.Exec(query, chain, block)
	return err
}

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
	case "Algorand":
		return dw.config.AlgorandCheckInterval
	case "Tron":
		return dw.config.TronCheckInterval
	default:
		return 10 * time.Second
	}
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

func (dw *EnhancedDepositWatcher) getEVMSender(tx *ethTypes.Transaction) (common.Address, error) {
	signer := ethTypes.LatestSignerForChainID(tx.ChainId())
	return ethTypes.Sender(signer, tx)
}

func parseXRPAmount(amountStr string) float64 {
	var amount float64
	fmt.Sscanf(amountStr, "%f", &amount)
	return amount / 1000000.0
}
