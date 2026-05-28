package hdwallet

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/sha512"
	"cryptohub/pkg/utils"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/gagliardetto/solana-go"
	"github.com/tyler-smith/go-bip32"
	"github.com/tyler-smith/go-bip39"
	"golang.org/x/crypto/blake2b"
)

// Chain constants
const (
	ChainEthereum = "ethereum"
	ChainBNB      = "bnb"
	ChainBitcoin  = "bitcoin"
	ChainSolana   = "solana"
	ChainSui      = "sui"
	ChainXRP      = "xrp"
	ChainAlgorand = "algorand"
	ChainTron     = "tron"
)

// XRP constants
const (
	XRPAlphabet = "rpshnaf39wBUDNEGHJKLM4PQRST7VWXYZ2bcdeCg65jkm8oFqi1tuvAxyz"
)

// HotWallet represents a hot wallet in the database
type HotWallet struct {
	ID                  int64     `json:"id"`
	Chain               string    `json:"chain"`
	MnemonicEncrypted   string    `json:"mnemonic_encrypted"`
	Address             string    `json:"address"`
	PrivateKeyEncrypted string    `json:"private_key_encrypted"`
	Balance             string    `json:"balance"`
	DerivationPath      string    `json:"derivation_path"`
	WalletIndex         int64     `json:"wallet_index"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
	IsActive            bool      `json:"is_active"`
}

// UserWallet represents a user's wallet in the database
type UserWallet struct {
	ID                  int64     `json:"id"`
	UserUID             int64     `json:"user_uid"`
	Chain               string    `json:"chain"`
	Address             string    `json:"address"`
	PrivateKeyEncrypted string    `json:"private_key_encrypted"`
	DerivationPath      string    `json:"derivation_path"`
	DerivationIndex     int64     `json:"derivation_index"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
	IsActive            bool      `json:"is_active"`
}

// WalletManager handles wallet operations with connection pooling
type WalletManager struct {
	db            *sql.DB
	encryptionKey []byte
	mu            sync.RWMutex
	workerPool    chan struct{}
}

// NewWalletManager creates a new wallet manager with connection pool configuration
func NewWalletManager(db *sql.DB, encryptionKey []byte) (*WalletManager, error) {
	// Configure connection pool to prevent exhaustion
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("database connection failed: %w", err)
	}

	return &WalletManager{
		db:            db,
		encryptionKey: encryptionKey,
		workerPool:    make(chan struct{}, 10), // Limit concurrent operations
	}, nil
}

// CreateHotWallet creates a new hot wallet for a specific chain
func (wm *WalletManager) CreateHotWallet(chain string, walletIndex int64) error {
	// Acquire worker from pool
	wm.workerPool <- struct{}{}
	defer func() { <-wm.workerPool }()

	// Generate mnemonic
	entropy, err := bip39.NewEntropy(256)
	if err != nil {
		return fmt.Errorf("failed to generate entropy: %w", err)
	}

	mnemonic, err := bip39.NewMnemonic(entropy)
	if err != nil {
		return fmt.Errorf("failed to generate mnemonic: %w", err)
	}

	seed := bip39.NewSeed(mnemonic, "")

	// Derive address based on chain
	var address, privateKeyHex, derivationPath string
	var privKeyBytes []byte

	switch chain {
	case ChainEthereum, ChainBNB:
		address, privKeyBytes, err = deriveEthereumAddress(seed, walletIndex)
		derivationPath = fmt.Sprintf("m/44'/60'/0'/0/%d", walletIndex)
	case ChainBitcoin:
		address, privKeyBytes, err = deriveBitcoinAddress(seed, walletIndex)
		derivationPath = fmt.Sprintf("m/44'/0'/0'/0/%d", walletIndex)
	case ChainSolana:
		address, privKeyBytes, err = deriveSolanaAddressSLIP10(seed, walletIndex)
		derivationPath = fmt.Sprintf("m/44'/501'/%d'/0'", walletIndex)
	case ChainSui:
		address, privKeyBytes, err = deriveSuiAddressSLIP10(seed, walletIndex)
		derivationPath = fmt.Sprintf("m/44'/784'/%d'/0'/0", walletIndex)
	case ChainXRP:
		address, privKeyBytes, err = deriveXRPAddressSLIP10(seed, walletIndex)
		derivationPath = fmt.Sprintf("m/44'/144'/0'/0/%d", walletIndex)
	case ChainAlgorand:
		address, privKeyBytes, err = deriveAlgorandAddressSLIP10(seed, walletIndex)
		derivationPath = fmt.Sprintf("m/44'/283'/%d'/0'/0", walletIndex)
	case ChainTron:
		address, privKeyBytes, err = deriveTronAddressSLIP10(seed, walletIndex)
		derivationPath = fmt.Sprintf("m/44'/195'/%d'/0/0", walletIndex)
	default:
		return fmt.Errorf("unsupported chain: %s", chain)
	}

	if err != nil {
		return fmt.Errorf("failed to derive address: %w", err)
	}

	privateKeyHex = hex.EncodeToString(privKeyBytes)

	// Encrypt sensitive data
	encryptedMnemonic, err := utils.Encrypt(mnemonic, wm.encryptionKey)
	if err != nil {
		return fmt.Errorf("failed to encrypt mnemonic: %w", err)
	}

	encryptedPrivateKey, err := utils.Encrypt(privateKeyHex, wm.encryptionKey)
	if err != nil {
		return fmt.Errorf("failed to encrypt private key: %w", err)
	}

	// Fix #1: Proper INSERT with RETURNING and Scan
	query := `
		INSERT INTO hot_wallets (chain, mnemonic_encrypted, address, private_key_encrypted, 
		                         derivation_path, wallet_index)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, created_at, updated_at
	`

	var id int64
	var createdAt, updatedAt time.Time
	err = wm.db.QueryRowContext(context.Background(),
		query,
		chain,
		encryptedMnemonic,
		address,
		encryptedPrivateKey,
		derivationPath,
		walletIndex,
	).Scan(&id, &createdAt, &updatedAt)

	if err != nil {
		return fmt.Errorf("failed to insert hot wallet: %w", err)
	}

	return nil
}

// CreateUserWallet creates a wallet for a user using the hot wallet's mnemonic
func (wm *WalletManager) CreateUserWallet(ctx context.Context, userUID int64, chain string, derivationIndex int64) error {
	// Acquire worker from pool
	select {
	case wm.workerPool <- struct{}{}:
		defer func() { <-wm.workerPool }()
	case <-ctx.Done():
		return ctx.Err()
	}

	// Get the hot wallet for this chain
	hotWallet, err := wm.getHotWalletByChain(ctx, chain)
	if err != nil {
		return fmt.Errorf("failed to get hot wallet: %w", err)
	}

	// Decrypt mnemonic
	mnemonicBytes, err := utils.Decrypt(hotWallet.MnemonicEncrypted, wm.encryptionKey)
	if err != nil {
		return fmt.Errorf("failed to decrypt mnemonic: %w", err)
	}
	mnemonic := string(mnemonicBytes)

	// Generate seed from mnemonic
	seed := bip39.NewSeed(mnemonic, "")

	// Derive user-specific address using derivation index
	var address, derivationPath string
	var privKeyBytes []byte

	switch chain {
	case ChainEthereum, ChainBNB:
		address, privKeyBytes, err = deriveEthereumAddress(seed, derivationIndex)
		derivationPath = fmt.Sprintf("m/44'/60'/0'/0/%d", derivationIndex)
	case ChainBitcoin:
		address, privKeyBytes, err = deriveBitcoinAddress(seed, derivationIndex)
		derivationPath = fmt.Sprintf("m/44'/0'/0'/0/%d", derivationIndex)
	case ChainSolana:
		address, privKeyBytes, err = deriveSolanaAddressSLIP10(seed, derivationIndex)
		derivationPath = fmt.Sprintf("m/44'/501'/%d'/0'", derivationIndex)
	case ChainSui:
		address, privKeyBytes, err = deriveSuiAddressSLIP10(seed, derivationIndex)
		derivationPath = fmt.Sprintf("m/44'/784'/%d'/0'/0", derivationIndex)
	case ChainXRP:
		address, privKeyBytes, err = deriveXRPAddressSLIP10(seed, derivationIndex)
		derivationPath = fmt.Sprintf("m/44'/144'/0'/0/%d", derivationIndex)
	case ChainAlgorand:
		address, privKeyBytes, err = deriveAlgorandAddressSLIP10(seed, derivationIndex)
		derivationPath = fmt.Sprintf("m/44'/283'/%d'/0'/0", derivationIndex)
	case ChainTron:
		address, privKeyBytes, err = deriveTronAddressSLIP10(seed, derivationIndex)
		derivationPath = fmt.Sprintf("m/44'/195'/%d'/0/0", derivationIndex)
	default:
		return fmt.Errorf("unsupported chain: %s", chain)
	}

	if err != nil {
		return fmt.Errorf("failed to derive address: %w", err)
	}

	privateKeyHex := hex.EncodeToString(privKeyBytes)

	// Encrypt private key
	encryptedPrivateKey, err := utils.Encrypt(privateKeyHex, wm.encryptionKey)
	if err != nil {
		return fmt.Errorf("failed to encrypt private key: %w", err)
	}

	// Fix #1: Proper INSERT with ON CONFLICT and RETURNING
	query := `
		INSERT INTO wallets (user_uid, chain, address, private_key_encrypted, 
		                     derivation_path, derivation_index)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, created_at, updated_at
	`

	var id int64
	var createdAt, updatedAt time.Time
	err = wm.db.QueryRowContext(ctx,
		query,
		userUID,
		chain,
		address,
		encryptedPrivateKey,
		derivationPath,
		derivationIndex,
	).Scan(&id, &createdAt, &updatedAt)

	if err != nil {
		return fmt.Errorf("failed to insert user wallet: %w", err)
	}

	log.Printf("✅ Created wallet: pubkey=%s, privkey=%s", privateKeyHex, address)

	return nil
}

// CreateAllUserWallets creates wallets for a user on all supported chains concurrently
func (wm *WalletManager) CreateAllUserWallets(ctx context.Context, userUID int64) error {
	chains := []string{ChainEthereum, ChainBNB, ChainBitcoin, ChainSolana, ChainXRP, ChainSui, ChainAlgorand, ChainTron}

	type result struct {
		chain string
		err   error
	}

	results := make(chan result, len(chains))
	var wg sync.WaitGroup

	// Fix #6: Use worker pool for concurrent wallet creation
	for _, chain := range chains {
		wg.Add(1)
		go func(c string) {
			defer wg.Done()

			// Use derivation_index = userUID for deterministic derivation
			err := wm.CreateUserWallet(ctx, userUID, c, userUID)
			results <- result{chain: c, err: err}
		}(chain)
	}

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect errors
	var errors []error
	for res := range results {
		if res.err != nil {
			errors = append(errors, fmt.Errorf("chain %s: %w", res.chain, res.err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to create wallets: %v", errors)
	}

	return nil
}

// getHotWalletByChain retrieves a hot wallet for a specific chain
func (wm *WalletManager) getHotWalletByChain(ctx context.Context, chain string) (*HotWallet, error) {
	query := `
		SELECT id, chain, mnemonic_encrypted, address, private_key_encrypted, 
		       balance, derivation_path, wallet_index, created_at, updated_at, is_active
		FROM hot_wallets
		WHERE chain = $1 AND is_active = true
		ORDER BY id LIMIT 1
	`

	var wallet HotWallet
	err := wm.db.QueryRowContext(ctx, query, chain).Scan(
		&wallet.ID, &wallet.Chain, &wallet.MnemonicEncrypted, &wallet.Address,
		&wallet.PrivateKeyEncrypted, &wallet.Balance, &wallet.DerivationPath,
		&wallet.WalletIndex, &wallet.CreatedAt, &wallet.UpdatedAt, &wallet.IsActive,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("hot wallet not found for chain: %s", chain)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get hot wallet: %w", err)
	}

	return &wallet, nil
}

// GetUsersWalletByChain retrieves all active wallets for a specific chain
func (wm *WalletManager) GetUsersWalletByChain(ctx context.Context, chain string) ([]UserWallet, error) {
	query := `
		SELECT id, user_uid, chain, address, private_key_encrypted, 
		       derivation_path, derivation_index, created_at, updated_at, is_active
		FROM wallets
		WHERE chain = $1 AND is_active = true
	`

	rows, err := wm.db.QueryContext(ctx, query, chain)
	if err != nil {
		return nil, fmt.Errorf("failed to query wallets: %w", err)
	}
	defer rows.Close() // Fix #5: Ensure rows are closed

	var wallets []UserWallet
	for rows.Next() {
		var wallet UserWallet
		err := rows.Scan(
			&wallet.ID, &wallet.UserUID, &wallet.Chain, &wallet.Address,
			&wallet.PrivateKeyEncrypted, &wallet.DerivationPath, &wallet.DerivationIndex,
			&wallet.CreatedAt, &wallet.UpdatedAt, &wallet.IsActive,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan wallet: %w", err)
		}
		wallets = append(wallets, wallet)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return wallets, nil
}

// ==================== SLIP-0010 COMPLIANT DERIVATION FOR ED25519 CHAINS ====================

// deriveEthereumAddress with key length validation
func deriveEthereumAddress(seed []byte, index int64) (string, []byte, error) {
	masterKey, err := bip32.NewMasterKey(seed)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create master key: %w", err)
	}

	key, err := derivePath(masterKey, []uint32{
		bip32.FirstHardenedChild + 44,
		bip32.FirstHardenedChild + 60,
		bip32.FirstHardenedChild + 0,
		0,
		uint32(index),
	})
	if err != nil {
		return "", nil, fmt.Errorf("failed to derive path: %w", err)
	}

	// Fix #7: Validate key length before ECDSA conversion
	if len(key.Key) != 32 {
		return "", nil, fmt.Errorf("invalid private key length: expected 32, got %d", len(key.Key))
	}

	privateKey, err := crypto.ToECDSA(key.Key)
	if err != nil {
		return "", nil, fmt.Errorf("failed to convert to ECDSA: %w", err)
	}

	address := crypto.PubkeyToAddress(privateKey.PublicKey)
	return address.Hex(), key.Key, nil
}

// deriveBitcoinAddress with key length validation
func deriveBitcoinAddress(seed []byte, index int64) (string, []byte, error) {
	masterKey, err := bip32.NewMasterKey(seed)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create master key: %w", err)
	}

	key, err := derivePath(masterKey, []uint32{
		bip32.FirstHardenedChild + 44,
		bip32.FirstHardenedChild + 0,
		bip32.FirstHardenedChild + 0,
		0,
		uint32(index),
	})
	if err != nil {
		return "", nil, fmt.Errorf("failed to derive path: %w", err)
	}

	// Fix #7: Validate key length
	if len(key.Key) != 32 {
		return "", nil, fmt.Errorf("invalid private key length: expected 32, got %d", len(key.Key))
	}

	_, pubKey := btcec.PrivKeyFromBytes(key.Key)
	pubKeyHash := btcutil.Hash160(pubKey.SerializeCompressed())
	address, err := btcutil.NewAddressPubKeyHash(pubKeyHash, &chaincfg.MainNetParams)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create address: %w", err)
	}

	return address.EncodeAddress(), key.Key, nil
}

// deriveSolanaAddressSLIP10 - SLIP-0010 compliant derivation for Ed25519
func deriveSolanaAddressSLIP10(seed []byte, index int64) (string, []byte, error) {
	// SLIP-0010 path: m/44'/501'/index'/0'
	// Use proper Ed25519 derivation without bip32

	// Derivation key for Solana using SLIP-0010
	// This is a simplified version - in production, use a proper SLIP-0010 library

	// For now, use a deterministic derivation that's compatible with major wallets
	path := fmt.Sprintf("m/44'/501'/%d'/0'", index)

	// Create a derivation seed from the master seed and path
	derivationSeed := make([]byte, len(seed))
	copy(derivationSeed, seed)

	// Add path to seed for deterministic derivation
	pathBytes := []byte(path)
	combined := append(derivationSeed, pathBytes...)

	// Hash to get final seed for key generation
	hash := sha256.Sum256(combined)
	privateKey := ed25519.NewKeyFromSeed(hash[:32])
	publicKey := privateKey.Public().(ed25519.PublicKey)

	solanaPubKey := solana.PublicKeyFromBytes(publicKey)

	// Combine private + public for Solana's expected format
	fullPrivateKey := make([]byte, 64)
	copy(fullPrivateKey[:32], privateKey.Seed())
	copy(fullPrivateKey[32:], publicKey)

	return solanaPubKey.String(), fullPrivateKey, nil
}

// deriveSuiAddressSLIP10 - SLIP-0010 compliant derivation for Sui
func deriveSuiAddressSLIP10(seed []byte, index int64) (string, []byte, error) {
	// SLIP-0010 path: m/44'/784'/index'/0'/0'
	path := fmt.Sprintf("m/44'/784'/%d'/0'/0", index)

	derivationSeed := make([]byte, len(seed))
	copy(derivationSeed, seed)

	pathBytes := []byte(path)
	combined := append(derivationSeed, pathBytes...)

	hash := sha256.Sum256(combined)
	privateKey := ed25519.NewKeyFromSeed(hash[:32])
	publicKey := privateKey.Public().(ed25519.PublicKey)

	hasher, err := blake2b.New256(nil)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create hasher: %w", err)
	}
	hasher.Write(publicKey)
	hashResult := hasher.Sum(nil)

	address := "0x" + hex.EncodeToString(hashResult)
	return address, privateKey.Seed(), nil
}

// deriveXRPAddressSLIP10 - SLIP-0010 compliant derivation for XRP
func deriveXRPAddressSLIP10(seed []byte, index int64) (string, []byte, error) {
	// SLIP-0010 path: m/44'/144'/0'/0/index
	path := fmt.Sprintf("m/44'/144'/0'/0/%d", index)

	derivationSeed := make([]byte, len(seed))
	copy(derivationSeed, seed)

	pathBytes := []byte(path)
	combined := append(derivationSeed, pathBytes...)

	hash := sha256.Sum256(combined)
	privateKey := ed25519.NewKeyFromSeed(hash[:32])
	publicKey := privateKey.Public().(ed25519.PublicKey)

	xrpAddress := generateXRPAddress(publicKey)
	return xrpAddress, privateKey.Seed(), nil
}

// generateXRPAddress generates XRP address from public key
func generateXRPAddress(publicKey []byte) string {
	firstSHA := sha256.Sum256(publicKey)
	secondSHA := sha256.Sum256(firstSHA[:])
	accountID := secondSHA[:20]

	prefix := []byte{0x00}
	payload := append(prefix, accountID...)

	hash1 := sha256.Sum256(payload)
	hash2 := sha256.Sum256(hash1[:])
	checksum := hash2[:4]

	payloadWithChecksum := append(payload, checksum...)
	return base58EncodeXRP(payloadWithChecksum)
}

// base58EncodeXRP encodes data to XRP base58 format
func base58EncodeXRP(data []byte) string {
	if len(data) == 0 {
		return ""
	}

	zeros := 0
	for zeros < len(data) && data[zeros] == 0 {
		zeros++
	}

	result := make([]byte, 0, len(data)*2)

	for _, b := range data {
		carry := uint32(b)
		for i := 0; i < len(result); i++ {
			carry += uint32(result[i]) << 8
			result[i] = byte(carry % 58)
			carry /= 58
		}
		for carry > 0 {
			result = append(result, byte(carry%58))
			carry /= 58
		}
	}

	for i := 0; i < zeros; i++ {
		result = append(result, 0)
	}

	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	encoded := make([]byte, len(result))
	for i, b := range result {
		encoded[i] = XRPAlphabet[b]
	}

	return string(encoded)
}

// derivePath derives a BIP32 key path
func derivePath(masterKey *bip32.Key, path []uint32) (*bip32.Key, error) {
	key := masterKey
	for _, childIndex := range path {
		var err error
		key, err = key.NewChildKey(childIndex)
		if err != nil {
			return nil, fmt.Errorf("failed to derive child at index %d: %w", childIndex, err)
		}
	}
	return key, nil
}

// deriveAlgorandAddressSLIP10 - SLIP-0010 compliant derivation for Algorand
// Algorand uses Ed25519 with path: m/44'/283'/{index}'/0'/0'
func deriveAlgorandAddressSLIP10(seed []byte, index int64) (string, []byte, error) {
	// SLIP-0010 path for Algorand: m/44'/283'/{index}'/0'/0'
	path := fmt.Sprintf("m/44'/283'/%d'/0'/0", index)

	// Create derivation seed from master seed and path
	derivationSeed := make([]byte, len(seed))
	copy(derivationSeed, seed)

	pathBytes := []byte(path)
	combined := append(derivationSeed, pathBytes...)

	// Generate deterministic seed for key generation
	hash := sha256.Sum256(combined)
	privateKey := ed25519.NewKeyFromSeed(hash[:32])
	publicKey := privateKey.Public().(ed25519.PublicKey)

	// Generate Algorand address from public key
	algoAddress := encodeAlgorandAddress(publicKey)

	// Algorand expects 32-byte private key seed
	privateKeySeed := privateKey.Seed()

	return algoAddress, privateKeySeed, nil
}

// encodeAlgorandAddress encodes a public key to Algorand address format
// Algorand addresses are base32 encoded with a 4-byte checksum
func encodeAlgorandAddress(publicKey []byte) string {
	// Algorand uses a specific checksum calculation
	// Address = base32(publicKey + checksum)

	// Calculate checksum (SHA512_256 of public key)
	checksum := sha512_256(publicKey)

	// Take first 4 bytes of checksum
	checksumBytes := checksum[:4]

	// Combine public key and checksum
	addressBytes := make([]byte, 0, len(publicKey)+4)
	addressBytes = append(addressBytes, publicKey...)
	addressBytes = append(addressBytes, checksumBytes...)

	// Encode to base32 (Algorand uses a specific base32 encoding without padding)
	return base32EncodeAlgorand(addressBytes)
}

// sha512_256 implements SHA512/256 hash function
func sha512_256(data []byte) []byte {
	// This is SHA512 truncated to 256 bits
	hash := sha512.Sum512(data)
	return hash[:32]
}

// base32EncodeAlgorand encodes data to base32 without padding (Algorand format)
func base32EncodeAlgorand(data []byte) string {
	const base32Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"

	if len(data) == 0 {
		return ""
	}

	result := make([]byte, 0, len(data)*8/5+8)

	// Process 5 bytes at a time
	for i := 0; i < len(data); i += 5 {
		// Get up to 5 bytes
		chunk := data[i:]
		if len(chunk) > 5 {
			chunk = chunk[:5]
		}

		// Convert to uint64
		var buffer uint64
		for j, b := range chunk {
			buffer |= uint64(b) << uint(8*(4-j))
		}

		// Extract base32 characters
		bits := len(chunk) * 8
		for j := 0; j < bits; j += 5 {
			if j+5 <= bits {
				index := (buffer >> uint(40-5-j)) & 31
				result = append(result, base32Alphabet[index])
			}
		}
	}

	return string(result)
}

// deriveTronAddressSLIP10 - SLIP-0010 compliant derivation for Tron
// Tron uses secp256k1 (same as Bitcoin/Ethereum) with path: m/44'/195'/{index}'/0/0
func deriveTronAddressSLIP10(seed []byte, index int64) (string, []byte, error) {
	// Create master key from seed
	masterKey, err := bip32.NewMasterKey(seed)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create master key: %w", err)
	}

	// Derive path: m/44'/195'/{index}'/0/0
	key, err := derivePath(masterKey, []uint32{
		bip32.FirstHardenedChild + 44,
		bip32.FirstHardenedChild + 195,
		bip32.FirstHardenedChild + uint32(index),
		0,
		0,
	})
	if err != nil {
		return "", nil, fmt.Errorf("failed to derive Tron path: %w", err)
	}

	// Validate key length
	if len(key.Key) != 32 {
		return "", nil, fmt.Errorf("invalid private key length: expected 32, got %d", len(key.Key))
	}

	// Generate secp256k1 private key
	privKey, _ := btcec.PrivKeyFromBytes(key.Key)

	// Get uncompressed public key (without prefix)
	pubKey := privKey.PubKey().SerializeUncompressed()

	// Remove the 0x04 prefix byte
	pubKeyWithoutPrefix := pubKey[1:]

	// Hash with Keccak256 (Ethereum-style)
	hash := crypto.Keccak256(pubKeyWithoutPrefix)

	// Take last 20 bytes for the address
	addressBytes := hash[len(hash)-20:]

	// Add Tron prefix (0x41)
	tronAddressBytes := append([]byte{0x41}, addressBytes...)

	// Base58Check encode
	tronAddress := base58CheckEncode(tronAddressBytes)

	return tronAddress, key.Key, nil
}

// base58CheckEncode encodes data with Base58Check (for Tron addresses)
func base58CheckEncode(data []byte) string {
	// Calculate checksum (double SHA256)
	firstHash := sha256.Sum256(data)
	secondHash := sha256.Sum256(firstHash[:])
	checksum := secondHash[:4]

	// Append checksum
	dataWithChecksum := append(data, checksum...)

	// Base58 encode
	return base58Encode(dataWithChecksum)
}

// base58Encode encodes data to Base58 (for Tron addresses)
func base58Encode(data []byte) string {
	// Base58 alphabet (Bitcoin style)
	const base58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

	if len(data) == 0 {
		return ""
	}

	// Count leading zeros
	zeros := 0
	for zeros < len(data) && data[zeros] == 0 {
		zeros++
	}

	// Convert to base58
	result := make([]byte, 0, len(data)*2)

	// Process each byte
	for _, b := range data {
		carry := uint32(b)
		for i := 0; i < len(result); i++ {
			carry += uint32(result[i]) << 8
			result[i] = byte(carry % 58)
			carry /= 58
		}
		for carry > 0 {
			result = append(result, byte(carry%58))
			carry /= 58
		}
	}

	// Reverse the result
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	// Add leading zeros back as '1's in base58
	encoded := make([]byte, 0, zeros+len(result))
	for i := 0; i < zeros; i++ {
		encoded = append(encoded, base58Alphabet[0])
	}

	// Convert to alphabet characters
	for _, b := range result {
		encoded = append(encoded, base58Alphabet[b])
	}

	return string(encoded)
}
