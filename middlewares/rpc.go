package middlewares

import (
	"time"

	"cryptohub/core/wallet/watcher"

	"github.com/gagliardetto/solana-go/rpc"
)

func DefaultWatcherConfig() watcher.WatcherConfig {
	return watcher.WatcherConfig{

		// Ethereum (Sepolia)
		EthereumRPCURL:        "https://ethereum-sepolia-rpc.publicnode.com",
		EthereumStartBlock:    5000000,
		EthereumCheckInterval: 10 * time.Second,

		// Solana Devnet
		SolanaRPCURL:        "https://api.devnet.solana.com",
		SolanaCheckInterval: 3 * time.Second,
		SolanaCommitment:    rpc.CommitmentConfirmed,

		// Bitcoin Testnet (Blockstream)
		BitcoinRPCURL:        "https://mempool.space/api",
		BitcoinRPCUser:       "",
		BitcoinRPCPassword:   "",
		BitcoinCheckInterval: 30 * time.Second,

		// BNB Testnet
		BNBRPCURL:     "https://bsc-dataseed2.defibit.io",
		BNBStartBlock: 35000000,

		// Sui Devnet
		SuiRPCURL:        "https://sui.api.metalamp.io",
		SuiCheckInterval: 5 * time.Second,

		// XRP Testnet
		XRPRPCURL:        "https://s2.ripple.com:51234",
		XRPCheckInterval: 10 * time.Second,

		// General settings (important for trading systems)
		MaxConfirmations: 6,
		WorkerCount:      10,
		RetryAttempts:    5,
		RetryDelay:       2 * time.Second,
		RetryBackoff:     2.0,
		CacheTTL:         30 * time.Second,
	}
}
