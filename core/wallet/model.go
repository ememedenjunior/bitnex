package wallet

import "time"

type Wallet struct {
	ID                  string
	UserID              string
	Address             string
	EncryptedPrivateKey string
	CreatedAt           time.Time
}

type HotWallet struct {
	ID                  string
	Address             string
	EncryptedPrivateKey string
	IsActive            bool
	CreatedAt           time.Time
}
