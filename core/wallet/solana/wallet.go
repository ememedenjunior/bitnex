package solana

import (
	"cryptohub/pkg/database"
	"cryptohub/pkg/utils"

	"github.com/gagliardetto/solana-go"

	"github.com/google/uuid"
)

func CreateUserSolanaWallet(userID int64, encryptionKey []byte) error {

	wallet := solana.NewWallet()

	address := wallet.PublicKey().String()
	privateKey := wallet.PrivateKey.String()

	encryptedKey, err := utils.Encrypt(privateKey, encryptionKey)
	if err != nil {
		return err
	}

	_, err = database.DB.Exec(`
		INSERT INTO wallets (
			user_uid,
			address,
			asset,
			encrypted_private_key
		)
		VALUES ($1, $2, $3, $4)
	`,
		userID,
		address,
		"SOLANA",
		encryptedKey,
	)

	return err
}

func CreateHotWallet(encryptionKey []byte) error {

	wallet := solana.NewWallet()

	address := wallet.PublicKey().String()
	privateKey := wallet.PrivateKey.String()

	encryptedKey, err := utils.Encrypt(privateKey, encryptionKey)
	if err != nil {
		return err
	}

	_, err = database.DB.Exec(`
		INSERT INTO hot_wallets (
			id,
			address,
			encrypted_private_key,
			is_active
		)
		VALUES ($1, $2, $3, true)
	`,
		uuid.New().String(),
		address,
		encryptedKey,
	)

	return err
}
