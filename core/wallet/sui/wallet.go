package sui

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/tyler-smith/go-bip39"
)

func main() {
	// 1. Mnemonic
	entropy, _ := bip39.NewEntropy(128)
	mnemonic, _ := bip39.NewMnemonic(entropy)

	// 2. Seed
	seed := bip39.NewSeed(mnemonic, "")

	// 3. Keypair
	privateKey := ed25519.NewKeyFromSeed(seed[:32])
	publicKey := privateKey.Public().(ed25519.PublicKey)

	// 4. Address
	hash := sha256.Sum256(publicKey)
	address := "0x" + hex.EncodeToString(hash[:20])

	fmt.Println("Mnemonic:", mnemonic)
	fmt.Println("Address:", address)
}
