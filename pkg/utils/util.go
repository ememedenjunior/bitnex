package utils

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"math/big"
)

func Encrypt(text string, key []byte) (string, error) {

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	ciphertext := make([]byte, aes.BlockSize+len(text))

	iv := ciphertext[:aes.BlockSize]

	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return "", err
	}

	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(ciphertext[aes.BlockSize:], []byte(text))

	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

func Decrypt(encryptedText string, key []byte) (string, error) {

	// decode base64
	ciphertext, err := base64.StdEncoding.DecodeString(encryptedText)
	if err != nil {
		return "", err
	}

	// create cipher block
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	// validate ciphertext length
	if len(ciphertext) < aes.BlockSize {
		return "", fmt.Errorf("ciphertext too short")
	}

	// extract IV
	iv := ciphertext[:aes.BlockSize]

	// extract encrypted content
	ciphertext = ciphertext[aes.BlockSize:]

	// create decrypt stream
	stream := cipher.NewCFBDecrypter(block, iv)

	// decrypt in-place
	stream.XORKeyStream(ciphertext, ciphertext)

	return string(ciphertext), nil
}

// GenerateSecure10DigitNumber generates a cryptographically secure 10-digit number
func GenerateSecure10DigitNumber() (int64, error) {

	min := int64(1000000000)
	max := int64(9999999999)
	rangeSize := big.NewInt(max - min + 1)

	n, err := rand.Int(rand.Reader, rangeSize)
	if err != nil {
		return 0, err
	}

	return min + n.Int64(), nil
}

func GenerateSecure6DigitCode() (string, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(900000))
	if err != nil {
		return "", err
	}

	code := n.Int64() + 100000
	return fmt.Sprintf("%06d", code), nil
}
