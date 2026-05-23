package auth

import (
	"crypto/rand"
	"cryptohub/core/wallet/solana"
	"cryptohub/pkg/utils"
	"database/sql"
	"encoding/base64"
	"errors"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
)

type Claims struct {
	UserID   string `json:"user_id"`
	Email    string `json:"email"`
	Verified bool   `json:"verified"`
	jwt.RegisteredClaims
}

type AuthService struct {
	DB        *sql.DB
	JWTSecret []byte
}

func (s *AuthService) Register(email, username, password string) (string, error) {

	if email == "" || username == "" || password == "" {
		return "", errors.New("missing required fields")
	}

	if len(password) < 8 {
		return "", errors.New("password too weak")
	}

	var exists bool
	err := s.DB.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM users WHERE email=$1 OR username=$2
		)
	`, email, username).Scan(&exists)

	if err != nil {
		return "", err
	}

	if exists {
		return "", errors.New("user already exists")
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), 14)
	if err != nil {
		return "", err
	}

	userID, _ := utils.GenerateSecure10DigitNumber()

	_, err = s.DB.Exec(`
		INSERT INTO users (user_uid, email, username, password_hash, is_verified, created_at, updated_at)
		VALUES ($1,$2,$3,$4,false,NOW(),NOW())
	`, userID, email, username, string(hash))

	if err != nil {
		return "", err
	}

	// create default accounts
	assets := []string{"SOLANA"}

	for _, asset := range assets {
		_, _ = s.DB.Exec(`
			INSERT INTO accounts (id, user_uid, asset, balance, created_at)
			VALUES ($1,$2,$3,0,NOW())
		`, uuid.New(), userID, asset)
	}

	e := solana.CreateUserSolanaWallet(userID, []byte("wsfouyrkrsqljtie"))
	if e != nil {
		return "", e
	}

	// verification token
	token, _ := utils.GenerateSecure6DigitCode()

	_, err = s.DB.Exec(`
		INSERT INTO verification_tokens (email, token, expires_at, used, created_at)
		VALUES ($1,$2,$3,false,NOW())
	`, email, token, time.Now().Add(24*time.Hour))

	if err != nil {
		return "", err
	}

	return token, nil
}

func (s *AuthService) VerifyUser(token string) error {

	var (
		id        string
		email     string
		expiresAt time.Time
		used      bool
	)

	err := s.DB.QueryRow(`
		SELECT id, email, expires_at, used
		FROM verification_tokens
		WHERE token = $1
	`, token).Scan(&id, &email, &expiresAt, &used)

	if err != nil {
		if err == sql.ErrNoRows {
			return errors.New("invalid token")
		}
		return err
	}

	if used {
		return errors.New("token already used")
	}

	if time.Now().After(expiresAt) {
		return errors.New("token expired")
	}

	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
		UPDATE users SET is_verified=true, updated_at=NOW()
		WHERE email=$1
	`, email)

	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
		UPDATE verification_tokens SET used=true WHERE id=$1
	`, id)

	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (s *AuthService) ResendVerification(email string) (string, error) {

	token := uuid.New().String()

	_, err := s.DB.Exec(`
		INSERT INTO verification_tokens (id, email, token, expires_at, used, created_at)
		VALUES ($1,$2,$3,$4,false,NOW())
	`, uuid.New().String(), email, token, time.Now().Add(24*time.Hour))

	if err != nil {
		return "", err
	}

	return token, nil
}

func (s *AuthService) CleanupExpiredTokens() error {
	_, err := s.DB.Exec(`
		DELETE FROM verification_tokens
		WHERE expires_at < NOW() OR used = true
	`)
	return err
}

func (s *AuthService) Login(email, password string) (userID, username string, verified bool, err error) {

	var passwordHash string

	err = s.DB.QueryRow(`
		SELECT user_uid, username, password_hash, is_verified
		FROM users WHERE email=$1
	`, email).Scan(&userID, &username, &passwordHash, &verified)

	if err != nil {
		if err == sql.ErrNoRows {
			return "", "", false, errors.New("invalid credentials")
		}
		return "", "", false, err
	}

	if bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(password)) != nil {
		return "", "", false, errors.New("invalid credentials")
	}

	if !verified {
		return "", "", false, errors.New("email not verified")
	}

	return userID, username, verified, nil
}

func (s *AuthService) GenerateToken(userID, email string, verified bool) (string, error) {

	claims := Claims{
		UserID:   userID,
		Email:    email,
		Verified: verified,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(s.JWTSecret)
}

func (s *AuthService) ValidateToken(tokenStr string) (*Claims, error) {

	token, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(token *jwt.Token) (any, error) {
		return s.JWTSecret, nil
	})

	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid token")
	}

	return claims, nil
}

func GenerateCSRFToken() (string, error) {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}
