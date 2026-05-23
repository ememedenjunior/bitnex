package auth

import "time"

// ============================
// 🧠 GO MODELS FOR USERS
// ============================

type User struct {
	ID           string
	Email        string
	Username     string
	PasswordHash string
	Role         string
	IsVerified   bool
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type Session struct {
	ID           string
	UserID       string
	RefreshToken string
	ExpiresAt    time.Time
	CreatedAt    time.Time
}

type VerificationToken struct {
	ID        string
	UserID    string
	Token     string
	ExpiresAt time.Time
	Used      bool
	CreatedAt time.Time
}

type Users struct {
	UserUID    string `json:"user_uid"`
	Email      string `json:"email"`
	UserName   string `json:"username"`
	IsVerified bool   `json:"is_verified"`
}

type Account struct {
	Asset   string `json:"asset"`
	Balance string `json:"balance"`
}

type Wallet struct {
	Address string `json:"address"`
}

type MeResponse struct {
	User    Users    `json:"user"`
	Account Account  `json:"account"`
	Wallets []Wallet `json:"wallets"`
}
