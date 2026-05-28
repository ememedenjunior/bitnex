package handler

import (
	"context"
	"cryptohub/auth"
	"cryptohub/middlewares"
	"database/sql"
	"errors"
	"log"

	"github.com/gofiber/fiber/v2"
)

type AuthHandler struct {
	Service *auth.AuthService
}

func (h *AuthHandler) Register(c *fiber.Ctx) error {

	type req struct {
		Email    string `json:"email"`
		Username string `json:"username"`
		Password string `json:"password"`
	}

	var body req

	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"message": "invalid request",
		})
	}

	token, user_id, err := h.Service.Register(body.Email, body.Username, body.Password)
	if err != nil {
		h.Service.EventBus.Publish(
			middlewares.UserRegistrationFailed,
			middlewares.UserRegistrationFailedPayload{
				Email:   body.Email,
				UserUID: user_id,
				Reason:  err,
			},
		)
		return c.Status(400).JSON(fiber.Map{
			"message": err.Error(),
		})
	}

	e := middlewares.SendVerificationEmail(body.Email, token)
	log.Println(e)
	if e != nil {
		h.Service.EventBus.Publish(
			middlewares.UserRegistrationFailed,
			middlewares.UserRegistrationFailedPayload{
				Email:   body.Email,
				UserUID: user_id,
				Reason:  err,
			},
		)
		return c.Status(500).JSON(fiber.Map{
			"message": e.Error(),
		})
	}

	return c.Status(201).JSON(fiber.Map{
		"message": "user created successfully, check your email for verification",
	})
}

func (h *AuthHandler) VerifyEmail(c *fiber.Ctx) error {

	token := c.Query("token")
	if token == "" {
		return c.Status(400).JSON(fiber.Map{
			"message": "missing token",
		})
	}

	err := h.Service.VerifyUser(token)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"message": "Internal Server Error",
		})
	}

	return c.Status(200).JSON(fiber.Map{
		"message": "email verified successfully",
	})
}

func (h *AuthHandler) ResendVerification(c *fiber.Ctx) error {

	email := c.Query("email")
	if email == "" {
		return c.Status(400).JSON(fiber.Map{
			"message": "missing email",
		})
	}

	token, err := h.Service.ResendVerification(email)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"message": "Internal Server Error",
		})
	}

	e := middlewares.SendVerificationEmail(email, token)
	if e != nil {
		return c.Status(500).JSON(fiber.Map{
			"message": "Internal Server Email Error",
		})
	}

	return c.JSON(fiber.Map{
		"message": "verification email resent",
	})
}

func (h *AuthHandler) Login(c *fiber.Ctx) error {

	type req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}

	var body req

	if err := c.BodyParser(&body); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"message": "invalid request",
		})
	}

	userID, username, verified, err := h.Service.Login(body.Email, body.Password)
	if err != nil {
		return c.Status(401).JSON(fiber.Map{
			"message": err.Error(),
		})
	}

	// generate JWT
	token, err := h.Service.GenerateToken(userID, body.Email, verified)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"message": "failed to generate token",
		})
	}

	// generate CSRF token
	csrfToken, err := auth.GenerateCSRFToken()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"message": "failed csrf token",
		})
	}

	// 🍪 JWT COOKIE (HTTP ONLY)
	c.Cookie(&fiber.Cookie{
		Name:     "auth_token",
		Value:    token,
		HTTPOnly: true,
		Secure:   true,
		SameSite: "Lax",
		Path:     "/",
		MaxAge:   60 * 60 * 24,
	})

	// 🍪 CSRF COOKIE (READABLE BY FRONTEND)
	c.Cookie(&fiber.Cookie{
		Name:     "csrf_token",
		Value:    csrfToken,
		HTTPOnly: false,
		Secure:   true,
		SameSite: "Lax",
		Path:     "/",
		MaxAge:   60 * 60 * 24,
	})

	return c.JSON(fiber.Map{
		"message": "login successful",
		"user":    username,
		"user_id": userID,
	})
}

func (h *AuthHandler) Logout(c *fiber.Ctx) error {

	c.Cookie(&fiber.Cookie{
		Name:     "auth_token",
		Value:    "",
		HTTPOnly: true,
		Secure:   true,
		SameSite: "Lax",
		Path:     "/",
		MaxAge:   -1,
	})

	c.Cookie(&fiber.Cookie{
		Name:     "csrf_token",
		Value:    "",
		HTTPOnly: false,
		Secure:   true,
		SameSite: "Lax",
		Path:     "/",
		MaxAge:   -1,
	})

	return c.JSON(fiber.Map{
		"message": "logged out",
	})
}

func (h *AuthHandler) AuthMiddleware(c *fiber.Ctx) error {

	// 1. check JWT cookie
	token := c.Cookies("auth_token")
	if token == "" {
		return c.Status(401).JSON(fiber.Map{
			"message": "unauthorized",
		})
	}

	claims, err := h.Service.ValidateToken(token)
	if err != nil {
		return c.Status(401).JSON(fiber.Map{
			"message": "invalid token",
		})
	}

	// 2. CSRF protection (only for unsafe methods)
	if c.Method() != "GET" {

		csrfCookie := c.Cookies("csrf_token")
		csrfHeader := c.Get("X-CSRF-Token")

		if csrfCookie == "" || csrfHeader == "" || csrfCookie != csrfHeader {
			return c.Status(403).JSON(fiber.Map{
				"message": "CSRF validation failed",
			})
		}
	}

	// 3. attach user context
	c.Locals("userID", claims.UserID)
	c.Locals("email", claims.Email)

	return c.Next()
}

func (h *AuthHandler) ForgotPassword(c *fiber.Ctx) error {

	var req struct {
		Email string `json:"email"`
	}

	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"message": "invalid request",
		})
	}

	token, err := h.Service.ForgotPassword(req.Email)
	if err != nil {
		return c.Status(401).JSON(fiber.Map{
			"message": err.Error(),
		})
	}

	e := middlewares.SendVerificationEmail(req.Email, token)
	if e != nil {
		return c.Status(500).JSON(fiber.Map{
			"message": "Internal Server Email Error",
		})
	}

	return c.JSON(fiber.Map{
		"message": "password reset token sent to email",
	})
}

func (h *AuthHandler) ResetPassword(c *fiber.Ctx) error {

	var req struct {
		Token       string `json:"token"`
		NewPassword string `json:"newPassword"`
	}

	if err := c.BodyParser(&req); err != nil {

		return c.Status(400).JSON(fiber.Map{
			"message": "invalid request",
		})
	}

	err := h.Service.ResetPassword(
		req.Token,
		req.NewPassword,
	)

	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"message": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"message": "password reset successful",
	})
}

func (h *AuthHandler) Profile(c *fiber.Ctx) error {
	userID := c.Locals("userID")
	ctx := context.Background()

	// 1. Get user
	var user struct {
		Useruid           string `json:"user_uid"`
		Email             string `json:"email"`
		UserName          string `json:"username"`
		IsVerified        bool   `json:"is_verified"`
		Daily_Open_Equity int64  `json:"daily_open_equity"`
	}

	var err error

	err = h.Service.DB.QueryRowContext(ctx, `
		SELECT user_uid, email, username, is_verified
		FROM users
		WHERE user_uid = $1
	`, userID).Scan(&user.Useruid, &user.Email, &user.UserName, &user.IsVerified)

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"message": "failed to fetch user",
		})
	}

	// 2. Get account info
	type Account struct {
		Asset   string `json:"asset"`
		Balance string `json:"balance"`
	}

	// 2. Get account info
	rows, err := h.Service.DB.QueryContext(ctx, `
		SELECT asset, balance
		FROM accounts
		WHERE user_uid = $1
		`, user.Useruid)

	if err != nil {
		return c.Status(500).JSON(fiber.Map{
			"message": "internal server error",
		})
	}
	defer rows.Close()

	accounts := []Account{}

	for rows.Next() {
		var account Account

		err := rows.Scan(&account.Asset, &account.Balance)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"message": "failed to scan account",
			})
		}

		accounts = append(accounts, account)
	}

	if len(accounts) == 0 {
		return c.Status(404).JSON(fiber.Map{
			"message": "accounts not found",
		})
	}

	// 3. Get wallets
	type wallet struct {
		Address string `json:"address"`
		Chain   string `json:"chain"`
	}

	rows, err = h.Service.DB.QueryContext(ctx, `
			SELECT address, chain
			FROM wallets
			WHERE user_uid = $1
		`, user.Useruid)

	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return c.Status(404).JSON(fiber.Map{
				"message": "account not found",
			})
		default:
			return c.Status(500).JSON(fiber.Map{
				"message": "internal server error",
			})
		}
	}
	defer rows.Close()

	var wallets []wallet

	for rows.Next() {
		var w wallet
		err := rows.Scan(&w.Address, &w.Chain)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"message": "failed to fetch wallets",
			})
		}
		wallets = append(wallets, w)
	}

	// 4. Final response
	return c.JSON(fiber.Map{
		"user":    user,
		"account": accounts,
		"wallets": wallets,
	})
}
