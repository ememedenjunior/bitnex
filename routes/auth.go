package routes

import (
	"context"
	"cryptohub/auth"
	"cryptohub/handler"
	"cryptohub/jobs"
	"cryptohub/middlewares"
	"errors"
	"log"

	"database/sql"

	"github.com/gofiber/fiber/v2"
)

// ============================
// SETUP ALL ROUTES
// ============================
var AuthService *auth.AuthService

func SetupRoutes(app *fiber.App, db *sql.DB, jwtSecret []byte, authLimiter fiber.Handler, eventBus *middlewares.EventBus) {

	authService := &auth.AuthService{
		DB:        db,
		JWTSecret: jwtSecret,
		EventBus:  eventBus,
	}

	authHandler := handler.AuthHandler{
		Service: authService,
	}

	auth := app.Group("/api/auth")
	protected := app.Group("/api/v1")

	// protected.Use(middlewares.CSRFProtection())

	// ============================
	// AUTH ROUTES
	// ============================
	auth.Post("/register", authLimiter, authHandler.Register)
	auth.Post("/login", authLimiter, authHandler.Login)
	auth.Get("/verify", authLimiter, authHandler.VerifyEmail)
	auth.Post("/resend", authLimiter, authHandler.ResendVerification)
	auth.Post("/logout", authLimiter, authHandler.Logout)
	auth.Post("/forgot-password", authLimiter, authHandler.ForgotPassword)
	auth.Post("/reset-password", authLimiter, authHandler.ResetPassword)

	// protected
	protected.Get("/me", func(c *fiber.Ctx) error {
		userID := 8223083199 // c.Locals("userID")
		ctx := context.Background()

		// 1. Get user
		var user struct {
			Useruid    string `json:"user_uid"`
			Email      string `json:"email"`
			UserName   string `json:"username"`
			IsVerified bool   `json:"is_verified"`
		}

		var err error

		err = db.QueryRowContext(ctx, `
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
		rows, err := db.QueryContext(ctx, `
		SELECT asset, balance
		FROM accounts
		WHERE user_uid = $1
		`, userID)

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

		rows, err = db.QueryContext(ctx, `
			SELECT address, chain
			FROM wallets
			WHERE user_uid = $1
		`, userID)

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
	})

	jobs.StartCron(authService)
	eventBus.Subscribe(
		middlewares.UserRegistrationFailed,
		func(data any) {
			payload := data.(middlewares.UserRegistrationFailedPayload)

			err := authService.CleanupUnverifiedUserIfError(
				payload.Email,
				payload.UserUID,
			)

			if err != nil {
				log.Printf(
					"cleanup failed for user %d: %v",
					payload.UserUID,
					err,
				)
			}
		},
	)
}
