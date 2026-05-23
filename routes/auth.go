package routes

import (
	"cryptohub/auth"
	"cryptohub/handler"
	"cryptohub/jobs"
	"fmt"

	"database/sql"

	"github.com/gofiber/fiber/v2"
)

// ============================
// SETUP ALL ROUTES
// ============================

func SetupRoutes(app *fiber.App, db *sql.DB, jwtSecret []byte) {

	authService := &auth.AuthService{
		DB:        db,
		JWTSecret: jwtSecret,
	}

	authHandler := handler.AuthHandler{
		Service: authService,
	}

	api := app.Group("/api/auth")

	// ============================
	// AUTH ROUTES
	// ============================
	api.Post("/register", authHandler.Register)
	api.Post("/login", authHandler.Login)
	api.Get("/verify", authHandler.VerifyEmail)
	api.Post("/resend", authHandler.ResendVerification)
	api.Post("/logout", authHandler.Logout)

	// protected
	api.Get("/me", authHandler.AuthMiddleware, func(c *fiber.Ctx) error {
		userID := c.Locals("userID")

		// 1. Get user
		var user struct {
			Useruid    string `json:"user_uid"`
			Email      string `json:"email"`
			UserName   string `json:"username"`
			IsVerified bool   `json:"is_verified"`
		}

		err := db.QueryRow(`
		SELECT user_uid, email, username, is_verified
		FROM users
		WHERE user_uid = $1
	`, userID).Scan(&user.Useruid, &user.Email, &user.UserName, &user.IsVerified)

		if err != nil {
			fmt.Println(err)
			return c.Status(500).JSON(fiber.Map{
				"error": "failed to fetch user",
			})
		}

		// 2. Get account info
		var account struct {
			Asset   string `json:"asset"`
			Balance string `json:"balance"`
		}

		err = db.QueryRow(`
		SELECT  asset, balance
		FROM accounts
		WHERE user_uid = $1
	`, userID).Scan(&account.Asset, &account.Balance)

		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "failed to fetch account",
			})
		}

		// 3. Get wallets
		type Wallet struct {
			Address string `json:"address"`
		}

		rows, err := db.Query(`
		SELECT address
		FROM wallets
		WHERE user_uid = $1
	`, userID)

		if err != nil {
			return c.Status(500).JSON(fiber.Map{
				"error": "failed to fetch wallets",
			})
		}
		defer rows.Close()

		var wallets []Wallet

		for rows.Next() {
			var w Wallet
			rows.Scan(&w.Address)
			wallets = append(wallets, w)
		}

		// 4. Final response
		return c.JSON(fiber.Map{
			"user":    user,
			"account": account,
			"wallets": wallets,
		})
	})

	jobs.StartCron(authService)
}
