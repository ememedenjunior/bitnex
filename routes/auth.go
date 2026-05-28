package routes

import (
	"cryptohub/auth"
	"cryptohub/handler"
	"cryptohub/jobs"
	"cryptohub/middlewares"
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
	protected.Get("/me", authHandler.AuthMiddleware, authHandler.Profile)

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
