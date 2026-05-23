package main

import (
	"log"
	"os"
	"time"

	"cryptohub/pkg/database"
	"cryptohub/routes"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/limiter"
)

func main() {

	// ================= DB =================
	database.ConnectDB()
	database.MigrateUser()
	database.MigrateWallets()
	database.MigrateAccounts()
	database.MigrateSessions()
	database.MigrateVerificationTokens()

	// ================= APP =================
	app := fiber.New(fiber.Config{
		Prefork:       false,
		CaseSensitive: true,
		StrictRouting: true,
	})

	// ================= CORS =================
	app.Use(cors.New(cors.Config{
		AllowOrigins:     "http://localhost:5173",
		AllowMethods:     "GET,POST,PUT,DELETE,OPTIONS",
		AllowHeaders:     "Origin,Content-Type,Accept,Authorization",
		AllowCredentials: true,
		MaxAge:           86400,
	}))

	// ================= GLOBAL RATE LIMITER =================
	app.Use(limiter.New(limiter.Config{
		Max:        200,             // general API limit
		Expiration: 1 * time.Minute, // per minute
		KeyGenerator: func(c *fiber.Ctx) string {
			return c.IP()
		},
		LimitReached: func(c *fiber.Ctx) error {
			return c.Status(429).JSON(fiber.Map{
				"error": "Too many requests",
			})
		},
	}))

	// ================= ROUTES =================
	routes.SetupRoutes(app, database.DB, []byte("Hello Word12345678967"))

	// ================= SERVER =================
	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}

	log.Fatal(app.Listen(":" + port))
}
