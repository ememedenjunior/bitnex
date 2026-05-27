package main

import (
	"log"
	"os"
	"os/signal"
	"time"

	"cryptohub/core/wallet/hdwallet"
	"cryptohub/middlewares"
	"cryptohub/pkg/database"
	"cryptohub/routes"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/limiter"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/fiber/v2/middleware/requestid"
	"go.uber.org/zap"
)

func main() {

	// ================= DB =================
	database.ConnectDB()
	database.MigrateUser()
	database.MigrateWallets()
	database.MigrateHotWallets()
	database.MigrateAccounts()
	database.MigrateVerificationTokens()

	// ================= How Wallet creation =================
	walletManager, err := hdwallet.NewWalletManager(database.DB, []byte("wsfouyrkrsqljtie"))
	if err != nil {
		log.Fatal("Failed to create wallet manager:", err)
	}

	// Create hot wallets for each chain
	chains := []string{
		hdwallet.ChainEthereum,
		hdwallet.ChainBNB,
		hdwallet.ChainBitcoin,
		hdwallet.ChainSolana,
		hdwallet.ChainSui,
		hdwallet.ChainXRP,
	}

	for i, chain := range chains {
		if err := walletManager.CreateHotWallet(chain, int64(i)); err != nil {
			log.Printf("Warning: Failed to create hot wallet for %s: %v", chain, err)
		} else {
			log.Printf("✓ Created hot wallet for %s", chain)
		}
	}

	middlewares.InitLog("production")
	defer middlewares.Sync()

	eventBus := middlewares.NewEventBus()

	// ================= APP =================
	app := fiber.New(fiber.Config{
		Prefork:       false,
		CaseSensitive: true,
		StrictRouting: true,
		BodyLimit:     2 * 1024 * 1024, // 2MB
	})

	app.Use(recover.New(recover.Config{
		EnableStackTrace: true,
	}))
	app.Use(requestid.New())
	app.Use(logger.New(logger.Config{
		Format: "${time} | ${status} | ${latency} | ${method} ${path}\n",
	}))
	app.Use(middlewares.SecurityHeaders())

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status": "ok",
		})
	})

	// ================= CORS =================
	app.Use(cors.New(cors.Config{
		AllowOrigins:     "https://crypt-hub-puce.vercel.app",
		AllowMethods:     "GET,POST,PUT,DELETE,OPTIONS",
		AllowHeaders:     "Origin,Content-Type,Accept,Authorization, X-CSRF-Token",
		AllowCredentials: true,
		MaxAge:           86400,
	}))

	// ================= AUTH RATE LIMITER =================
	authLimiter := limiter.New(limiter.Config{
		Max:        4,
		Expiration: time.Minute,

		KeyGenerator: func(c *fiber.Ctx) string {
			return c.IP()

		},

		LimitReached: func(c *fiber.Ctx) error {
			return c.Status(429).JSON(fiber.Map{
				"error": "Too many authentication attempts",
			})
		},
	})

	// ================= ROUTES =================
	routes.SetupRoutes(app, database.DB, []byte("Hello Word12345678967"), authLimiter, eventBus)

	// ================= SERVER =================
	port := "8000"

	middlewares.Log.Info("server starting",
		zap.String("service", "cryptohub"),
		zap.Int("port", 8000),
	)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	go func() {
		if err := app.Listen(":" + port); err != nil {
			log.Fatal(err)
		}
	}()

	<-quit
	log.Println("Shutting down server...")
	_ = app.Shutdown()
}
