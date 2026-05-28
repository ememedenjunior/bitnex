package jobs

import (
	"cryptohub/auth"
	"cryptohub/middlewares"
	service "cryptohub/services"
	"fmt"
	"log"
	"time"

	"github.com/robfig/cron/v3"
)

func StartCron(authService *auth.AuthService) {

	equityService := service.EquityService{
		DB: authService.DB,
	}

	fmt.Println("Cron jobs started")

	c := cron.New(
		cron.WithLocation(time.UTC), // IMPORTANT: force UTC
	)

	// 1. Cleanup expired tokens (existing job)
	c.AddFunc("@every 10m", func() {
		err := authService.CleanupExpiredTokens()
		if err != nil {
			log.Println("cleanup error:", err)
			middlewares.Error("cleanup error:", err)
		} else {
			log.Println("expired tokens cleaned at", time.Now().UTC())
			middlewares.Info("expired tokens cleaned")
		}
	})

	// 2. DAILY OPEN EQUITY SNAPSHOT (NEW)
	c.AddFunc("0 0 * * *", func() {
		err := equityService.CalculateDailyOpenEquity()
		if err != nil {
			log.Println("❌ equity snapshot error:", err)
			middlewares.Error("equity snapshot error:", err)
		} else {
			log.Println("✅ daily open equity snapshot completed at", time.Now().UTC())
		}
	})

	c.Start()
}
