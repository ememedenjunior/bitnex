package jobs

import (
	"cryptohub/auth"
	"cryptohub/middlewares"
	"fmt"
	"log"
	"time"

	"github.com/robfig/cron/v3"
)

func StartCron(auth *auth.AuthService) {
	fmt.Println("Cron jobs started")
	c := cron.New()

	// Run every 10 minutes
	c.AddFunc("@every 10m", func() {
		err := auth.CleanupExpiredTokens()
		if err != nil {
			log.Println("cleanup error:", err)
			middlewares.Error("cleanup error:", err)
		} else {
			middlewares.Info("expired tokens cleaned at")
			log.Println("expired tokens cleaned at", time.Now())
		}
	})

	c.Start()
}
