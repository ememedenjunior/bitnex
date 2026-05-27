package middlewares

import (
	"fmt"
	"net/smtp"
)

const (
	smtpHost = "smtp.gmail.com"
	smtpPort = "587"
)

// ============================
// ENV LOADING
// ============================

var (
	fromEmail string
	password  string
)

// func init() {
// 	err := godotenv.Load()
// 	if err != nil {
// 		log.Println("⚠️ .env file not found, using system env")
// 	}

// 	fromEmail = os.Getenv("EMAIL_USER")
// 	password = os.Getenv("EMAIL_PASS")

// 	if fromEmail == "" || password == "" {
// 		log.Fatal("❌ Missing SMTP credentials")
// 	}
// }

// ============================
// SEND VERIFICATION EMAIL (TOKEN ONLY)
// ============================

func SendVerificationEmail(toEmail, token string) error {

	fromEmail = "simplyemem10@gmail.com"
	password = "axxs toiw yxgv nykb"

	subject := "Your Verification Code 🚀"

	body := fmt.Sprintf(`
		<html>
		<body style="font-family: Arial, sans-serif; background:#f4f4f4; padding:20px;">
			
			<div style="max-width:600px; margin:auto; background:#ffffff; padding:20px; border-radius:10px;">
				
				<h2>Crypto Platform Verification</h2>

				<p>Use the verification code below to activate your account:</p>

				<div style="
					font-size:24px;
					font-weight:bold;
					letter-spacing:2px;
					background:#f0f0f0;
					padding:15px;
					text-align:center;
					border-radius:8px;
					margin:20px 0;
				">
					%s
				</div>

				<p>This code will expire in a short time for security reasons.</p>

				<hr>
				<p style="font-size:12px;color:gray;">
					If you did not request this, ignore this email.
				</p>

			</div>

		</body>
		</html>
	`, token)

	message := fmt.Sprintf(
		"From: %s\r\n"+
			"To: %s\r\n"+
			"Subject: %s\r\n"+
			"MIME-version: 1.0;\r\n"+
			"Content-Type: text/html; charset=\"UTF-8\";\r\n\r\n"+
			"%s",
		fromEmail,
		toEmail,
		subject,
		body,
	)

	auth := smtp.PlainAuth(
		"",
		fromEmail,
		password,
		smtpHost,
	)

	err := smtp.SendMail(
		smtpHost+":"+smtpPort,
		auth,
		fromEmail,
		[]string{toEmail},
		[]byte(message),
	)

	if err != nil {
		return fmt.Errorf("failed to send email: %w", err)
	}

	return nil
}
