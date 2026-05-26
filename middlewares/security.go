package middlewares

import "github.com/gofiber/fiber/v2"

func SecurityHeaders() fiber.Handler {
	return func(c *fiber.Ctx) error {

		c.Set("X-Frame-Options", "DENY")
		c.Set("X-Content-Type-Options", "nosniff")
		c.Set("X-XSS-Protection", "1; mode=block")
		c.Set("Referrer-Policy", "strict-origin-when-cross-origin")
		c.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")

		c.Set(
			"Content-Security-Policy",
			"default-src 'self'; "+
				"script-src 'self'; "+
				"style-src 'self' 'unsafe-inline'; "+
				"img-src 'self' data: https:; "+
				"connect-src 'self'; "+
				"frame-ancestors 'none';",
		)

		return c.Next()
	}
}

func CSRFProtection() fiber.Handler {

	return func(c *fiber.Ctx) error {

		switch c.Method() {
		case fiber.MethodGet,
			fiber.MethodHead,
			fiber.MethodOptions:
			return c.Next()
		}

		cookieToken := c.Cookies("csrf_token")
		headerToken := c.Get("X-CSRF-Token")

		if cookieToken == "" {
			return c.Status(403).JSON(fiber.Map{
				"error": "missing csrf token",
			})
		}

		if headerToken == "" {
			return c.Status(403).JSON(fiber.Map{
				"error": "missing csrf header",
			})
		}

		if cookieToken != headerToken {
			return c.Status(403).JSON(fiber.Map{
				"error": "invalid csrf token",
			})
		}

		return c.Next()
	}
}
