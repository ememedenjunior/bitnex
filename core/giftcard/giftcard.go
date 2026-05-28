package giftcard

import (
	"database/sql"
	"errors"
	"time"
)

/*
============================
 MODELS
============================
*/

type GiftCard struct {
	ID       uint
	Name     string
	Brand    string
	Country  string
	BuyRate  float64
	SellRate float64
	Currency string
	Active   bool
}

type GiftCardTrade struct {
	ID        uint
	UserID    uint
	Type      string // BUY or SELL
	CardType  string
	Amount    float64
	Rate      float64
	Fee       float64
	Payout    float64
	Status    string // pending, processing, completed, rejected
	Code      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

/*
============================
 SERVICE
============================
*/

type GiftCardService struct {
	DB *sql.DB
}

/*
============================
 1. LIST GIFT CARDS
============================
*/

func (s *GiftCardService) GetGiftCards() ([]GiftCard, error) {
	rows, err := s.DB.Query(`
		SELECT id, name, brand, country, buy_rate, sell_rate, currency, active
		FROM gift_cards
		WHERE active = 1
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cards []GiftCard

	for rows.Next() {
		var c GiftCard

		err := rows.Scan(
			&c.ID,
			&c.Name,
			&c.Brand,
			&c.Country,
			&c.BuyRate,
			&c.SellRate,
			&c.Currency,
			&c.Active,
		)
		if err != nil {
			return nil, err
		}

		cards = append(cards, c)
	}

	return cards, nil
}

/*
============================
 2. CREATE TRADE
============================
*/

func (s *GiftCardService) CreateTrade(trade *GiftCardTrade) error {

	if trade.Amount <= 0 {
		return errors.New("invalid amount")
	}

	trade.Status = "pending"
	trade.CreatedAt = time.Now().UTC()

	_, err := s.DB.Exec(`
		INSERT INTO giftcard_trades
		(user_id, type, card_type, amount, rate, fee, payout, status, code, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		trade.UserID,
		trade.Type,
		trade.CardType,
		trade.Amount,
		trade.Rate,
		trade.Fee,
		trade.Payout,
		trade.Status,
		trade.Code,
		trade.CreatedAt,
	)

	return err
}

/*
============================
 3. CALCULATE PAYOUT
============================
*/

func (s *GiftCardService) CalculatePayout(amount float64, rate float64, feePercent float64) (float64, float64) {

	gross := amount * rate
	fee := gross * feePercent / 100
	payout := gross - fee

	return payout, fee
}

/*
============================
 4. VALIDATE GIFT CARD
============================
*/

func (s *GiftCardService) ValidateGiftCard(code string) (bool, error) {

	if len(code) < 5 {
		return false, errors.New("invalid gift card code")
	}

	var exists int

	err := s.DB.QueryRow(`
		SELECT COUNT(*)
		FROM giftcard_trades
		WHERE code = ?
	`, code).Scan(&exists)

	if err != nil {
		return false, err
	}

	if exists > 0 {
		return false, errors.New("gift card already used")
	}

	return true, nil
}

/*
============================
 5. PROCESS TRADE
============================
*/

func (s *GiftCardService) ProcessTrade(tradeID uint) error {

	var trade GiftCardTrade

	err := s.DB.QueryRow(`
		SELECT id, user_id, type, card_type, amount, rate, fee, payout, status
		FROM giftcard_trades
		WHERE id = ?
	`, tradeID).Scan(
		&trade.ID,
		&trade.UserID,
		&trade.Type,
		&trade.CardType,
		&trade.Amount,
		&trade.Rate,
		&trade.Fee,
		&trade.Payout,
		&trade.Status,
	)

	if err != nil {
		return err
	}

	if trade.Status != "pending" {
		return errors.New("trade already processed")
	}

	_, err = s.DB.Exec(`
		UPDATE giftcard_trades
		SET status = 'processing', updated_at = ?
		WHERE id = ?
	`, time.Now().UTC(), tradeID)

	return err
}

/*
============================
 6. APPROVE TRADE
============================
*/

func (s *GiftCardService) ApproveTrade(tradeID uint) error {

	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}

	var trade GiftCardTrade

	err = tx.QueryRow(`
		SELECT id, user_id, payout
		FROM giftcard_trades
		WHERE id = ?
	`, tradeID).Scan(&trade.ID, &trade.UserID, &trade.Payout)

	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
		UPDATE users
		SET balance = balance + ?
		WHERE id = ?
	`, trade.Payout, trade.UserID)

	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(`
		UPDATE giftcard_trades
		SET status = 'completed', updated_at = ?
		WHERE id = ?
	`, time.Now().UTC(), tradeID)

	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

/*
============================
 7. REJECT TRADE
============================
*/

func (s *GiftCardService) RejectTrade(tradeID uint) error {

	_, err := s.DB.Exec(`
		UPDATE giftcard_trades
		SET status = 'rejected', updated_at = ?
		WHERE id = ?
	`, time.Now().UTC(), tradeID)

	return err
}

/*
============================
 8. GET USER TRADES
============================
*/

func (s *GiftCardService) GetUserTrades(userID uint) ([]GiftCardTrade, error) {

	rows, err := s.DB.Query(`
		SELECT id, user_id, type, card_type, amount, rate, fee, payout, status, code, created_at
		FROM giftcard_trades
		WHERE user_id = ?
		ORDER BY created_at DESC
	`, userID)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var trades []GiftCardTrade

	for rows.Next() {
		var t GiftCardTrade

		err := rows.Scan(
			&t.ID,
			&t.UserID,
			&t.Type,
			&t.CardType,
			&t.Amount,
			&t.Rate,
			&t.Fee,
			&t.Payout,
			&t.Status,
			&t.Code,
			&t.CreatedAt,
		)

		if err != nil {
			return nil, err
		}

		trades = append(trades, t)
	}

	return trades, nil
}

/*
============================
 9. GET RATES (STATIC LOGIC)
============================
*/

func (s *GiftCardService) GetRates() ([]GiftCard, error) {
	return s.GetGiftCards()
}

/*
============================
 10. ANALYTICS (BASIC)
============================
*/

func (s *GiftCardService) GetTotalVolume() (float64, error) {

	var total float64

	err := s.DB.QueryRow(`
		SELECT IFNULL(SUM(amount * rate), 0)
		FROM giftcard_trades
		WHERE status = 'completed'
	`).Scan(&total)

	return total, err
}
