package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

type EquityService struct {
	DB *sql.DB
}

/*
	-------------------------------
	  MAIN JOB FUNCTION

--------------------------------
*/
func (e *EquityService) CalculateDailyOpenEquity() error {

	// 1. get all accounts
	accounts, err := e.GetAllAccounts()
	if err != nil {
		return err
	}

	// 2. get all prices
	prices, err := e.GetAllPrices()
	if err != nil {
		return err
	}

	priceMap := make(map[string]float64)
	for _, p := range prices {
		priceMap[p.Asset] = p.Price
	}

	// 3. GROUP accounts by user (IMPORTANT FIX)
	userAccounts := make(map[uint][]Account)

	for _, acc := range accounts {
		userAccounts[acc.UserID] = append(userAccounts[acc.UserID], acc)
	}

	// 4. calculate PER USER
	for userID, accs := range userAccounts {

		var totalEquity float64 = 0

		for _, acc := range accs {

			price, ok := priceMap[acc.Asset]
			if !ok {
				continue // skip unknown asset
			}

			totalEquity += price * acc.Amount
		}

		// 5. save once per user
		err := e.SaveDailyOpenEquity(userID, totalEquity)
		if err != nil {
			return err
		}
	}

	return nil
}

/*
	-------------------------------
	  1. GET ALL ACCOUNTS

--------------------------------
*/
type Account struct {
	UserID uint
	Asset  string
	Amount float64
}

func (e *EquityService) GetAllAccounts() ([]Account, error) {

	rows, err := e.DB.Query(`
		SELECT user_id, asset, balance
		FROM accounts
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accounts []Account

	for rows.Next() {

		var acc Account

		err := rows.Scan(&acc.UserID, &acc.Asset, &acc.Amount)
		if err != nil {
			return nil, err
		}

		accounts = append(accounts, acc)
	}

	return accounts, nil
}

/*
	-------------------------------
	  2. GET ALL PRICES

--------------------------------
*/
type Price struct {
	Asset string
	Price float64
}

func (e *EquityService) GetAllPrices() ([]Price, error) {

	// CoinGecko IDs (IMPORTANT: must match API naming)
	ids := []string{
		"bitcoin",
		"ethereum",
		"binancecoin",
		"solana",
		"sui",
		"ripple",
		"algorand",
		"tron",
	}

	url := fmt.Sprintf(
		"https://api.coingecko.com/api/v3/simple/price?ids=%s&vs_currencies=usd",
		strings.Join(ids, ","),
	)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var data map[string]map[string]float64

	err = json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return nil, err
	}

	// map CoinGecko IDs → your internal symbols
	idToSymbol := map[string]string{
		"bitcoin":     "BITCOIN",
		"ethereum":    "ETHEREUM",
		"binancecoin": "BNB",
		"solana":      "SOLANA",
		"sui":         "SUI",
		"ripple":      "XRP",
		"algorand":    "ALGORAND",
		"tron":        "TRON",
	}

	var prices []Price

	for id, p := range data {

		symbol, ok := idToSymbol[id]
		if !ok {
			continue
		}

		prices = append(prices, Price{
			Asset: symbol,
			Price: p["usd"],
		})
	}

	return prices, nil
}

/*
	-------------------------------
	  3. SAVE DAILY OPEN EQUITY

--------------------------------
*/
func (e *EquityService) SaveDailyOpenEquity(userID uint, equity float64) error {

	_, err := e.DB.Exec(`
		UPDATE users
		SET daily_open_equity = ?
		WHERE user_uid = ?
	`, equity, userID)

	return err
}
