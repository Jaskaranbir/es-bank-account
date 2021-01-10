package model

import (
	"time"
)

// Transaction represents a single account-Transaction.
type Transaction struct {
	ID         string    `json:"id"`
	CustomerID string    `json:"customer_id"`
	LoadAmount float64   `json:"load_amount"`
	Time       time.Time `json:"time"`
}
