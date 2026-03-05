package exchanges

type PriceData struct {
	Symbol    string
	Source    string
	Price     float64
	Timestamp int64
}

type OrderbookData struct {
	Symbol    string
	Source    string
	BestBid   float64
	BestAsk   float64
	Timestamp int64
}

type TradeData struct {
	Symbol    string
	Source    string
	Price     float64
	Quantity  string
	Side      string // "buy" or "sell" (normalized)
	Timestamp int64
}

type FundingData struct {
	Symbol          string  `json:"symbol"`
	Source          string  `json:"source"`
	Rate            float64 `json:"rate"` // Backward-compatible display rate (decimal form).
	LastRate        float64 `json:"last_rate"`
	NextRate        float64 `json:"next_rate"`
	NextFundingTime int64   `json:"next_funding_time"`
	MarkPrice       float64 `json:"mark_price"`
	IndexPrice      float64 `json:"index_price"`
	PremiumPct      float64 `json:"premium_pct"` // ((mark-index)/index)*100
	Timestamp       int64   `json:"timestamp"`
}
