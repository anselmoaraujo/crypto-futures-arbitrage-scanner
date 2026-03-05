package exchanges

import (
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type coinbaseTickerMessage struct {
	Type      string `json:"type"`
	ProductID string `json:"product_id"`
	Price     string `json:"price"`
	BestBid   string `json:"best_bid"`
	BestAsk   string `json:"best_ask"`
	LastSize  string `json:"last_size"`
	Side      string `json:"side"`
	Time      string `json:"time"`
}

func ConnectCoinbaseSpot(symbols []string, priceChan chan<- PriceData, orderbookChan chan<- OrderbookData, tradeChan chan<- TradeData) {
	wsURL := "wss://ws-feed.exchange.coinbase.com"
	products := make([]string, 0, len(symbols))

	for _, symbol := range symbols {
		if productID := convertToCoinbaseProduct(symbol); productID != "" {
			products = append(products, productID)
		}
	}

	if len(products) == 0 {
		log.Printf("No Coinbase products mapped for symbols: %v", symbols)
		return
	}

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("Coinbase spot connection error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Connected to Coinbase spot WebSocket")

		subscribe := map[string]interface{}{
			"type":        "subscribe",
			"product_ids": products,
			"channels":    []string{"ticker"},
		}

		if err := conn.WriteJSON(subscribe); err != nil {
			log.Printf("Coinbase spot subscription error: %v", err)
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		for {
			var raw json.RawMessage
			err := conn.ReadJSON(&raw)
			if err != nil {
				log.Printf("Coinbase spot read error: %v", err)
				conn.Close()
				break
			}

			var msg coinbaseTickerMessage
			if err := json.Unmarshal(raw, &msg); err != nil {
				continue
			}
			if msg.Type != "ticker" || msg.ProductID == "" {
				continue
			}

			symbol := convertFromCoinbaseProduct(msg.ProductID)
			if symbol == "" {
				continue
			}

			ts := time.Now().UnixMilli()
			if msg.Time != "" {
				if parsed, err := time.Parse(time.RFC3339Nano, msg.Time); err == nil {
					ts = parsed.UnixMilli()
				}
			}

			// Prefer best bid/ask when present.
			bid, bidErr := strconv.ParseFloat(msg.BestBid, 64)
			ask, askErr := strconv.ParseFloat(msg.BestAsk, 64)
			if bidErr == nil && askErr == nil && bid > 0 && ask > 0 {
				orderbookChan <- OrderbookData{
					Symbol:    symbol,
					Source:    "coinbase_spot",
					BestBid:   bid,
					BestAsk:   ask,
					Timestamp: ts,
				}
			}

			if msg.Price != "" {
				if px, err := strconv.ParseFloat(msg.Price, 64); err == nil {
					qty := msg.LastSize
					if qty == "" {
						qty = "0"
					}
					side := strings.ToLower(msg.Side)
					if side != "buy" && side != "sell" {
						side = "buy"
					}

					tradeChan <- TradeData{
						Symbol:    symbol,
						Source:    "coinbase_spot",
						Price:     px,
						Quantity:  qty,
						Side:      side,
						Timestamp: ts,
					}
				}
			}
		}

		time.Sleep(2 * time.Second)
	}
}

func convertToCoinbaseProduct(symbol string) string {
	switch symbol {
	case "BTCUSDT":
		return "BTC-USD"
	case "ETHUSDT":
		return "ETH-USD"
	case "XRPUSDT":
		return "XRP-USD"
	case "SOLUSDT":
		return "SOL-USD"
	default:
		return ""
	}
}

func convertFromCoinbaseProduct(product string) string {
	switch product {
	case "BTC-USD":
		return "BTCUSDT"
	case "ETH-USD":
		return "ETHUSDT"
	case "XRP-USD":
		return "XRPUSDT"
	case "SOL-USD":
		return "SOLUSDT"
	default:
		return ""
	}
}
