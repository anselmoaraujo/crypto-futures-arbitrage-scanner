package exchanges

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type coinbaseINTXMessage struct {
	Type          string `json:"type"`
	Channel       string `json:"channel"`
	ProductID     string `json:"product_id"`
	Time          string `json:"time"`
	BidPrice      string `json:"bid_price"`
	BidQty        string `json:"bid_qty"`
	AskPrice      string `json:"ask_price"`
	AskQty        string `json:"ask_qty"`
	TradePrice    string `json:"trade_price"`
	TradeQty      string `json:"trade_qty"`
	AggressorSide string `json:"aggressor_side"`
}

type coinbaseINTXBookState struct {
	bid float64
	ask float64
}

func ConnectCoinbaseFutures(symbols []string, priceChan chan<- PriceData, orderbookChan chan<- OrderbookData, tradeChan chan<- TradeData) {
	key := os.Getenv("COINBASE_INTX_KEY")
	secret := os.Getenv("COINBASE_INTX_SECRET")
	passphrase := os.Getenv("COINBASE_INTX_PASSPHRASE")
	if key == "" || secret == "" || passphrase == "" {
		log.Printf("Coinbase INTX futures disabled: set COINBASE_INTX_KEY/SECRET/PASSPHRASE")
		return
	}

	productIDs := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		if p := convertToCoinbasePerpProduct(symbol); p != "" {
			productIDs = append(productIDs, p)
		}
	}
	if len(productIDs) == 0 {
		log.Printf("No Coinbase INTX perp products mapped for symbols: %v", symbols)
		return
	}

	wsURL := "wss://ws-md.international.coinbase.com"
	book := make(map[string]coinbaseINTXBookState)

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("Coinbase INTX futures connection error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Connected to Coinbase INTX futures WebSocket")

		ts := strconv.FormatInt(time.Now().Unix(), 10)
		signature := buildCoinbaseINTXSignature(ts, key, passphrase, secret)
		subscribe := map[string]interface{}{
			"type":       "SUBSCRIBE",
			"product_ids": productIDs,
			"channels":   []string{"LEVEL1", "MATCH"},
			"time":       ts,
			"key":        key,
			"passphrase": passphrase,
			"signature":  signature,
		}

		if err := conn.WriteJSON(subscribe); err != nil {
			log.Printf("Coinbase INTX futures subscription error: %v", err)
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		for {
			var raw json.RawMessage
			err := conn.ReadJSON(&raw)
			if err != nil {
				log.Printf("Coinbase INTX futures read error: %v", err)
				conn.Close()
				break
			}

			var msg coinbaseINTXMessage
			if err := json.Unmarshal(raw, &msg); err != nil {
				continue
			}
			if msg.ProductID == "" {
				continue
			}

			symbol := convertFromCoinbasePerpProduct(msg.ProductID)
			if symbol == "" {
				continue
			}
			timestamp := parseCoinbaseTimestampMs(msg.Time)

			channel := strings.ToUpper(msg.Channel)
			switch channel {
			case "LEVEL1":
				state := book[msg.ProductID]
				if msg.BidPrice != "" {
					if v, err := strconv.ParseFloat(msg.BidPrice, 64); err == nil {
						state.bid = v
					}
				}
				if msg.AskPrice != "" {
					if v, err := strconv.ParseFloat(msg.AskPrice, 64); err == nil {
						state.ask = v
					}
				}
				book[msg.ProductID] = state

				if state.bid > 0 && state.ask > 0 {
					orderbookChan <- OrderbookData{
						Symbol:    symbol,
						Source:    "coinbase_futures",
						BestBid:   state.bid,
						BestAsk:   state.ask,
						Timestamp: timestamp,
					}
				}
			case "MATCH":
				if msg.TradePrice == "" {
					continue
				}
				price, err := strconv.ParseFloat(msg.TradePrice, 64)
				if err != nil {
					continue
				}
				qty := msg.TradeQty
				if qty == "" {
					qty = "0"
				}
				side := "buy"
				switch strings.ToUpper(msg.AggressorSide) {
				case "BUY":
					side = "buy"
				case "SELL":
					side = "sell"
				default:
					// Ignore OPENING_FILL / unknown aggressor values for side-normalized trade feed.
					continue
				}

				tradeChan <- TradeData{
					Symbol:    symbol,
					Source:    "coinbase_futures",
					Price:     price,
					Quantity:  qty,
					Side:      side,
					Timestamp: timestamp,
				}
			}
		}

		time.Sleep(2 * time.Second)
	}
}

func buildCoinbaseINTXSignature(ts, key, passphrase, secret string) string {
	payload := ts + key + "CBINTLMD" + passphrase

	secretBytes, err := base64.StdEncoding.DecodeString(secret)
	if err != nil {
		secretBytes = []byte(secret)
	}

	mac := hmac.New(sha256.New, secretBytes)
	mac.Write([]byte(payload))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func parseCoinbaseTimestampMs(raw string) int64 {
	if raw == "" {
		return time.Now().UnixMilli()
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t.UnixMilli()
	}
	return time.Now().UnixMilli()
}

func convertToCoinbasePerpProduct(symbol string) string {
	switch symbol {
	case "BTCUSDT":
		return "BTC-PERP"
	case "ETHUSDT":
		return "ETH-PERP"
	case "XRPUSDT":
		return "XRP-PERP"
	case "SOLUSDT":
		return "SOL-PERP"
	default:
		return ""
	}
}

func convertFromCoinbasePerpProduct(product string) string {
	switch product {
	case "BTC-PERP":
		return "BTCUSDT"
	case "ETH-PERP":
		return "ETHUSDT"
	case "XRP-PERP":
		return "XRPUSDT"
	case "SOL-PERP":
		return "SOLUSDT"
	default:
		return ""
	}
}
