package exchanges

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	binanceReadTimeout       = 70 * time.Second
	binanceWriteTimeout      = 10 * time.Second
	binancePingInterval      = 20 * time.Second
	binanceBaseReconnectWait = 2 * time.Second
	binanceMaxReconnectWait  = 30 * time.Second
	binanceLogEveryNFailures = 5
)

func setupBinanceConnection(conn *websocket.Conn) chan struct{} {
	stopPing := make(chan struct{})

	_ = conn.SetReadDeadline(time.Now().Add(binanceReadTimeout))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(binanceReadTimeout))
	})

	go func() {
		ticker := time.NewTicker(binancePingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				_ = conn.SetWriteDeadline(time.Now().Add(binanceWriteTimeout))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			case <-stopPing:
				return
			}
		}
	}()

	return stopPing
}

func binanceReconnectDelay(attempt int) time.Duration {
	// Exponential backoff capped at binanceMaxReconnectWait.
	delay := binanceBaseReconnectWait
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= binanceMaxReconnectWait {
			return binanceMaxReconnectWait
		}
	}
	if delay > binanceMaxReconnectWait {
		return binanceMaxReconnectWait
	}
	return delay
}

func shouldLogBinanceFailure(failureCount int) bool {
	return failureCount == 1 || failureCount%binanceLogEveryNFailures == 0
}

type BinanceFuturesTrade struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	TradeID   int64  `json:"a"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
	TradeTime int64  `json:"T"`
	IsMaker   bool   `json:"m"`
}

type BinanceFuturesBookTicker struct {
	EventType   string `json:"e"`
	EventTime   int64  `json:"E"`
	Symbol      string `json:"s"`
	BestBidPrice string `json:"b"`
	BestBidQty   string `json:"B"`
	BestAskPrice string `json:"a"`
	BestAskQty   string `json:"A"`
}

func ConnectBinanceFutures(symbols []string, priceChan chan<- PriceData, orderbookChan chan<- OrderbookData, tradeChan chan<- TradeData) {
	streamNames := make([]string, len(symbols)*2)
	for i, symbol := range symbols {
		streamNames[i*2] = strings.ToLower(symbol) + "@bookTicker"
		streamNames[i*2+1] = strings.ToLower(symbol) + "@aggTrade"
	}
	streamParam := strings.Join(streamNames, "/")

	wsURL := fmt.Sprintf("wss://fstream.binance.com/stream?streams=%s", streamParam)
	reconnectAttempt := 0
	readFailures := 0

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			reconnectAttempt++
			wait := binanceReconnectDelay(reconnectAttempt)
			if shouldLogBinanceFailure(reconnectAttempt) {
				log.Printf("Binance futures connection error (attempt=%d, retry=%s): %v", reconnectAttempt, wait, err)
			}
			time.Sleep(wait)
			continue
		}

		log.Printf("Connected to Binance futures WebSocket")
		reconnectAttempt = 0
		readFailures = 0
		stopPing := setupBinanceConnection(conn)

		for {
			var message struct {
				Stream string          `json:"stream"`
				Data   json.RawMessage `json:"data"`
			}

			err := conn.ReadJSON(&message)
			if err != nil {
				readFailures++
				if shouldLogBinanceFailure(readFailures) {
					log.Printf("Binance futures read error (count=%d): %v", readFailures, err)
				}
				close(stopPing)
				conn.Close()
				break
			}
			_ = conn.SetReadDeadline(time.Now().Add(binanceReadTimeout))

			if strings.Contains(message.Stream, "@bookTicker") {
				var bookTicker BinanceFuturesBookTicker
				if err := json.Unmarshal(message.Data, &bookTicker); err != nil {
					continue
				}

				bidPrice, err1 := strconv.ParseFloat(bookTicker.BestBidPrice, 64)
				askPrice, err2 := strconv.ParseFloat(bookTicker.BestAskPrice, 64)
				if err1 != nil || err2 != nil {
					continue
				}

				orderbookData := OrderbookData{
					Symbol:    bookTicker.Symbol,
					Source:    "binance_futures",
					BestBid:   bidPrice,
					BestAsk:   askPrice,
					Timestamp: bookTicker.EventTime,
				}

				orderbookChan <- orderbookData

			} else if strings.Contains(message.Stream, "@aggTrade") {
				var trade BinanceFuturesTrade
				if err := json.Unmarshal(message.Data, &trade); err != nil {
					continue
				}

				price, err := strconv.ParseFloat(trade.Price, 64)
				if err != nil {
					continue
				}

				// Normalize trade side (isMaker: false = buy aggressor, true = sell aggressor)
				var side string
				if !trade.IsMaker {
					side = "buy"
				} else {
					side = "sell"
				}

				tradeData := TradeData{
					Symbol:    trade.Symbol,
					Source:    "binance_futures",
					Price:     price,
					Quantity:  trade.Quantity,
					Side:      side,
					Timestamp: trade.TradeTime,
				}

				tradeChan <- tradeData
			}
		}

		reconnectAttempt++
		time.Sleep(binanceReconnectDelay(reconnectAttempt))
	}
}

// BinanceSpotTrade represents the structure for Binance spot trade data
type BinanceSpotTrade struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	TradeID   int64  `json:"a"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
	TradeTime int64  `json:"T"`
	IsMaker   bool   `json:"m"`
}

type BinanceSpotBookTicker struct {
	EventType    string `json:"e"`
	EventTime    int64  `json:"E"`
	Symbol       string `json:"s"`
	BestBidPrice string `json:"b"`
	BestBidQty   string `json:"B"`
	BestAskPrice string `json:"a"`
	BestAskQty   string `json:"A"`
}

// ConnectBinanceSpot connects to Binance spot trading WebSocket API
func ConnectBinanceSpot(symbols []string, priceChan chan<- PriceData, orderbookChan chan<- OrderbookData, tradeChan chan<- TradeData) {
	streamNames := make([]string, len(symbols)*2)
	for i, symbol := range symbols {
		streamNames[i*2] = strings.ToLower(symbol) + "@bookTicker"
		streamNames[i*2+1] = strings.ToLower(symbol) + "@aggTrade"
	}
	streamParam := strings.Join(streamNames, "/")

	wsURL := fmt.Sprintf("wss://stream.binance.com:9443/stream?streams=%s", streamParam)
	reconnectAttempt := 0
	readFailures := 0

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			reconnectAttempt++
			wait := binanceReconnectDelay(reconnectAttempt)
			if shouldLogBinanceFailure(reconnectAttempt) {
				log.Printf("Binance spot connection error (attempt=%d, retry=%s): %v", reconnectAttempt, wait, err)
			}
			time.Sleep(wait)
			continue
		}

		log.Printf("Connected to Binance spot WebSocket")
		reconnectAttempt = 0
		readFailures = 0
		stopPing := setupBinanceConnection(conn)

		for {
			var message struct {
				Stream string          `json:"stream"`
				Data   json.RawMessage `json:"data"`
			}

			err := conn.ReadJSON(&message)
			if err != nil {
				readFailures++
				if shouldLogBinanceFailure(readFailures) {
					log.Printf("Binance spot read error (count=%d): %v", readFailures, err)
				}
				close(stopPing)
				conn.Close()
				break
			}
			_ = conn.SetReadDeadline(time.Now().Add(binanceReadTimeout))

			if strings.Contains(message.Stream, "@bookTicker") {
				var bookTicker BinanceSpotBookTicker
				if err := json.Unmarshal(message.Data, &bookTicker); err != nil {
					continue
				}

				bidPrice, err1 := strconv.ParseFloat(bookTicker.BestBidPrice, 64)
				askPrice, err2 := strconv.ParseFloat(bookTicker.BestAskPrice, 64)
				if err1 != nil || err2 != nil {
					continue
				}

				orderbookData := OrderbookData{
					Symbol:    bookTicker.Symbol,
					Source:    "binance_spot",
					BestBid:   bidPrice,
					BestAsk:   askPrice,
					Timestamp: bookTicker.EventTime,
				}

				orderbookChan <- orderbookData

			} else if strings.Contains(message.Stream, "@aggTrade") {
				var trade BinanceSpotTrade
				if err := json.Unmarshal(message.Data, &trade); err != nil {
					continue
				}

				price, err := strconv.ParseFloat(trade.Price, 64)
				if err != nil {
					continue
				}

				// Normalize trade side (isMaker: false = buy aggressor, true = sell aggressor)
				var side string
				if !trade.IsMaker {
					side = "buy"
				} else {
					side = "sell"
				}

				tradeData := TradeData{
					Symbol:    trade.Symbol,
					Source:    "binance_spot",
					Price:     price,
					Quantity:  trade.Quantity,
					Side:      side,
					Timestamp: trade.TradeTime,
				}

				tradeChan <- tradeData
			}
		}

		reconnectAttempt++
		time.Sleep(binanceReconnectDelay(reconnectAttempt))
	}
}
