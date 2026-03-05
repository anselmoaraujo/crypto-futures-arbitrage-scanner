package exchanges

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const fundingPollInterval = 30 * time.Second

var fundingHTTPClient = &http.Client{
	Timeout: 12 * time.Second,
}

func ConnectFundingRates(symbols []string, fundingChan chan<- FundingData) {
	log.Printf("Starting funding-rate polling for perpetual futures")

	poll := func() {
		now := time.Now().UnixMilli()

		publishFunding(fetchBinanceFunding(symbols, now), fundingChan)
		publishFunding(fetchBybitFunding(symbols, now), fundingChan)
		publishFunding(fetchOKXFunding(symbols, now), fundingChan)
		publishFunding(fetchKrakenFunding(now), fundingChan)
		publishFunding(fetchHyperliquidFunding(symbols, now), fundingChan)
	}

	poll()

	ticker := time.NewTicker(fundingPollInterval)
	defer ticker.Stop()

	for range ticker.C {
		poll()
	}
}

func publishFunding(items []FundingData, fundingChan chan<- FundingData) {
	for _, item := range items {
		fundingChan <- item
	}
}

func fetchBinanceFunding(symbols []string, now int64) []FundingData {
	type response struct {
		Symbol          string `json:"symbol"`
		LastFundingRate string `json:"lastFundingRate"`
		NextFundingTime int64  `json:"nextFundingTime"`
		MarkPrice       string `json:"markPrice"`
		IndexPrice      string `json:"indexPrice"`
	}

	results := make([]FundingData, 0, len(symbols))
	for _, symbol := range symbols {
		url := fmt.Sprintf("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=%s", symbol)
		body, err := httpGet(url)
		if err != nil {
			log.Printf("Binance funding fetch error for %s: %v", symbol, err)
			continue
		}

		var resp response
		if err := json.Unmarshal(body, &resp); err != nil {
			log.Printf("Binance funding decode error for %s: %v", symbol, err)
			continue
		}

		rate, err := strconv.ParseFloat(resp.LastFundingRate, 64)
		if err != nil {
			continue
		}
		markPrice, _ := strconv.ParseFloat(resp.MarkPrice, 64)
		indexPrice, _ := strconv.ParseFloat(resp.IndexPrice, 64)
		premiumPct := calcPremiumPct(markPrice, indexPrice)

		results = append(results, FundingData{
			Symbol:          symbol,
			Source:          "binance_futures",
			Rate:            rate,
			LastRate:        rate,
			NextRate:        rate,
			NextFundingTime: resp.NextFundingTime,
			MarkPrice:       markPrice,
			IndexPrice:      indexPrice,
			PremiumPct:      premiumPct,
			Timestamp:       now,
		})
	}

	return results
}

func fetchBybitFunding(symbols []string, now int64) []FundingData {
	type tickerItem struct {
		Symbol          string `json:"symbol"`
		FundingRate     string `json:"fundingRate"`
		NextFundingTime string `json:"nextFundingTime"`
		MarkPrice       string `json:"markPrice"`
		IndexPrice      string `json:"indexPrice"`
	}
	type response struct {
		Result struct {
			List []tickerItem `json:"list"`
		} `json:"result"`
	}

	results := make([]FundingData, 0, len(symbols))
	for _, symbol := range symbols {
		url := fmt.Sprintf("https://api.bybit.com/v5/market/tickers?category=linear&symbol=%s", symbol)
		body, err := httpGet(url)
		if err != nil {
			log.Printf("Bybit funding fetch error for %s: %v", symbol, err)
			continue
		}

		var resp response
		if err := json.Unmarshal(body, &resp); err != nil {
			log.Printf("Bybit funding decode error for %s: %v", symbol, err)
			continue
		}
		if len(resp.Result.List) == 0 {
			continue
		}

		item := resp.Result.List[0]
		rate, err := strconv.ParseFloat(item.FundingRate, 64)
		if err != nil {
			continue
		}
		markPrice, _ := strconv.ParseFloat(item.MarkPrice, 64)
		indexPrice, _ := strconv.ParseFloat(item.IndexPrice, 64)
		premiumPct := calcPremiumPct(markPrice, indexPrice)

		nextFunding := now + 8*60*60*1000
		if item.NextFundingTime != "" {
			if v, err := strconv.ParseInt(item.NextFundingTime, 10, 64); err == nil {
				nextFunding = v
			}
		}

		results = append(results, FundingData{
			Symbol:          symbol,
			Source:          "bybit_futures",
			Rate:            rate,
			LastRate:        rate,
			NextRate:        rate,
			NextFundingTime: nextFunding,
			MarkPrice:       markPrice,
			IndexPrice:      indexPrice,
			PremiumPct:      premiumPct,
			Timestamp:       now,
		})
	}

	return results
}

func fetchOKXFunding(symbols []string, now int64) []FundingData {
	type fundingItem struct {
		FundingRate     string `json:"fundingRate"`
		NextFundingRate string `json:"nextFundingRate"`
		NextFundingTime string `json:"nextFundingTime"`
	}
	type response struct {
		Data []fundingItem `json:"data"`
	}

	results := make([]FundingData, 0, len(symbols))
	for _, symbol := range symbols {
		instID := toOKXSwapInstID(symbol)
		url := fmt.Sprintf("https://www.okx.com/api/v5/public/funding-rate?instId=%s", instID)
		body, err := httpGet(url)
		if err != nil {
			log.Printf("OKX funding fetch error for %s: %v", symbol, err)
			continue
		}

		var resp response
		if err := json.Unmarshal(body, &resp); err != nil {
			log.Printf("OKX funding decode error for %s: %v", symbol, err)
			continue
		}
		if len(resp.Data) == 0 {
			continue
		}

		item := resp.Data[0]
		lastRate, err := strconv.ParseFloat(item.FundingRate, 64)
		if err != nil {
			continue
		}
		nextRate := lastRate
		if item.NextFundingRate != "" {
			if v, err := strconv.ParseFloat(item.NextFundingRate, 64); err == nil {
				nextRate = v
			}
		}

		nextFunding := now + 8*60*60*1000
		if item.NextFundingTime != "" {
			if v, err := strconv.ParseInt(item.NextFundingTime, 10, 64); err == nil {
				nextFunding = v
			}
		}

		markPrice, indexPrice := fetchOKXMarkAndIndex(instID)
		premiumPct := calcPremiumPct(markPrice, indexPrice)

		results = append(results, FundingData{
			Symbol:          symbol,
			Source:          "okx_futures",
			Rate:            nextRate,
			LastRate:        lastRate,
			NextRate:        nextRate,
			NextFundingTime: nextFunding,
			MarkPrice:       markPrice,
			IndexPrice:      indexPrice,
			PremiumPct:      premiumPct,
			Timestamp:       now,
		})
	}

	return results
}

func fetchKrakenFunding(now int64) []FundingData {
	type ticker struct {
		Symbol             string          `json:"symbol"`
		FundingRate        json.RawMessage `json:"fundingRate"`
		FundingRatePred    json.RawMessage `json:"fundingRatePrediction"`
		NextFundingRateTS  json.RawMessage `json:"nextFundingRateTime"`
		MarkPrice          json.RawMessage `json:"markPrice"`
		IndexPrice         json.RawMessage `json:"indexPrice"`
	}
	type response struct {
		Result string `json:"result"`
		Tickers []ticker `json:"tickers"`
	}

	body, err := httpGet("https://futures.kraken.com/derivatives/api/v3/tickers")
	if err != nil {
		log.Printf("Kraken funding fetch error: %v", err)
		return nil
	}

	var resp response
	if err := json.Unmarshal(body, &resp); err != nil {
		log.Printf("Kraken funding decode error: %v", err)
		return nil
	}

	targets := map[string]string{
		"PF_XBTUSD": "BTCUSDT",
		"PF_ETHUSD": "ETHUSDT",
		"PF_XRPUSD": "XRPUSDT",
		"PF_SOLUSD": "SOLUSDT",
	}

	results := make([]FundingData, 0, 4)
	for _, t := range resp.Tickers {
		symbol, ok := targets[t.Symbol]
		if !ok {
			continue
		}

		lastRate, err := parseJSONFloat(t.FundingRate)
		if err != nil {
			continue
		}
		nextRate, err := parseJSONFloat(t.FundingRatePred)
		if err != nil {
			nextRate = lastRate
		}
		// Kraken funding fields are often percent units; normalize to decimal.
		lastRate = normalizeKrakenRate(lastRate)
		nextRate = normalizeKrakenRate(nextRate)
		markPrice, _ := parseJSONFloat(t.MarkPrice)
		indexPrice, _ := parseJSONFloat(t.IndexPrice)
		premiumPct := calcPremiumPct(markPrice, indexPrice)

		nextFunding := now + 4*60*60*1000
		if len(t.NextFundingRateTS) > 0 {
			if v, err := parseJSONTimestampMs(t.NextFundingRateTS); err == nil {
				nextFunding = v
			}
		}

		results = append(results, FundingData{
			Symbol:          symbol,
			Source:          "kraken_futures",
			Rate:            nextRate,
			LastRate:        lastRate,
			NextRate:        nextRate,
			NextFundingTime: nextFunding,
			MarkPrice:       markPrice,
			IndexPrice:      indexPrice,
			PremiumPct:      premiumPct,
			Timestamp:       now,
		})
	}

	return results
}

func fetchHyperliquidFunding(symbols []string, now int64) []FundingData {
	requestBody := map[string]string{
		"type": "metaAndAssetCtxs",
	}
	body, err := httpPostJSON("https://api.hyperliquid.xyz/info", requestBody)
	if err != nil {
		log.Printf("Hyperliquid funding fetch error: %v", err)
		return nil
	}

	var root []json.RawMessage
	if err := json.Unmarshal(body, &root); err != nil || len(root) < 2 {
		log.Printf("Hyperliquid funding decode error: unexpected response format")
		return nil
	}

	var meta struct {
		Universe []struct {
			Name string `json:"name"`
		} `json:"universe"`
	}
	if err := json.Unmarshal(root[0], &meta); err != nil {
		log.Printf("Hyperliquid funding decode error (meta): %v", err)
		return nil
	}

	type assetCtx struct {
		Funding  json.RawMessage `json:"funding"`
		MarkPx   json.RawMessage `json:"markPx"`
		OraclePx json.RawMessage `json:"oraclePx"`
	}
	var ctxs []assetCtx
	if err := json.Unmarshal(root[1], &ctxs); err != nil {
		log.Printf("Hyperliquid funding decode error (asset ctx): %v", err)
		return nil
	}

	symbolSet := make(map[string]struct{}, len(symbols))
	for _, s := range symbols {
		symbolSet[s] = struct{}{}
	}

	nextFunding := nextHourUnixMs(now)
	results := make([]FundingData, 0, len(symbols))
	for i, coin := range meta.Universe {
		if i >= len(ctxs) {
			break
		}
		symbol := strings.ToUpper(coin.Name) + "USDT"
		if _, ok := symbolSet[symbol]; !ok {
			continue
		}

		fundingRate, err := parseJSONFloat(ctxs[i].Funding)
		if err != nil {
			continue
		}
		markPrice, _ := parseJSONFloat(ctxs[i].MarkPx)
		indexPrice, _ := parseJSONFloat(ctxs[i].OraclePx)
		premiumPct := calcPremiumPct(markPrice, indexPrice)

		results = append(results, FundingData{
			Symbol:          symbol,
			Source:          "hyperliquid_futures",
			Rate:            fundingRate,
			LastRate:        fundingRate,
			NextRate:        fundingRate,
			NextFundingTime: nextFunding,
			MarkPrice:       markPrice,
			IndexPrice:      indexPrice,
			PremiumPct:      premiumPct,
			Timestamp:       now,
		})
	}

	return results
}

func httpGet(url string) ([]byte, error) {
	resp, err := fundingHTTPClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

func httpPostJSON(url string, payload interface{}) ([]byte, error) {
	reqBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := fundingHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

func toOKXSwapInstID(symbol string) string {
	if strings.HasSuffix(symbol, "USDT") {
		base := strings.TrimSuffix(symbol, "USDT")
		return base + "-USDT-SWAP"
	}
	return symbol + "-SWAP"
}

func fetchOKXMarkAndIndex(instID string) (float64, float64) {
	type tickerItem struct {
		MarkPx  string `json:"markPx"`
		IndexPx string `json:"idxPx"`
	}
	type response struct {
		Data []tickerItem `json:"data"`
	}

	url := fmt.Sprintf("https://www.okx.com/api/v5/market/ticker?instId=%s", instID)
	body, err := httpGet(url)
	if err != nil {
		return 0, 0
	}

	var resp response
	if err := json.Unmarshal(body, &resp); err != nil || len(resp.Data) == 0 {
		return 0, 0
	}

	mark, _ := strconv.ParseFloat(resp.Data[0].MarkPx, 64)
	index, _ := strconv.ParseFloat(resp.Data[0].IndexPx, 64)
	return mark, index
}

func calcPremiumPct(markPrice, indexPrice float64) float64 {
	if indexPrice == 0 {
		return 0
	}
	return ((markPrice - indexPrice) / indexPrice) * 100
}

func parseAnyFloat(values ...string) (float64, bool) {
	for _, raw := range values {
		if raw == "" {
			continue
		}
		v, err := strconv.ParseFloat(raw, 64)
		if err == nil {
			// Kraken may expose percentage as percent units.
			if strings.Contains(raw, "%") {
				return v / 100.0, true
			}
			return v, true
		}
	}
	return 0, false
}

func parseAnyFloatJSON(values ...json.RawMessage) (float64, bool) {
	for _, raw := range values {
		v, err := parseJSONFloat(raw)
		if err == nil {
			return v, true
		}
	}
	return 0, false
}

func parseJSONFloat(raw json.RawMessage) (float64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, fmt.Errorf("empty")
	}

	// Number form.
	var asNum float64
	if err := json.Unmarshal(raw, &asNum); err == nil {
		return asNum, nil
	}

	// String form.
	var asStr string
	if err := json.Unmarshal(raw, &asStr); err == nil {
		asStr = strings.TrimSpace(asStr)
		if asStr == "" {
			return 0, fmt.Errorf("empty string")
		}
		hasPercent := strings.HasSuffix(asStr, "%")
		asStr = strings.TrimSuffix(asStr, "%")
		v, err := strconv.ParseFloat(asStr, 64)
		if err != nil {
			return 0, err
		}
		if hasPercent {
			v = v / 100.0
		}
		return v, nil
	}

	return 0, fmt.Errorf("unsupported float format")
}

func parseJSONTimestampMs(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, fmt.Errorf("empty")
	}

	var asNum int64
	if err := json.Unmarshal(raw, &asNum); err == nil {
		if asNum < 1_000_000_000_000 {
			return asNum * 1000, nil
		}
		return asNum, nil
	}

	var asStr string
	if err := json.Unmarshal(raw, &asStr); err == nil {
		return parseTimestampFlexible(asStr)
	}

	return 0, fmt.Errorf("unsupported timestamp format")
}

func parseTimestampFlexible(raw string) (int64, error) {
	if raw == "" {
		return 0, fmt.Errorf("empty timestamp")
	}

	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if n < 1_000_000_000_000 {
			return n * 1000, nil
		}
		return n, nil
	}

	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return 0, err
	}
	return parsed.UnixMilli(), nil
}

func nextHourUnixMs(nowMs int64) int64 {
	t := time.UnixMilli(nowMs).UTC()
	next := t.Truncate(time.Hour).Add(time.Hour)
	return next.UnixMilli()
}

func normalizeKrakenRate(v float64) float64 {
	// Values above 1% are unlikely as decimal funding rates and are typically percent points.
	if v > 0.01 || v < -0.01 {
		return v / 100.0
	}
	return v
}
