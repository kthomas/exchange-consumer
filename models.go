package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kthomas/go-db-config"
)

type Model struct {
	ID     uint   `gorm:"primary_key",json:"id"`
	Errors Errors `gorm:"-",json:"-"`
}

type Error struct {
	Message string `json:"message"`
	Status  int    `json:"status"`
}
type Errors []Error

type Tick struct {
	Model
	Symbol    string    `gorm:"index:idx_tick_symbol;not null",json:"symbol"`
	Timestamp time.Time `gorm:"index:idx_tick_timestamp;not null",json:"timestamp"`
	Type      string    `gorm:"index:idx_tick_type",json:"type"`
	OrderType string    `gorm:"index:idx_tick_order_type",json:"order_type"`
	Side      string    `gorm:"index:idx_tick_order_side",json:"side"`
	Bid       float64   `json:"bid"`
	Ask       float64   `json:"ask"`
	Price     float64   `json:"price"`
	Size      float64   `json:"size"`
	Volume    float64   `json:"volume"`
	Liquidity float64   `json:"liquidity"`
}
type Ticks []Tick

func (m *Tick) Create() (bool, []error) {
	var errs []error
	db := dbconf.DatabaseConnection()
	if db.NewRecord(m) {
		result := db.Create(&m)
		rowsAffected := result.RowsAffected
		errs = result.GetErrors()
		if !db.NewRecord(m) {
			return (rowsAffected > 0), errs
		}
	}
	return false, errs
}

//type TickerMessage interface {
//	CreateTick() error
//}

type GdaxMessage struct {
	Sequence  int64     `json:"sequence"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"time"`
	ProductId string    `json:"product_id"`
	ClientId  string    `json:"client_oid,omitempty"`
	OrderId   string    `json:"order_id,omitempty"`
	OrderType string    `json:"order_type,omitempty"`
	Bid       string    `json:"bid,omitempty"`
	Ask       string    `json:"ask,omitempty"`
	Size      string    `json:"size,omitempty"`
	Price     string    `json:"price"`
	Funds     string    `json:"funds,omitempty"`
	Side      string    `json:"side,omitempty"`
	Volume    string    `json:"volume,omitempty"`
	Reason    string    `json:"reason,omitempty"`
}

func (msg *GdaxMessage) CreateTick() error {
	tick := &Tick{
		Symbol:    msg.ProductId,
		Timestamp: msg.Timestamp,
		Type:      msg.Type,
		OrderType: msg.OrderType,
		Side:      msg.Side,
	}
	if msg.Bid != "" {
		tick.Bid, _ = strconv.ParseFloat(msg.Bid, 8)
	}
	if msg.Ask != "" {
		tick.Ask, _ = strconv.ParseFloat(msg.Ask, 8)
	}
	if msg.Price != "" {
		tick.Price, _ = strconv.ParseFloat(msg.Price, 8)
	}
	if msg.Size != "" {
		tick.Size, _ = strconv.ParseFloat(msg.Size, 8)
	}
	if msg.Volume != "" {
		tick.Volume, _ = strconv.ParseFloat(msg.Volume, 8)
	}
	created, errs := tick.Create()
	if !created || len(errs) > 0 {
		return errors.New("Failed to persist GDAX message tick")
	}
	return nil
}

func (msg *GdaxMessage) Tick() error {

	return nil
}

type OandaMessage struct {
	Type        string                   `json:"type"`
	Timestamp   time.Time                `json:"time"`
	Bids        []map[string]interface{} `json:"bids,omitempty"`
	Asks        []map[string]interface{} `json:"asks,omitempty"`
	CloseoutBid string                   `json:"closeoutBid,omitempty"`
	CloseoutAsk string                   `json:"closeoutAsk,omitempty"`
	Status      string                   `json:"status,omitempty"`
	Tradeable   bool                     `json:"tradeable,omitempty"`
	Instrument  string                   `json:"instrument,omitempty"`
}

func (msg *OandaMessage) CreateTick() error {
	createdCount := 0
	for _, ask := range msg.Asks {
		tick := &Tick{
			Symbol:    strings.Replace(msg.Instrument, "_", "-", 1),
			Timestamp: msg.Timestamp,
			Type:      strings.ToLower(msg.Type),
		}
		if price, ok := ask["price"]; ok {
			tick.Ask, _ = strconv.ParseFloat(price.(string), 8)
		}
		if liquidity, ok := ask["liquidity"]; ok {
			tick.Liquidity = liquidity.(float64)
		}
		created, errs := tick.Create()
		if created {
			createdCount += 1
		} else if !created || len(errs) > 0 {
			return errors.New("Failed to persist ask tick in OANDA message")
		}
	}
	for _, bid := range msg.Bids {
		tick := &Tick{
			Symbol:    strings.Replace(msg.Instrument, "_", "-", 1),
			Timestamp: msg.Timestamp,
			Type:      strings.ToLower(msg.Type),
		}
		if price, ok := bid["price"]; ok {
			tick.Ask, _ = strconv.ParseFloat(price.(string), 8)
		}
		if liquidity, ok := bid["liquidity"]; ok {
			tick.Liquidity = liquidity.(float64)
		}
		created, errs := tick.Create()
		if created {
			createdCount += 1
		} else if !created || len(errs) > 0 {
			return errors.New("Failed to persist bid tick in OANDA message")
		}
	}
	expectedCount := len(msg.Asks) + len(msg.Bids)
	if createdCount != expectedCount {
		msg := fmt.Sprintf("Failed to persist all OANDA bid/ask ticks; expected %v but persisted only %v", expectedCount, createdCount)
		return errors.New(msg)
	}
	return nil
}
