package main

import (
	"encoding/json"

	"github.com/kthomas/go-amqputil"
	"github.com/kthomas/go-logger"
	"github.com/streadway/amqp"
)

type GdaxTickerMessageConsumer struct {
	log *logger.Logger
}

func (c *GdaxTickerMessageConsumer) Deliver(msg *amqp.Delivery) error {
	var gdaxMessage GdaxMessage
	err := json.Unmarshal(msg.Body, &gdaxMessage)
	if err == nil {
		c.log.Debugf("Unmarshaled AMQP message body to GDAX message: %s", gdaxMessage)

		if gdaxMessage.Type == "done" && gdaxMessage.Reason == "filled" && gdaxMessage.Price != "" {
			c.log.Infof("%s", gdaxMessage)
			c.log.Infof("FILLED @ %s", gdaxMessage.Price)
		}

		if err == nil {
			c.log.Debugf("Persisted GDAX message: %s", gdaxMessage)
			msg.Ack(false)
		} else {
			c.log.Errorf("Failed to persist GDAX message: %s", err)
			if !msg.Redelivered {
				return amqputil.AmqpDeliveryErrRequireRequeue
			} else {
				c.log.Errorf("GDAX message has already failed redelivery attempt, dropping message: %s", err)
			}
		}
	} else {
		c.log.Debugf("Failed to parse GDAX message: %s; %s", msg.Body, err)
		return amqputil.AmqpDeliveryErrRequireRequeue
	}
	return nil
}

func GdaxMessageConsumerFactory(lg *logger.Logger, symbol string) *amqputil.Consumer {
	consumer, err := newGdaxTickerMessageConsumer(Log, symbol)
	if err != nil {
		lg.Errorf("Failed to initialize GDAX message consumer for symbol %s; %s", symbol, err)
	}
	return consumer
}

func newGdaxTickerMessageConsumer(lg *logger.Logger, queue string) (*amqputil.Consumer, error) {
	delegate := new(GdaxTickerMessageConsumer)
	delegate.log = lg.Clone()

	config := amqputil.AmqpConfigFactory(queue)

	c, err := amqputil.NewConsumer(lg, config, "exchange-consumer", delegate)
	if err != nil {
		lg.Errorf("Failed to initialize AMQP consumer instance with config %s; %s", config, err)
		return nil, err
	}

	return c, nil
}

type OandaTickerMessageConsumer struct {
	log *logger.Logger
}

func (c *OandaTickerMessageConsumer) Deliver(msg *amqp.Delivery) error {
	var oandaMessage OandaMessage
	err := json.Unmarshal(msg.Body, &oandaMessage)
	if err == nil {
		c.log.Debugf("Unmarshaled AMQP message body to OANDA message: %s", oandaMessage)
		if oandaMessage.Type != "HEARTBEAT" {
			c.log.Warningf("OANDA AMQP message no-op")

			if err == nil {
				c.log.Debugf("Persisted OANDA message: %s", oandaMessage)
				msg.Ack(false)
			} else {
				c.log.Errorf("Failed to persist OANDA message: %s", err)
				if !msg.Redelivered {
					return amqputil.AmqpDeliveryErrRequireRequeue
				} else {
					c.log.Errorf("GDAX message has already failed redelivery attempt, dropping message: %s", err)
				}
			}
		} else {
			c.log.Debugf("Dropping OANDA heartbeat message: %s", oandaMessage)
			msg.Ack(false)
		}
	} else {
		c.log.Debugf("Failed to parse OANDA message: %s; %s", msg.Body, err)
		return amqputil.AmqpDeliveryErrRequireRequeue
	}
	return nil
}

func OandaMessageConsumerFactory(lg *logger.Logger, symbol string) *amqputil.Consumer {
	consumer, err := newOandaTickerMessageConsumer(Log, symbol)
	if err != nil {
		lg.Errorf("Failed to initialize OANDA message consumer for symbol %s; %s", symbol, err)
	}
	return consumer
}

func newOandaTickerMessageConsumer(lg *logger.Logger, queue string) (*amqputil.Consumer, error) {
	delegate := new(OandaTickerMessageConsumer)
	delegate.log = lg.Clone()

	config := amqputil.AmqpConfigFactory(queue)

	c, err := amqputil.NewConsumer(lg, config, "exchange-consumer", delegate)
	if err != nil {
		lg.Errorf("Failed to initialize AMQP consumer instance with config %s; %s", config, err)
		return nil, err
	}

	return c, nil
}
