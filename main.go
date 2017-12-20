package main

import (
	"sync"

	"github.com/kthomas/go-amqputil"
	"github.com/kthomas/go-logger"
)

var (
	Log       = logger.NewLogger("exchange-consumer", "INFO", true)
	WaitGroup sync.WaitGroup

	Consumers = []*amqputil.Consumer{
		//GdaxMessageConsumerFactory(Log, "BTC-USD"),
		//GdaxMessageConsumerFactory(Log, "ETH-USD"),
		//GdaxMessageConsumerFactory(Log, "ETH-BTC"),
		GdaxMessageConsumerFactory(Log, "LTC-USD"),

		//OandaMessageConsumerFactory(Log, "EUR-USD"),
		//OandaMessageConsumerFactory(Log, "USD-CNY"),
		//OandaMessageConsumerFactory(Log, "USD-JPY"),
	}
)

func bootstrap() {
	MigrateSchema()
}

func setupLogging() {
	prefix := "exchange-consumer"
	lvl := "INFO"
	console := true

	Log = logger.NewLogger(prefix, lvl, console)
	Log.Infof("Logging initialized; log level: %s", lvl)
}

func run() {
	for _, consumer := range Consumers {
		runConsumer(consumer)
	}
}

func runConsumer(consumer *amqputil.Consumer) {
	WaitGroup.Add(1)
	go func() {
		consumer.Run()
		Log.Infof("Exiting ticker message consumer %s", consumer)
	}()
}

func main() {
	// FIXME-- reintroduce this if warehousing historical price data is
	setupLogging()
	bootstrap()
	run()

	WaitGroup.Wait()
	Log.Infof("Exiting exchange consumer")
}
