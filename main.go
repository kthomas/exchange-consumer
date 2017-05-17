package main

import (
	"sync"

	"github.com/kthomas/go-logger"
	"github.com/kthomas/go-amqputil"
)

var (
	Log = logger.NewLogger("AMQP", "debug", true)
	WaitGroup sync.WaitGroup

	Consumers = []*amqputil.Consumer{
		GdaxMessageConsumerFactory(Log, "BTC-USD"),
		GdaxMessageConsumerFactory(Log, "ETH-USD"),
		GdaxMessageConsumerFactory(Log, "ETH-BTC"),

		OandaMessageConsumerFactory(Log, "EUR-USD"),
		OandaMessageConsumerFactory(Log, "USD-CNY"),
		OandaMessageConsumerFactory(Log, "USD-JPY"),
	}
)

func bootstrap() {
	MigrateSchema()
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
	bootstrap()
	run()

	WaitGroup.Wait()
	Log.Infof("Exiting exchange consumer")
}
