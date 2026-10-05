//go:build kafka

package main

import (
	"log/slog"

	"github.com/ShrinidhiMane/eventbank/internal/broker"
	"github.com/ShrinidhiMane/eventbank/internal/kafka"
	"github.com/ShrinidhiMane/eventbank/internal/metrics"
)

func init() {
	newKafkaBroker = func(bootstrap, group string, m *metrics.Registry, log *slog.Logger) (broker.Broker, error) {
		return kafka.New(bootstrap, group, m, log)
	}
}
