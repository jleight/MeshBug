// Package mqtt connects to one or more MQTT brokers and forwards
// observer-published messages onto a channel for the ingest pipeline.
package mqtt

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"strings"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/jleight/meshbug/internal/config"
)

// Message is one delivered MQTT message annotated with the broker it came from.
type Message struct {
	Broker  string
	Topic   string
	Payload []byte
}

// Manager owns one paho client per broker.
type Manager struct {
	brokers []config.Broker
	clients []paho.Client
	out     chan Message
	log     *slog.Logger
}

func NewManager(brokers []config.Broker, log *slog.Logger) *Manager {
	return &Manager{
		brokers: brokers,
		out:     make(chan Message, 1024),
		log:     log,
	}
}

func (m *Manager) Messages() <-chan Message {
	return m.out
}

// Start connects to every broker and subscribes to <prefix>+/+/status and
// <prefix>+/+/packets. Errors per broker are logged and reconnect is handled
// by paho.
func (m *Manager) Start(ctx context.Context) error {
	for _, b := range m.brokers {
		clientID := fmt.Sprintf("meshbug-%s-%d", b.Name, time.Now().UnixNano())

		opts := paho.
			NewClientOptions().
			AddBroker(b.URL).
			SetClientID(clientID).
			SetUsername(b.Username).
			SetPassword(b.Password).
			SetCleanSession(true).
			SetAutoReconnect(true).
			SetConnectRetry(true).
			SetConnectRetryInterval(5 * time.Second).
			SetMaxReconnectInterval(60 * time.Second).
			SetKeepAlive(30 * time.Second)

		isTLS := strings.HasPrefix(b.URL, "wss://") ||
			strings.HasPrefix(b.URL, "ssl://") ||
			strings.HasPrefix(b.URL, "tls://") ||
			strings.HasPrefix(b.URL, "mqtts://")
		if isTLS {
			opts.SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12})
		}

		opts.SetOnConnectHandler(func(c paho.Client) {
			m.log.Info(
				"mqtt connected",
				"broker",
				b.Name,
				"url",
				b.URL,
			)

			for _, suffix := range []string{"+/+/status", "+/+/packets"} {
				topic := b.TopicPrefix + suffix

				token := c.Subscribe(
					topic,
					0,
					func(_ paho.Client, msg paho.Message) {
						select {
						case m.out <- Message{
							Broker:  b.Name,
							Topic:   msg.Topic(),
							Payload: msg.Payload(),
						}:
						case <-ctx.Done():
						}
					},
				)

				go func(topic string) {
					token.Wait()

					if err := token.Error(); err != nil {
						m.log.Error(
							"mqtt subscribe failed",
							"broker",
							b.Name,
							"topic",
							topic,
							"err",
							err,
						)
					} else {
						m.log.Info(
							"mqtt subscribed",
							"broker",
							b.Name,
							"topic",
							topic,
						)
					}
				}(topic)
			}
		})

		opts.SetConnectionLostHandler(func(_ paho.Client, err error) {
			m.log.Warn(
				"mqtt connection lost",
				"broker",
				b.Name,
				"err",
				err,
			)
		})

		c := paho.NewClient(opts)

		token := c.Connect()
		go func() {
			token.Wait()

			if err := token.Error(); err != nil {
				m.log.Error(
					"mqtt initial connect failed (will retry)",
					"broker",
					b.Name,
					"err",
					err,
				)
			}
		}()

		m.clients = append(m.clients, c)
	}

	go func() {
		<-ctx.Done()

		for i, c := range m.clients {
			c.Disconnect(500)

			m.log.Info(
				"mqtt disconnected",
				"broker",
				m.brokers[i].Name,
			)
		}

		close(m.out)
	}()

	return nil
}
