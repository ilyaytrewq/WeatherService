package weatherservice

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	rabbitConn    *amqp.Connection
	rabbitChannel *amqp.Channel
	emailExchange = "email_exchange"
	emailQueue    = "email_queue"
)

type EmailTask struct {
	To      string                 `json:"to"`
	Subject string                 `json:"subject"`
	Body    string                 `json:"body"`
	Type    string                 `json:"type,omitempty"`
	Meta    map[string]interface{} `json:"meta,omitempty"`
}

func InitRabbit() error {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		return fmt.Errorf("RABBITMQ_URL env not set")
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("InitRabbit: dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("InitRabbit: channel: %w", err)
	}

	if err := ch.ExchangeDeclare(
		emailExchange, // name
		"direct",      // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // args
	); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("InitRabbit: exchange declare: %w", err)
	}

	_, err = ch.QueueDeclare(
		emailQueue, // name
		true,       // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("InitRabbit: queue declare: %w", err)
	}

	if err := ch.QueueBind(emailQueue, "send_email", emailExchange, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("InitRabbit: queue bind: %w", err)
	}

	rabbitConn = conn
	rabbitChannel = ch
	log.Println("InitRabbit: connected")
	return nil
}

func publishEmailTask(ctx context.Context, task EmailTask) error {
	if rabbitChannel == nil {
		return fmt.Errorf("rabbit channel not initialized")
	}
	body, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("publishEmailTask: marshal: %w", err)
	}

	err = rabbitChannel.PublishWithContext(ctx,
		emailExchange, // exchange
		"send_email",  // routing key
		false,         // mandatory
		false,         // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
		},
	)
	if err != nil {
		return fmt.Errorf("publishEmailTask: publish: %w", err)
	}
	return nil
}
