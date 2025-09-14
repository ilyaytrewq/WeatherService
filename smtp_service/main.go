package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	gomail "gopkg.in/gomail.v2"
)

type EmailTask struct {
	To      string                 `json:"to"`
	Subject string                 `json:"subject"`
	Body    string                 `json:"body"`
	Type    string                 `json:"type,omitempty"`
	Meta    map[string]interface{} `json:"meta,omitempty"`
}

func main() {
	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		log.Fatal("RABBITMQ_URL not set")
	}

	// Подключаемся к RabbitMQ с ретраями
	var conn *amqp.Connection
	var err error
	for i := 0; i < 10; i++ {
		conn, err = amqp.Dial(rabbitURL)
		if err == nil {
			break
		}
		log.Printf("RabbitMQ not ready, retry in 2s... (%d/10)", i+1)
		time.Sleep(2 * time.Second)
	}
	if conn == nil {
		log.Fatalf("could not connect to rabbit after retries: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("chan: %v", err)
	}
	defer ch.Close()

	queueName := "email_queue"

	_, err = ch.QueueDeclare(
		queueName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		log.Fatalf("declare queue: %v", err)
	}

	if err := ch.Qos(5, 0, false); err != nil {
		log.Fatalf("qos: %v", err)
	}

	msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("consume: %v", err)
	}

	smtpHost := os.Getenv("SMTP_HOST")
	smtpPort, err := strconv.Atoi(os.Getenv("SMTP_PORT"))
	if err != nil {
		log.Fatalf("Invalid SMTP_PORT: %v", err)
	}
	smtpUser := os.Getenv("SMTP_USER")
	smtpPass := os.Getenv("SMTP_PASSWORD")
	fromAddr := os.Getenv("SMTP_FROM")

	workerCount := 3
	for i := 0; i < workerCount; i++ {
		go func(id int) {
			log.Printf("worker %d started", id)
			for d := range msgs {
				var t EmailTask
				if err := json.Unmarshal(d.Body, &t); err != nil {
					log.Printf("worker %d: bad message json: %v", id, err)
					d.Ack(false)
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				err := sendMail(ctx, smtpHost, smtpPort, smtpUser, smtpPass, fromAddr, t)
				cancel()
				if err != nil {
					log.Printf("worker %d: send mail failed for %s: %v", id, t.To, err)
					d.Nack(false, true) // повторим сообщение
					continue
				}
				d.Ack(false)
				log.Printf("worker %d: email sent to %s", id, t.To)
			}
		}(i)
	}

	// ждём сигнала для graceful shutdown
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	s := <-sigc
	log.Printf("Shutting down, signal: %v", s)
	ch.Close()
	conn.Close()
	time.Sleep(500 * time.Millisecond)
}

func sendMail(ctx context.Context, host string, port int, user, pass, from string, t EmailTask) error {
	m := gomail.NewMessage()
	m.SetHeader("From", from)
	m.SetHeader("To", t.To)
	m.SetHeader("Subject", t.Subject)
	m.SetBody("text/html", t.Body)

	d := gomail.NewDialer(host, port, user, pass)

	errCh := make(chan error, 1)
	go func() {
		errCh <- d.DialAndSend(m)
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("send timeout")
	case err := <-errCh:
		return err
	}
}
