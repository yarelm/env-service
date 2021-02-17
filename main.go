package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/pubsub"
	_ "github.com/lib/pq"
)

func main() {
	log.Print("starting server...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handleSignals(cancel)

	db, err := initDB(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err = consumePaymentEvents(ctx, db); err != nil {
		log.Fatal(err)
	}
	log.Printf("going down. bye!")
}

func handleSignals(doneFunc func()) {
	signalC := make(chan os.Signal, 1)
	signal.Notify(signalC, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-signalC
		log.Printf("got signal: %v", sig)
		doneFunc()
	}()
}

func initDB(ctx context.Context) (*sql.DB, error) {
	psqlInfo := fmt.Sprintf("host=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		os.Getenv("PG_HOST"), os.Getenv("PG_USER"), os.Getenv("PG_USER"), os.Getenv("PG_USER"))

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, fmt.Errorf("failed opening sql conn: %w", err)
	}

	err = db.PingContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed pinging DB: %w", err)
	}

	_, err = db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS payment_events (id text);")
	if err != nil {
		return nil, fmt.Errorf("failed creating paymen events table: %w", err)
	}

	log.Println("Successfully connected to DB!")
	return db, nil
}

func consumePaymentEvents(ctx context.Context, db *sql.DB) error {
	gcpProject := os.Getenv("GCP_PROJECT")
	pubsubSubscription := os.Getenv("PUBSUB_SUBSCRIPTION")

	client, err := pubsub.NewClient(ctx, gcpProject)
	if err != nil {
		return fmt.Errorf("failed creating pubsub client: %w", err)
	}

	sub := client.Subscription(pubsubSubscription)
	log.Printf("started listening to pubsub subscription %v in project %v...", pubsubSubscription, gcpProject)

	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		log.Printf("Got message: %s", m.Data)

		_, err := db.ExecContext(ctx, "INSERT INTO payment_events VALUES (?)", m.ID)
		if err != nil {
			log.Fatal(err)
		}
		m.Ack()
	})
	if err != nil {
		return fmt.Errorf("failed receiving from pubsub: %w", err)
	}

	log.Print("done listening to pubsub")
	return nil
}
