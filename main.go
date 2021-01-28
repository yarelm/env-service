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

	db := connectDb(ctx)
	defer db.Close()

	consumeMessages(ctx, db)
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

func connectDb(ctx context.Context) *sql.DB {
	psqlInfo := fmt.Sprintf("host=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		os.Getenv("PG_HOST"), os.Getenv("PG_USER"), os.Getenv("PG_USER"), os.Getenv("PG_USER"))

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}

	err = db.PingContext(ctx)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Successfully connected to DB!")
	return db
}

func consumeMessages(ctx context.Context, db *sql.DB) {
	gcpProject := os.Getenv("GCP_PROJECT")
	pubsubSubscription := os.Getenv("PUBSUB_SUBSCRIPTION")

	log.Println(gcpProject, pubsubSubscription)

	client, err := pubsub.NewClient(ctx, gcpProject)
	if err != nil {
		log.Fatal(err)
	}

	sub := client.Subscription(pubsubSubscription)
	log.Printf("started listening to pubsub subscription %v...", pubsubSubscription)

	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		log.Printf("Got message: %s", m.Data)

		// TODO: write to DB
		m.Ack()
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Print("done listening to pubsub")
}
