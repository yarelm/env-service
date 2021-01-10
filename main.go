package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/pubsub"
)

func main() {
	log.Print("starting server...")
	http.HandleFunc("/", handler)

	// Determine port for HTTP service.
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
		log.Printf("defaulting to port %s", port)
	}

	go consume()

	// Start HTTP server.
	log.Printf("listening on port %s", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatal(err)
	}
}

func consume() {
	gcpProject := os.Getenv("GCP_PROJECT")
	pubsubSubscription := os.Getenv("PUBSUB_SUBSCRIPTION")
	pubsubTopic := os.Getenv("PUBSUB_TOPIC")

	client, err := pubsub.NewClient(context.Background(), gcpProject)
	if err != nil {
		log.Fatal(err)
	}

	topic, err := client.CreateTopic(context.Background(), pubsubTopic)
	if err != nil {
		log.Fatal(err)
	}

	sub, err := client.CreateSubscription(context.Background(), pubsubSubscription,
		pubsub.SubscriptionConfig{Topic: topic})
	if err != nil {
		log.Fatal(err)
	}

	log.Print("started listening to pubsub...")

	err = sub.Receive(context.Background(), func(ctx context.Context, m *pubsub.Message) {
		log.Printf("Got message: %s", m.Data)
		m.Ack()
	})
	if err != nil {
		log.Fatal(err)
	}
}

func handler(w http.ResponseWriter, r *http.Request) {
	name := os.Getenv("NAME")
	if name == "" {
		name = "World"
	}
	fmt.Fprintf(w, "Hello %s!\n", name)
}
