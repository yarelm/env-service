package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/pubsub"
	_ "github.com/lib/pq"
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		log.Printf("got signal: %v", sig)
		cancel()
		close(done)
	}()

	go func() {
		// Start HTTP server.
		log.Printf("listening on port %s", port)
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Fatal(err)
		}
	}()

	validateDbConnectivity(ctx)
	consume(ctx)
	<-done
	fmt.Printf("bye!")
}

func validateDbConnectivity(ctx context.Context) {
	psqlInfo := fmt.Sprintf("host=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		os.Getenv("PG_HOST"), os.Getenv("PG_USER"), os.Getenv("PG_USER"), os.Getenv("PG_USER"))

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.PingContext(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Successfully connected to DB!")
}

func consume(ctx context.Context) {
	gcpProject := os.Getenv("GCP_PROJECT")
	pubsubSubscription := os.Getenv("PUBSUB_SUBSCRIPTION")

	log.Println(gcpProject, pubsubSubscription)

	client, err := pubsub.NewClient(ctx, gcpProject)
	if err != nil {
		log.Fatal(err)
	}

	sub := client.Subscription(pubsubSubscription)
	perms, err := sub.IAM().TestPermissions(ctx, []string{
		"pubsub.subscriptions.consume",
	})
	if err != nil {
		log.Fatal(err)
	}
	for _, perm := range perms {
		log.Printf("Allowed: %v\n", perm)
	}

	log.Print("started listening to pubsub...")

	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		log.Printf("Got message: %s", m.Data)
		m.Ack()
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Print("done listening to pubsub")
}

func handler(w http.ResponseWriter, r *http.Request) {
	name := os.Getenv("NAME")
	if name == "" {
		name = "World"
	}
	fmt.Fprintf(w, "Hello %s!\n", name)
}
