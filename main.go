package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

const (
	projectID = "lunar-planet-282612"
	topicID = "my-topic"
	subsID = "my-subs"
)

func publish(topic string) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID, option.WithCredentialsFile("credentials.json"))
	if err != nil {
			return fmt.Errorf("pubsub.NewClient: %v", err)
	}
	defer client.Close()

	t := client.Topic(topic)
	t.EnableMessageOrdering = true
	for i := 0; i < 10; i++ {
		msg := "this is message " + strconv.Itoa(i)
		result := t.Publish(ctx, &pubsub.Message{
			Data: []byte(msg),
			OrderingKey: strconv.Itoa(i),
		})
		// Block until the result is returned and a server-generated
		// ID is returned for the published message.
		id, err := result.Get(ctx)
		if err != nil {
				return fmt.Errorf("Get: %v", err)
		}
		fmt.Printf("Published message %d; msg ID: %v\n", i, id)

		// Sleep 3 seconds
		time.Sleep(3 * time.Second)
	}
	return nil
}

func pullMsgs(subscription, instanceName string) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID, option.WithCredentialsFile("credentials.json"))
	if err != nil {
			return fmt.Errorf("pubsub.NewClient: %v", err)
	}
	defer client.Close()

	sub := client.Subscription(subscription)

	// Receive messages for 10 seconds, which simplifies testing.
	// Comment this out in production, since `Receive` should
	// be used as a long running operation.
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	var received int32
	err = sub.Receive(ctx, func(_ context.Context, msg *pubsub.Message) {
			fmt.Printf("Instance %s, Got message: %q\n",instanceName, string(msg.Data))
			atomic.AddInt32(&received, 1)
			msg.Ack()
	})
	if err != nil {
			return fmt.Errorf("sub.Receive: %v", err)
	}
	fmt.Printf("Received %d messages\n", received)

	return nil
}

func main() {
	var worker, topic, subs, name string
	flag.StringVar(&worker, "worker", "consumer", "Worker process, producer or consumer")
	flag.StringVar(&topic, "topic", topicID, "Topic name")
	flag.StringVar(&subs, "subs", subsID, "Subscription name")
	flag.StringVar(&name, "name", "1", "Instance name")
	flag.Parse()
	var err error
	if worker == "producer" {
		err = publish(topic)
	} else {
		err = pullMsgs(subs, name)
	}
	if err != nil {
		fmt.Println(err)
	}
}
