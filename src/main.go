package main

import (
	"crypto/rand"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const NUM_MSG = 1000000
const MSG_SIZE = 1e4
const BROKER = "localhost:9092"

var chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"

func shortID(length int) string {
	ll := len(chars)
	b := make([]byte, length)
	rand.Read(b) // generates len(b) random bytes
	for i := 0; i < length; i++ {
		b[i] = chars[int(b[i])%ll]
	}
	return string(b)
}

type ConfluentKafka struct {
	TopicID string
}

func (k *ConfluentKafka) Produce() {
	//Init topic
	k.TopicID = shortID(10)
	msg := shortID(MSG_SIZE)

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": BROKER})

	if err != nil {
		log.Fatal("Can't connect to brokers", err)
	}

	// Async event handler
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Println("Errors in TOPIC partition: ", ev)
				}

			case kafka.Error:
				fmt.Println("Errors: ", ev)
			default:
				fmt.Println("Ignore events")
			}
		}
	}()

	//Async producer
	for i := 0; i < NUM_MSG; i++ {
		//Retries this message
		for {
			err := producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &k.TopicID},
				Value:          []byte(msg),
			}, nil)

			if err != nil {
				if err.(kafka.Error).Code() == kafka.ErrQueueFull {
					continue
				}
			} else {
				break
			}
		}
	}

	for producer.Flush(5000) > 0 {
		fmt.Print("Still waiting to flush outstanding messages\n")
	}
}

func (k *ConfluentKafka) Consume() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        BROKER,
		"group.id":                 shortID(3), //random group
		"auto.offset.reset":        "smallest",
		"enable.auto.offset.store": false,
	})
	if err != nil {
		log.Fatal("Can't connect to brokers", err)
	}

	err = c.SubscribeTopics([]string{k.TopicID}, nil)
	if err != nil {
		log.Fatal("Can't subcribe", err)
	}

	totalMsg := 0
	for {
		ev := c.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			//Retries inf
			for {
				_, err := c.StoreMessage(e)
				if err == nil {
					totalMsg++
					break
				}
			}

		default:
			continue
		}
		if totalMsg == NUM_MSG {
			break
		}
	}
}

func calc_time(f func()) {
	start := time.Now()
	f()
	end := time.Since(start)
	log.Printf("%d msg/sec and %f mb/sec", NUM_MSG/int(end.Seconds()), NUM_MSG*MSG_SIZE/1024.0/1024/float64(end.Seconds()))
}

func main() {
	//Connect to the kafka
	for {
		conn, err := net.Dial("tcp", "localhost:9092")
		if err == nil && conn != nil {
			conn.Close()
			break
		}
		fmt.Println("wait for kafka")
		//Some error like
		// %6|1670379905.979|FAIL|rdkafka#producer-1| [thrd:localhost:9092/bootstrap]: localhost:9092/bootstrap: Disconnected while requesting ApiVersion: might be caused by incorrect security.protocol configuration (connecting to a SSL listener?) or broker version is < 0.10 (see api.version.request) (after 0ms in state APIVERSION_QUERY, 1 identical error(s) suppressed)

		time.Sleep(20 * time.Second)
	}

	confluent := ConfluentKafka{TopicID: shortID(10)}
	fmt.Print("PRODUCER: ")
	calc_time(confluent.Produce)
	fmt.Print("CONSUMER: ")
	calc_time(confluent.Consume)
}
