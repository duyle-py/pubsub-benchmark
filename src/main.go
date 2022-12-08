package main

import (
	"crypto/rand"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

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

const NUM_MSG = 1000000
const MSG_SIZE = 1e4

var topic = shortID(10)

func Producer() {
	start := time.Now()
	msg := shortID(MSG_SIZE)

	var wg sync.WaitGroup

	NUM_WORKER := 1
	for w := 0; w < NUM_WORKER; w++ {
		wg.Add(1)

		go func() {
			producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})

			if err != nil {
				log.Fatal(err)
			}
			go func() {
				for _ = range producer.Events() {
				}
			}()

			for i := 0; i < NUM_MSG; i++ {
				for {
					err := producer.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: &topic},
						Value:          []byte(msg),
					}, nil)

					if err == nil {
						break
					} else {
						continue
					}
				}
			}

			for producer.Flush(5000) > 0 {
				fmt.Print("Still waiting to flush outstanding messages\n")
			}

			wg.Done()
		}()
	}
	wg.Wait()

	elapsed := time.Since(start)

	log.Printf("%d msg/sec and %d mb/sec", NUM_MSG*NUM_WORKER/int(elapsed.Seconds()), NUM_MSG*NUM_WORKER*MSG_SIZE/1024/1024/int(elapsed.Seconds()))
}

func Consumer() {
	// totalReturnEvents := 0

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        "localhost:9092",
		"group.id":                 shortID(3),
		"auto.offset.reset":        "smallest",
		"enable.auto.offset.store": false,
	})
	start := time.Now()

	if err != nil {
		log.Println(err)
	}

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Println(err)
	}
	totalMsg := 0
	for {
		ev := c.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			_, err := c.StoreMessage(e)
			if err == nil {
				totalMsg++
			}
		}
		if totalMsg == NUM_MSG {
			break
		}
	}

	c.Close()
	elapsed := time.Since(start)

	log.Printf("%d msg/sec and %f mb/sec", NUM_MSG/int(elapsed.Seconds()), NUM_MSG*MSG_SIZE/1024.0/1024/float64(elapsed.Seconds()))

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

	Producer()

	Consumer()

}
