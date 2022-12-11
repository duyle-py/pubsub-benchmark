package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	kafka_go "github.com/segmentio/kafka-go"
)

const NUM_MSG = 1000000
const MSG_SIZE = 1e4
const BROKER = "127.0.0.1:9092"

var MSG = shortID(MSG_SIZE)

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
	num_worker := 10
	var wg sync.WaitGroup

	for w := 0; w < num_worker; w++ {
		wg.Add(1)

		go func() {
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
			for i := 0; i < NUM_MSG/num_worker; i++ {
				//Retries this message
				for {
					err := producer.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: &k.TopicID},
						Value:          []byte(MSG),
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
				log.Print("Still waiting to flush outstanding messages\n")
			}
			wg.Done()

		}()
	}
	wg.Wait()

}

func (k *ConfluentKafka) Consume() {
	num_worker := 10
	var wg sync.WaitGroup

	for w := 0; w < num_worker; w++ {
		wg.Add(1)

		go func() {
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
				if totalMsg == NUM_MSG/num_worker {
					break
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

type GoKafka struct {
	TopicID string
}

func (k *GoKafka) Produce() {
	fmt.Println("Running producer for topic ", k.TopicID)

	num_worker := 10000
	var wg sync.WaitGroup

	producer := kafka_go.Writer{
		Addr:                   kafka_go.TCP(BROKER),
		Topic:                  k.TopicID,
		Balancer:               &kafka_go.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
	for w := 0; w < num_worker; w++ {
		wg.Add(1)

		go func() {
			for i := 0; i < NUM_MSG/num_worker; i++ {
				for {
					err := producer.WriteMessages(context.Background(),
						kafka_go.Message{
							Value: []byte(MSG),
						})
					if err == nil {
						break
					}
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func (k *GoKafka) Consume() {
	// make a new reader that consumes from topic-A

	num_worker := 1000
	var wg sync.WaitGroup

	for w := 0; w < num_worker; w++ {
		consumer := kafka_go.NewReader(kafka_go.ReaderConfig{
			Brokers:  []string{BROKER},
			GroupID:  shortID(5),
			Topic:    k.TopicID,
			MinBytes: 1e3,  // 1KB
			MaxBytes: 10e6, // 10MB
		})
		wg.Add(1)

		go func() {
			totalMsg := 0
			ctx := context.Background()

			for {
				for {
					m, err := consumer.FetchMessage(ctx)
					if err != nil {
						continue
					}
					if err := consumer.CommitMessages(ctx, m); err == nil {
						totalMsg++
						break
					}
				}
				if totalMsg == NUM_MSG/num_worker {
					break
				}
			}
			consumer.Close()
			wg.Done()

		}()
	}
	wg.Wait()
}

func calc_time(f func()) {
	start := time.Now()
	f()
	end := time.Since(start)
	fmt.Printf("%d msg/sec and %f mb/sec\n", NUM_MSG/int(end.Seconds()), NUM_MSG*MSG_SIZE/1024.0/1024/float64(end.Seconds()))
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

	fmt.Println("Confluent Kafka")
	confluent := ConfluentKafka{TopicID: shortID(10)}
	fmt.Print("PRODUCER: ")
	calc_time(confluent.Produce)
	fmt.Print("CONSUMER: ")
	calc_time(confluent.Consume)

	fmt.Println("=================")
	fmt.Println("kafka go")
	kafkaGo := GoKafka{TopicID: shortID(10)}
	fmt.Print("PRODUCER: ")
	calc_time(kafkaGo.Produce)
	fmt.Print("CONSUMER: ")
	calc_time(kafkaGo.Consume)
}
