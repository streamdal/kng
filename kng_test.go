package kng

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/segmentio/kafka-go"
)

var _ = Describe("kng", func() {
	var (
		k *Kafka

		ctx          = context.Background()
		kafkaBrokers = []string{"localhost:9092"}
	)

	BeforeEach(func() {
		var err error

		k, err = New(&Options{
			Brokers:                kafkaBrokers,
			ServiceShutdownContext: context.Background(),
			MainShutdownFunc:       func() {},
		})

		Expect(err).ToNot(HaveOccurred())
		Expect(k).ToNot(BeNil())
	})

	Context("CreateTopic", func() {
		It("should work", func() {
			topic := uuid.NewV4().String()

			// Verify that topic does not exist (need to use sarama, since
			// kafka-go doesn't have the functionality
			clusterAdmin, err := sarama.NewClusterAdmin(k.Options.Brokers, sarama.NewConfig())
			Expect(err).ToNot(HaveOccurred())
			Expect(clusterAdmin).ToNot(BeNil())

			topics, err := clusterAdmin.ListTopics()
			Expect(err).ToNot(HaveOccurred())

			_, ok := topics[topic]
			Expect(ok).To(BeFalse())

			// Create topic
			err = k.CreateTopic(ctx, topic)
			Expect(err).ToNot(HaveOccurred())

			// Give kafka a bit of time to catch up
			time.Sleep(time.Second)

			// Verify that the topic now exists
			topics, err = clusterAdmin.ListTopics()
			Expect(err).ToNot(HaveOccurred())

			_, ok = topics[topic]
			Expect(ok).To(BeTrue())
		})
	})

	Context("DeleteTopic", func() {
		It("should work", func() {
			topic := uuid.NewV4().String()

			// Verify that topic does not exist (need to use sarama, since
			// kafka-go doesn't have the functionality
			clusterAdmin, err := sarama.NewClusterAdmin(k.Options.Brokers, sarama.NewConfig())
			Expect(err).ToNot(HaveOccurred())
			Expect(clusterAdmin).ToNot(BeNil())

			topics, err := clusterAdmin.ListTopics()
			Expect(err).ToNot(HaveOccurred())

			_, ok := topics[topic]
			Expect(ok).To(BeFalse())

			// Create a topic
			err = k.CreateTopic(ctx, topic)
			Expect(err).ToNot(HaveOccurred())

			// Give kafka a second to catch up
			time.Sleep(time.Second)

			// Verify topic is created
			topics, err = clusterAdmin.ListTopics()
			Expect(err).ToNot(HaveOccurred())

			_, ok = topics[topic]
			Expect(ok).To(BeTrue())

			// Delete topic
			err = k.DeleteTopic(ctx, topic)
			Expect(err).ToNot(HaveOccurred())

			// Give kafka time to catch up
			time.Sleep(time.Second)

			// Verify its deleted
			topics, err = clusterAdmin.ListTopics()
			Expect(err).ToNot(HaveOccurred())

			_, ok = topics[topic]
			Expect(ok).To(BeFalse())
		})
	})

	Context("Publish", func() {
		It("should work", func() {
			topic := uuid.NewV4().String()
			testData := []byte("testData")

			err := createTopic(k.Options.Brokers, topic)
			Expect(err).ToNot(HaveOccurred())

			k.Publish(ctx, topic, testData)

			// Our publisher batches by default - it may take a few secs to publish
			time.Sleep(5 * time.Second)

			readData, err := read(k.Options.Brokers, topic)
			Expect(err).ToNot(HaveOccurred())
			Expect(readData).To(Equal(testData))
		})
	})

	Context("NewReader.Read", func() {
		It("should work", func() {
			topic := uuid.NewV4().String()
			testData := []byte("testData")

			// Create temp topic
			err := createTopic(k.Options.Brokers, topic)
			Expect(err).ToNot(HaveOccurred())

			// Give kafka a second to catch up
			time.Sleep(time.Second)

			// Write test data
			err = write(k.Options.Brokers, topic, testData)
			Expect(err).ToNot(HaveOccurred())

			// Verify that data is there
			msg, err := k.NewReader("foo", "", topic).Reader.ReadMessage(ctx)

			Expect(err).ToNot(HaveOccurred())
			Expect(msg.Value).To(Equal(testData))
			Expect(msg.Topic).To(Equal(topic))
		})
	})

	Context("buildBatch", func() {
		It("should work", func() {
			perBatch := 100

			cases := map[int]int{
				23929: 240,
				4828:  49,
				1:     1,
				2:     1,
				99:    1,
				100:   1,
				101:   2,
				10000: 100,
			}

			for fullSize, expected := range cases {
				msgs := make([]kafka.Message, 0)

				for i := 0; i < fullSize; i++ {
					msgs = append(msgs, kafka.Message{})
				}

				result := buildBatch(msgs, perBatch)

				Expect(len(result)).To(Equal(expected))
			}
		})
	})
})

func read(brokers []string, topic string) ([]byte, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
	})

	msg, err := reader.ReadMessage(context.Background())
	if err != nil {
		return nil, errors.Wrap(err, "unable to read message from kafkaa")
	}

	return msg.Value, nil
}

func write(brokers []string, topic string, data []byte) error {
	writer := &kafka.Writer{
		Addr:  kafka.TCP(brokers[0]),
		Topic: topic,
	}

	if err := writer.WriteMessages(context.Background(), kafka.Message{
		Value: data,
	}); err != nil {
		return errors.Wrap(err, "unable to write message")
	}

	return nil
}

func createTopic(brokers []string, topic string) error {
	clusterAdmin, err := sarama.NewClusterAdmin(brokers, sarama.NewConfig())
	if err != nil {
		return errors.Wrap(err, "unable to establish cluster admin conn")
	}

	if err := clusterAdmin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false); err != nil {
		return errors.Wrap(err, "unable to create topic")
	}

	// Give kafka enough time to catch up
	time.Sleep(time.Second)

	return nil
}
