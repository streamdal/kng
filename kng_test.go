package kng

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

var (
	log *logrus.Logger
)

func init() {
	log = logrus.New()
	log.SetLevel(logrus.InfoLevel)
}

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
			clusterAdmin, err := sarama.NewClusterAdmin(k.Options.Brokers, newSaramaConfig())
			Expect(err).ToNot(HaveOccurred())
			Expect(clusterAdmin).ToNot(BeNil())

			topics, err := clusterAdmin.ListTopics()
			Expect(err).ToNot(HaveOccurred())

			_, ok := topics[topic]
			Expect(ok).To(BeFalse())

			// Create topic``
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
			clusterAdmin, err := sarama.NewClusterAdmin(k.Options.Brokers, newSaramaConfig())
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

	Context("New", func() {
		It("should error whe no username present", func() {
			_, err := New(&Options{
				Brokers:                kafkaBrokers,
				ServiceShutdownContext: context.Background(),
				MainShutdownFunc:       func() {},
				SaslType:               "plain",
				Username:               "",
				Password:               "",
			})

			Expect(err).To(Equal(ErrMissingUsername))
		})

		It("should error whe no username present", func() {
			_, err := New(&Options{
				Brokers:                kafkaBrokers,
				ServiceShutdownContext: context.Background(),
				MainShutdownFunc:       func() {},
				SaslType:               "plain",
				Username:               "testing",
				Password:               "",
			})

			Expect(err).To(Equal(ErrMissingPassword))
		})
	})

	Context("GetConsumerGroupOffsets", func() {
		It("should return correct offsets", func() {
			rand.Seed(time.Now().UnixNano())

			topic := fmt.Sprintf("GetConsumerGroupOffsetsTest-%d", rand.Intn(100_000)+1)
			cg := topic + "-cg"
			llog := log.WithField("context", "GetConsumerGroupOffsets")

			llog.Infof("Creating topic '%s'", topic)

			// Create a topic with 4 partitions (4 to ensure that our offset map
			// partition traversal logic is working properly)
			err := createTopic(kafkaBrokers, topic, 4)
			Expect(err).To(BeNil())

			defer func() {
				if err := cleanupTopic(kafkaBrokers, topic); err != nil {
					llog.Info("Cleanup topic error: ", err)
					os.Exit(2)
				}
			}()

			// Give kafka a moment to catch up with createTopic
			time.Sleep(time.Second)

			llog.Info("Publishing messages")

			// Write 100 messages
			for i := 0; i < 100; i++ {
				k.Publish(context.Background(), topic, []byte(fmt.Sprintf("test-%d", i)))
			}

			// Give kng a moment to complete the publishes
			time.Sleep(5 * time.Second)

			llog.Info("Creating a consumer group")

			// Create consumer group
			_, err = k.CreateConsumerGroup(context.Background(), topic, cg)
			Expect(err).To(BeNil())

			// Give consumer group creation a sec to complete
			time.Sleep(time.Second)

			// Ideally, we would want to clean up the created consumer group(s)
			// but consumer group deletion in Kafka is a potentially complex and
			// involved process - need to determine if cg is actively used ->
			// get member group instance id's -> delete members -> delete cg.
			// Also, group instance ID's are not supported in sarama which is
			// what sarama's RemoveMemberFromConsumerGroup() expects. In other
			// words - not worth it. ~DS 12.20.22

			// Read 100 messages in topic
			reader := k.NewReader("GetConsumerGroupOffsets-reader", cg, topic)
			Expect(reader).ToNot(BeNil())

			readerOffsets := make(map[int]int64)
			var readCount int

			// Kafka _initial_ reads can take a really long time to complete but
			// we should bail if takes more than a few minutes.
			ctx, _ := context.WithTimeout(context.Background(), 5*time.Minute)

			llog.Info("Reading messages")

			for {
				if readCount == 100 {
					break
				}

				msg, err := reader.Reader.ReadMessage(ctx)
				Expect(err).To(BeNil())

				// Record latest offsets for each partition
				if _, ok := readerOffsets[msg.Partition]; !ok {
					readerOffsets[msg.Partition] = msg.Offset
				} else {
					if msg.Offset > readerOffsets[msg.Partition] {
						readerOffsets[msg.Partition] = msg.Offset
					}
				}

				readCount += 1
			}

			for k, v := range readerOffsets {
				llog.Infof("ReaderOffsets: partition: %d offset: %d", k, v)
			}

			// Close reader (to be polite and allow potential cg removal)
			err = reader.Reader.Close()
			Expect(err).To(BeNil(), "should be able to close reader when done")

			llog.Info("Getting consumer group offsets")

			offsets, err := k.GetConsumerGroupOffsets(context.Background(), topic, cg)
			Expect(err).To(BeNil())
			Expect(offsets).ToNot(BeNil())

			for k, v := range offsets {
				llog.Infof("GetConsumerOffsets: partition: %d offset: %d", k, v)
			}

			// Compare GetConsumerGroupOffsets() result with our readOffset map
			for readPartition, readOffset := range readerOffsets {
				_, ok := offsets[readPartition]
				Expect(ok).To(BeTrue())

				// We need to +1 to get the "NextOffset"
				Expect(readOffset+1).To(Equal(offsets[readPartition]), "read offsets")
			}
		})
	})

	Context("SetConsumerGroupOffsets", func() {
		It("should set consumer group offsets", func() {
			llog := log.WithField("context", "SetConsumerGroupOffsets")

			// Create a topic
			rand.Seed(time.Now().UnixNano())

			topic := fmt.Sprintf("SetConsumerGroupOffsetsTest-%d", rand.Intn(100_000)+1)
			cgOld := topic + "-cg-old"
			cgNew := topic + "-cg-new"

			llog.Infof("Creating topic '%s'", topic)

			err := createTopic(kafkaBrokers, topic)
			Expect(err).To(BeNil())

			defer func() {
				if err := cleanupTopic(kafkaBrokers, topic); err != nil {
					llog.Error("Cleanup topic error: ", err)
					os.Exit(2)
				}
			}()

			// Give kafka a moment to catch up with createTopic
			time.Sleep(time.Second)

			// Create consumer group 1 ("old" cg)
			llog.Infof("Creating consumer group '%s'", cgOld)

			_, err = k.CreateConsumerGroup(context.Background(), topic, cgOld)
			Expect(err).To(BeNil())

			publishCount := 200

			llog.Infof("Publishing '%d' messages to topic '%s'", publishCount, topic)

			// Write 200 messages to topic
			for i := 0; i < publishCount; i++ {
				k.Publish(context.Background(), topic, []byte(fmt.Sprintf("old-%d", i)))
			}

			// Give kng a moment to complete publish
			time.Sleep(time.Second)

			// Consume 100 messages with cgOld
			readCount := 100

			readerOld := k.NewReader("SetConsumerGroupOffsets-reader-old", cgOld, topic)
			Expect(readerOld).ToNot(BeNil())

			llog.Infof("Reading %d messages from topic '%s' using consumer group '%s'", readCount, topic, cgOld)

			for i := 0; i < readCount; i++ {
				msg, err := readerOld.Reader.FetchMessage(ctx)
				Expect(err).To(BeNil(), "ReadMessage should not error")

				err = readerOld.Reader.CommitMessages(context.Background(), msg)
				Expect(err).To(BeNil())
			}

			// Close reader
			err = readerOld.Reader.Close()
			Expect(err).To(BeNil(), "close should not error")

			llog.Infof("Getting latest offsets for consumer group '%s'", cgOld)

			// Get offsets for cgOld
			ctx, _ = context.WithTimeout(context.Background(), time.Minute)

			offsetsOld, err := k.GetConsumerGroupOffsets(ctx, topic, cgOld)
			Expect(err).To(BeNil())
			Expect(offsetsOld).ToNot(BeNil())

			for partition, offset := range offsetsOld {
				llog.Infof("cgOld: partition '%d' offset '%d'", partition, offset)
			}

			llog.Infof("Creating & updating consumer group '%s' with offsets from consumer group '%s'", cgNew, cgOld)

			// Update cgNew offsets with cgOld offsets
			ctx, _ = context.WithTimeout(context.Background(), time.Minute)
			err = k.SetConsumerGroupOffsets(ctx, topic, cgNew, offsetsOld)
			Expect(err).To(BeNil())

			// JIC, give kafka a moment to commit new offset
			time.Sleep(5 * time.Second)

			llog.Infof("Fetching offsets for consumer group '%s'", cgNew)

			// GetNextOffsets should return same offset info as cgOld
			offsetsNew, err := k.GetConsumerGroupOffsets(context.Background(), topic, cgNew)
			Expect(err).To(BeNil())
			Expect(offsetsNew).ToNot(BeNil())

			for partition, offset := range offsetsNew {
				llog.Infof("cgNew: partition '%d' offset '%d'", partition, offset)
			}

			// Offsets should be the same
			Expect(offsetsNew).To(Equal(offsetsOld))
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

// Sarama defaults to an old kafka API version which does not have support for
// newer features (like create/delete consumer groups). This helper forces
// sarama to use a newer API version.
func newSaramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_6_0_0

	return cfg
}

func cleanupTopic(brokers []string, topic string) error {
	clusterAdmin, err := sarama.NewClusterAdmin(brokers, newSaramaConfig())
	if err != nil {
		return errors.Wrap(err, "unable to establish cluster admin conn")
	}

	if err := clusterAdmin.DeleteTopic(topic); err != nil {
		return errors.Wrapf(err, "unable to delete topic '%s'", topic)
	}

	return nil
}

func createTopic(brokers []string, topic string, numPartitions ...int) error {
	clusterAdmin, err := sarama.NewClusterAdmin(brokers, newSaramaConfig())
	if err != nil {
		return errors.Wrap(err, "unable to establish cluster admin conn")
	}

	topicCfg := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	if len(numPartitions) > 0 {
		topicCfg.NumPartitions = int32(numPartitions[0])
	}

	if err := clusterAdmin.CreateTopic(topic, topicCfg, false); err != nil {
		return errors.Wrap(err, "unable to create topic")
	}

	// Give kafka enough time to catch up
	time.Sleep(time.Second)

	return nil
}
