package kng

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	uuid "github.com/satori/go.uuid"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/sirupsen/logrus"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

const (
	DefaultReplicationFactor     = 2
	DefaultNumPartitionsPerTopic = 20
	DefaultBatchSize             = 10
	DefaultConnectTimeout        = 10 * time.Second
	PublishInterval              = 100 * time.Millisecond
	PublishShutdownInterval      = time.Second
	DefaultWorkerIdleTimeout     = time.Minute
	DefaultReaderMaxWait         = 5 * time.Second
	DefaultSubBatchSize          = 1000
	DefaultMaxPublishRetries     = 3
	DefaultPublishRetryInterval  = time.Second * 3
	DefaultRequiredWriteAcks     = kafka.RequireNone
)

var (
	ErrMissingUsername = errors.New("missing username for SASL authentication")
	ErrMissingPassword = errors.New("missing password for SASL authentication")
)

type IKafka interface {
	NewReader(id, groupID, topic string) *Reader
	Publish(ctx context.Context, topic string, value []byte)
	DeletePublisher(ctx context.Context, topic string) bool
	DeleteTopic(ctx context.Context, topic string) error
	CreateTopic(ctx context.Context, topic string) error
	GetNextOffset(ctx context.Context, topic, consumerGroup string) (int64, error)
	CreateConsumerGroup(ctx context.Context, topic, consumerName string, startOffset int64) error
	DeleteConsumerGroup(ctx context.Context, consumerName string) error
}

type IReader interface {
	Read(ctx context.Context) (kafka.Message, error)
}

type Kafka struct {
	Dialer             *kafka.Dialer
	Writer             *kafka.Writer
	PublisherMap       map[string]*Publisher
	PublisherMutex     *sync.RWMutex
	Options            *Options
	initialBrokerCheck bool
	log                *logrus.Entry

	// ServiceShutdownContext is used by main() to shutdown services before application termination
	ServiceShutdownContext context.Context

	// MainShutdownFunc is triggered by watchForShutdown() after all publisher queues are exhausted
	// and is used to trigger shutdown of APIs and then main()
	MainShutdownFunc context.CancelFunc
}

type Publisher struct {
	ID          string
	Topic       string
	Writer      *kafka.Writer
	Looper      director.Looper
	Queue       []kafka.Message
	QueueMutex  *sync.RWMutex
	IdleTimeout time.Duration
	Kafka       *Kafka

	// PublisherContext is used to close a specific publisher
	PublisherContext context.Context

	// PublisherCancel is used to cancel a specific publisher's context
	PublisherCancel context.CancelFunc

	// ServiceShutdownContext is used by main() to shutdown services before application termination
	ServiceShutdownContext context.Context

	log *logrus.Entry
}

type Reader struct {
	ID     string
	Reader *kafka.Reader
	log    *logrus.Entry
}

type Options struct {
	Brokers           []string
	Timeout           time.Duration
	BatchSize         int
	UseTLS            bool
	WorkerIdleTimeout time.Duration

	// Authentication
	SaslType string
	Username string
	Password string

	// Consumer specific settings
	ReaderMaxWait        time.Duration
	ReaderUseLastOffset  bool
	ReaderUseFirstOffset bool

	// Producer specific settings
	NumPartitionsPerTopic int
	ReplicationFactor     int
	NumPublishRetries     int

	// PublishRetryInterval determines how long to wait between publish failures before it will try again
	PublishRetryInterval time.Duration

	// ServiceShutdownContext is used by main() to shutdown services before application termination
	ServiceShutdownContext context.Context

	// MainShutdownFunc is triggered by watchForShutdown() after all publisher queues are exhausted
	// and is used to trigger shutdown of APIs and then main()
	MainShutdownFunc context.CancelFunc

	// The level of required acknowledgements to ask the kafka broker for.
	// More acks == more reliable but slower. Default: None
	RequiredWriteAcks kafka.RequiredAcks

	EnableKafkaGoLogs bool
}

// New is used for instantiating the library.
func New(opts *Options) (*Kafka, error) {
	if err := validateOptions(opts); err != nil {
		return nil, errors.Wrap(err, "invalid options")
	}

	dialer := &kafka.Dialer{
		Timeout: opts.Timeout,
	}

	if opts.UseTLS {
		dialer.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	// SASL Authentication
	if opts.SaslType != "" {
		if opts.Username == "" {
			return nil, ErrMissingUsername
		}
		if opts.Password == "" {
			return nil, ErrMissingPassword
		}

		switch opts.SaslType {
		case "scram":
			mechanism, err := scram.Mechanism(scram.SHA512, opts.Username, opts.Password)
			if err != nil {
				return nil, errors.Wrap(err, "unable to initiate scram authentication")
			}
			dialer.SASLMechanism = mechanism
		default:
			dialer.SASLMechanism = plain.Mechanism{
				Username: opts.Username,
				Password: opts.Password,
			}
		}
	}

	ctxWithTimeout, _ := context.WithTimeout(context.Background(), opts.Timeout)

	_, err := dialKafka(ctxWithTimeout, dialer, opts.Brokers)
	if err != nil {
		return nil, fmt.Errorf("unable to create initial connection to broker(s): %s", err)
	}

	transport := &kafka.Transport{
		DialTimeout: opts.Timeout,
		// Bug? TLS has to be specified here; TLS on dialer doesn't work.
		TLS:  dialer.TLS,
		SASL: dialer.SASLMechanism,
	}

	llog := logrus.WithField("pkg", "kafka")

	k := &Kafka{
		Writer: &kafka.Writer{
			Addr:         kafka.TCP(opts.Brokers...),
			BatchSize:    opts.BatchSize,
			Transport:    transport,
			RequiredAcks: opts.RequiredWriteAcks,
		},
		Dialer:                 dialer,
		PublisherMutex:         &sync.RWMutex{},
		PublisherMap:           make(map[string]*Publisher),
		Options:                opts,
		ServiceShutdownContext: opts.ServiceShutdownContext,
		MainShutdownFunc:       opts.MainShutdownFunc,
		log:                    llog,
	}

	if k.Options.EnableKafkaGoLogs {
		k.Writer.Logger = kafka.LoggerFunc(llog.Infof)
		k.Writer.ErrorLogger = kafka.LoggerFunc(llog.Errorf)
	}

	if k.Options.ReplicationFactor == 0 {
		k.Options.ReplicationFactor = DefaultReplicationFactor
	}

	if k.Options.NumPartitionsPerTopic == 0 {
		k.Options.NumPartitionsPerTopic = DefaultNumPartitionsPerTopic
	}

	if k.Options.NumPublishRetries == 0 {
		k.Options.NumPublishRetries = DefaultMaxPublishRetries
	}

	if k.Options.PublishRetryInterval == 0 {
		k.Options.PublishRetryInterval = DefaultPublishRetryInterval
	}

	// This goroutine waits for service cancel context to trigger and then loops until all publishers have pushed
	// their batch before triggering MainShutdownFunc()
	go k.watchForShutdown()

	return k, nil
}

// NewReader creates a new reader instance.
func (k *Kafka) NewReader(id, groupID, topic string) *Reader {
	llog := logrus.WithField("readerID", id)

	readerConfig := kafka.ReaderConfig{
		Brokers: k.Options.Brokers,
		GroupID: groupID,
		Topic:   topic,
		MaxWait: k.Options.ReaderMaxWait,
		Dialer:  k.Dialer,
	}

	if k.Options.EnableKafkaGoLogs {
		k.Writer.Logger = kafka.LoggerFunc(llog.Infof)
		k.Writer.ErrorLogger = kafka.LoggerFunc(llog.Errorf)
	}

	if k.Options.ReaderUseLastOffset {
		readerConfig.StartOffset = kafka.LastOffset
	} else if k.Options.ReaderUseFirstOffset {
		readerConfig.StartOffset = kafka.FirstOffset
	}

	return &Reader{
		ID:     id,
		Reader: kafka.NewReader(readerConfig),
		log:    llog,
	}
}

// Publish provides a simple interface for performing batch writes to Kafka.
//
//   - It will automatically create a dedicated publisher for the given topic IF
//     a publisher does not already exist.
//   - It will start a background publisher in a goroutine that will clear its
//     queue on an interval defined by PublishInterval const.
//   - The publisher goroutine will be stopped if it is idle for longer than
//     WorkerIdleTimeout.
//   - To avoid kafka from rejecting a batch containing too many messages, the
//     publisher will automatically divide the batch into "sub-batches" (whose
//     size is defined by DefaultSubBatchSize.
//
// NOTE: The internal queue for the publisher is unbounded which means that the
// longer the flush interval, the more memory the collector will consume.
func (k *Kafka) Publish(ctx context.Context, topic string, value []byte) {
	span, ctx := tracer.StartSpanFromContext(ctx, "kafka.Publish")
	defer span.Finish()

	k.getPublisherByTopic(topic).batch(ctx, value)
}

func (k *Kafka) getSaramaConfig() *sarama.Config {
	cfg := sarama.NewConfig()

	if k.Options.UseTLS {
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	if k.Options.SaslType != "" {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.User = k.Options.Username
		cfg.Net.SASL.Password = k.Options.Password
		if k.Options.SaslType == "scram" {
			cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		} else {
			cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		}
	}

	return cfg
}

func (k *Kafka) GetNextOffset(ctx context.Context, topic, consumerGroup string) (int64, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "kafka.GetNextOffset")
	defer span.Finish()

	c, err := sarama.NewClient(k.Options.Brokers, k.getSaramaConfig())
	if err != nil {
		return 0, errors.Wrap(err, "unable to create kafka client")
	}

	om, err := sarama.NewOffsetManagerFromClient(consumerGroup, c)
	if err != nil {
		return 0, errors.Wrap(err, "unable to create offset manager client")
	}

	partitions, err := c.Partitions(topic)
	if err != nil {
		return 0, errors.Wrap(err, "unable to list partitions")
	}

	var nextOffset int64 = 0

	for _, partition := range partitions {
		p, err := om.ManagePartition(topic, partition)
		if err != nil {
			return 0, errors.Wrap(err, "unable to get partition manager")
		}

		res, _ := p.NextOffset()

		if res > nextOffset {
			nextOffset = res
		}
	}

	return nextOffset, nil
}

func (k *Kafka) CreateConsumerGroup(ctx context.Context, topic, consumerName string, startOffset int64) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "kafka.CreateConsumerGroup")
	defer span.Finish()

	// TODO: figure these options out
	cfg := kafka.ConsumerGroupConfig{
		ID:                    consumerName,
		Brokers:               k.Options.Brokers,
		Dialer:                k.Dialer,
		Topics:                []string{topic},
		WatchPartitionChanges: true,
		GroupBalancers:        nil,
		RetentionTime:         -1,
		StartOffset:           startOffset,
		Timeout:               time.Second * 10,
		//SessionTimeout:         0,
		//RebalanceTimeout:       0,
		//JoinGroupBackoff:       0,
		//HeartbeatInterval:      0,
		//PartitionWatchInterval: 0,
	}

	if k.Options.EnableKafkaGoLogs {
		cfg.Logger = kafka.LoggerFunc(k.log.Infof)
		cfg.ErrorLogger = kafka.LoggerFunc(k.log.Errorf)
	}

	_, err := kafka.NewConsumerGroup(cfg)
	if err != nil {
		err = errors.Wrapf(err, "unable to create consumer group '%s' for topic '%s'", consumerName, topic)
		span.SetTag("error", err)
		return err
	}

	return nil
}

func (k *Kafka) CreateTopic(ctx context.Context, topic string) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "kafka.CreateTopic")
	defer span.Finish()

	clusterAdmin, err := sarama.NewClusterAdmin(k.Options.Brokers, k.getSaramaConfig())
	if err != nil {
		err = errors.Wrap(err, "could not open new connection to kafka")
		span.SetTag("error", err)
		return err
	}

	if err := k.sanityCheckPartitions(clusterAdmin); err != nil {
		span.SetTag("error", err)
		return err
	}

	opts := &sarama.TopicDetail{
		NumPartitions:     int32(k.Options.NumPartitionsPerTopic),
		ReplicationFactor: int16(k.Options.ReplicationFactor),
	}

	if err := clusterAdmin.CreateTopic(topic, opts, false); err != nil {
		err = errors.Wrap(err, "unable to create kafka topic")
		span.SetTag("error", err)
		return err
	}

	return nil
}

// sanityCheckPartitions overrides replica and partition configs when running with only one broker, aka local docker
func (k *Kafka) sanityCheckPartitions(clusterAdmin sarama.ClusterAdmin) error {
	// Only perform this check once in attempt to avoid "Request exceeded the user-specified time limit in the request"
	// error when creating a large amount of topics at once. This error is caused by frequent metadata requests slowing
	// things down.
	if k.initialBrokerCheck {
		return nil
	}

	brokers, _, err := clusterAdmin.DescribeCluster()
	if err != nil {
		return errors.Wrap(err, "could not get broker list")
	}

	// If local, we do not want to overload kafka - use sensible settings
	if len(brokers) == 1 {
		k.Options.ReplicationFactor = 1
		k.Options.NumPartitionsPerTopic = 1
	}

	k.initialBrokerCheck = true

	return nil
}

func (k *Kafka) DeleteConsumerGroup(ctx context.Context, consumerName string) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "kafka.DeleteConsumerGroup")
	defer span.Finish()

	clusterAdmin, err := sarama.NewClusterAdmin(k.Options.Brokers, k.getSaramaConfig())
	if err != nil {
		err = errors.Wrap(err, "could not open new connection to kafka")
		span.SetTag("error", err)
		return err
	}

	if err := clusterAdmin.DeleteConsumerGroup(consumerName); err != nil {
		err = errors.Wrapf(err, "unable to delete consumer group '%s'", consumerName)
		span.SetTag("error", err)
		return err
	}

	return nil
}

// DeleteTopic deletes a topic from Kafka. It uses the Shopify/sarama library
// as we were running into problems doing the same with segmentio/kafka-go.
func (k *Kafka) DeleteTopic(ctx context.Context, topic string) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "kafka.DeleteTopic")
	defer span.Finish()

	// Sarama is the only library capable of deleting topics from our kafka cluster
	// Kafka-go doesn't work at all
	// Confluent does not support TLS
	clusterAdmin, err := sarama.NewClusterAdmin(k.Options.Brokers, k.getSaramaConfig())
	if err != nil {
		err = errors.Wrap(err, "could not open new connection to kafka")
		span.SetTag("error", err)
		return err
	}

	if err := clusterAdmin.DeleteTopic(topic); err != nil {
		err = errors.Wrap(err, "unable to delete kafka topic")
		span.SetTag("error", err)
		return err
	}

	return nil
}

// DeletePublisher will stop the batch publisher goroutine and remove the
// publisher from the shared publisher map.
//
// It is safe to call this if a publisher for the topic does not exist.
//
// Returns bool which indicate if publisher exists.
func (k *Kafka) DeletePublisher(ctx context.Context, topic string) bool {
	span, _ := tracer.StartSpanFromContext(ctx, "kafka.DeletePublisher")
	defer span.Finish()

	k.PublisherMutex.RLock()
	publisher, ok := k.PublisherMap[topic]
	k.PublisherMutex.RUnlock()

	if !ok {
		k.log.Debugf("publisher for topic '%s' not found", topic)
		return false
	}

	k.log.Debugf("found existing publisher in cache for topic '%s' - closing and removing", topic)

	// Stop batch publisher goroutine
	publisher.PublisherCancel()

	k.PublisherMutex.Lock()
	delete(k.PublisherMap, topic)
	k.PublisherMutex.Unlock()

	return true
}

func validateOptions(opts *Options) error {
	if len(opts.Brokers) == 0 {
		return errors.New("brokers cannot be empty")
	}

	if opts.Timeout == 0 {
		opts.Timeout = DefaultConnectTimeout
	}

	if opts.BatchSize <= 0 {
		opts.BatchSize = DefaultBatchSize
	}

	if opts.WorkerIdleTimeout == 0 {
		opts.WorkerIdleTimeout = DefaultWorkerIdleTimeout
	}

	if opts.ReaderMaxWait == 0 {
		opts.ReaderMaxWait = DefaultReaderMaxWait
	}

	if opts.RequiredWriteAcks == 0 {
		opts.RequiredWriteAcks = DefaultRequiredWriteAcks
	}

	if opts.ServiceShutdownContext == nil {
		return errors.New("ServiceShutdownContext cannot be nil")
	}

	if opts.MainShutdownFunc == nil {
		return errors.New("MainShutdownFunc cannot be nil")
	}

	return nil
}

// watchForShutdown blocks until ServiceShutdownContext is cancelled. It then triggers an infinite loop until all
// publishers have been canceled. Once all publishers are finished, MainShutdownFunc() is called to allow the API
// to shutdown and main() to exit
func (k *Kafka) watchForShutdown() {

	// Block until we've received shutdown signal for services
	<-k.ServiceShutdownContext.Done()

	k.log.Debugf("kafka received shutdown signal, waiting for all publishers to shut down")

	// Loop until PublisherMap is empty
	timeout := time.After(30 * time.Second)

MAIN:
	for {
		select {
		case <-timeout:
			k.log.Warning("timed out waiting for publisher shutdown")
			break MAIN
		default:
			// don't block on the timeout ch
		}

		k.PublisherMutex.RLock()
		if len(k.PublisherMap) == 0 {
			k.PublisherMutex.RUnlock()
			break
		}

		k.PublisherMutex.RUnlock()
		k.log.Debug("publishers still processing, waiting...")
		time.Sleep(PublishShutdownInterval)
	}

	k.log.Debugf("all publishers shut down, canceling main context")
	k.MainShutdownFunc()
}

func (k *Kafka) newPublisher(id, topic string) *Publisher {
	ctx, cancel := context.WithCancel(context.Background())

	publisher := &Publisher{
		ID:                     id,
		Topic:                  topic,
		Writer:                 k.Writer,
		Kafka:                  k,
		Looper:                 director.NewTimedLooper(director.FOREVER, PublishInterval, make(chan error, 1)),
		Queue:                  make([]kafka.Message, 0),
		QueueMutex:             &sync.RWMutex{},
		PublisherContext:       ctx,
		PublisherCancel:        cancel,
		ServiceShutdownContext: k.ServiceShutdownContext,
		IdleTimeout:            k.Options.WorkerIdleTimeout,
		log:                    k.log.WithField("id", id),
	}

	go publisher.runBatchPublisher(ctx)

	return publisher
}

// runBatchPublisher flushes its queue on an interval defined by
func (p *Publisher) runBatchPublisher(ctx context.Context) {
	var quit bool

	lastArrivedAt := time.Now()

	p.Looper.Loop(func() error {
		span, ctx := tracer.StartSpanFromContext(ctx, "kafka.publisher.runBatchPublisher")
		defer span.Finish()

		p.QueueMutex.RLock()
		remaining := len(p.Queue)
		p.QueueMutex.RUnlock()

		if quit && remaining == 0 {
			p.Looper.Quit()
			p.Kafka.DeletePublisher(ctx, p.Topic)
			return nil
		}

		// Should we shutdown?
		select {
		case <-ctx.Done(): // DeletePublisher context
			p.log.Debugf("publisher id '%s' received notice to quit", p.ID)
			quit = true

		case <-p.ServiceShutdownContext.Done():
			p.log.Debugf("publisher id '%s' received app shutdown signal, waiting for batch to be empty", p.ID)
			quit = true
		default:
			// NOOP
		}

		// No reason to keep goroutines running forever
		if remaining == 0 && time.Since(lastArrivedAt) > p.IdleTimeout {
			p.log.Debugf("idle timeout reached (%s); exiting", p.IdleTimeout)

			p.Kafka.DeletePublisher(ctx, p.Topic)
			return nil
		}

		if remaining == 0 {
			// Queue is empty, nothing to do
			return nil
		}

		p.QueueMutex.Lock()
		tmpQueue := p.Queue
		p.Queue = make([]kafka.Message, 0)
		p.QueueMutex.Unlock()

		lastArrivedAt = time.Now()

		// This MUST be context.Background() otherwise the last batch of messages will fail to write since the
		// publisher context has been cancelled.
		_ = p.WriteMessagesBatch(context.Background(), tmpQueue)

		return nil
	})

	p.log.Debugf("publisher id '%s' exiting", p.ID)
}

func (p *Publisher) WriteMessagesBatch(ctx context.Context, msgs []kafka.Message) error {
	p.log.Debugf("creating a batch for %d msgs", len(msgs))

	maxRetries := p.Kafka.Options.NumPublishRetries
	batch := buildBatch(msgs, DefaultSubBatchSize)

MAIN:
	for _, b := range batch {
		var err error
		for i := 1; i <= maxRetries; i++ {
			switch err := p.Writer.WriteMessages(ctx, b...).(type) {
			case nil:
				// No error, continue with the rest of the batches
				continue MAIN
			//case kafka.MessageTooLargeError:
			// TODO: figure this out
			case kafka.WriteErrors:
				// Errors, pick out which messages failed and create a new batch with them
				newBatch := make([]kafka.Message, 0)
				for m := 0; m < len(err); m++ {
					if err[m] != nil {
						newBatch = append(newBatch, b[m])
					}
				}

				b = newBatch

				p.log.Errorf("unable to write %d message(s) [retry %d/%d]", len(b), i, maxRetries)
			default:
				p.log.Errorf("Got unknown error from kafka writer: %s", err)
				time.Sleep(p.Kafka.Options.PublishRetryInterval)
			}
		}

		p.log.Errorf("Failed to write %d messages(s) after %d retries: %s", len(b), maxRetries, err)
	}

	return nil
}

// We need to cut up the slice into batches of 5k because kafka lib has a batch
// limit of 10k per WriteMessages.
func buildBatch(slice []kafka.Message, entriesPerBatch int) [][]kafka.Message {
	batch := make([][]kafka.Message, 0)

	if len(slice) < entriesPerBatch {
		return append(batch, slice)
	}

	// How many iterations should we have?
	iterations := len(slice) / entriesPerBatch

	// We're operating in ints - we need the remainder
	remainder := len(slice) % entriesPerBatch

	var startIndex int
	nextIndex := entriesPerBatch

	for i := 0; i != iterations; i++ {
		batch = append(batch, slice[startIndex:nextIndex])

		startIndex = nextIndex
		nextIndex = nextIndex + entriesPerBatch
	}

	if remainder != 0 {
		batch = append(batch, slice[startIndex:])
	}

	return batch
}

func (p *Publisher) batch(ctx context.Context, value []byte) {
	span, ctx := tracer.StartSpanFromContext(ctx, "kafka.publisher.batch")
	defer span.Finish()

	p.QueueMutex.Lock()
	defer p.QueueMutex.Unlock()

	p.Queue = append(p.Queue, kafka.Message{
		Topic: p.Topic,
		Value: value,
	})
}

func (k *Kafka) getPublisherByTopic(topic string) *Publisher {
	k.PublisherMutex.Lock()
	defer k.PublisherMutex.Unlock()

	p, ok := k.PublisherMap[topic]
	if !ok {
		k.log.Debugf("creating new publisher goroutine for topic '%s'", topic)

		p = k.newPublisher(uuid.NewV4().String(), topic)
		k.PublisherMap[topic] = p
	}

	return p
}

func dialKafka(ctx context.Context, dialer *kafka.Dialer, brokers []string) (*kafka.Conn, error) {
	for _, addr := range brokers {
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			logrus.Errorf("unable to dial '%s': %s", addr, err)
			continue
		}

		return conn, nil
	}

	return nil, errors.New("unable to dial kafka broker(s) - broker list exhausted")
}
