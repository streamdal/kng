package kng

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	uuid "github.com/satori/go.uuid"
	"github.com/segmentio/kafka-go"
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
)

type IKafka interface {
	NewReader(id, groupID, topic string) *Reader
	Publish(ctx context.Context, topic string, value []byte)
	DeletePublisher(ctx context.Context, topic string) bool
	DeleteTopic(ctx context.Context, topic string) error
	CreateTopic(ctx context.Context, topic string) error
}

type IReader interface {
	Read(ctx context.Context) (kafka.Message, error)
}

type Kafka struct {
	Conn           *kafka.Conn
	Dialer         *kafka.Dialer
	Writer         *kafka.Writer
	PublisherMap   map[string]*Publisher
	PublisherMutex *sync.RWMutex
	Options        *Options
	log            *logrus.Entry

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

	// Consumer specific settings
	ReaderMaxWait       time.Duration
	ReaderUseLastOffset bool

	// ServiceShutdownContext is used by main() to shutdown services before application termination
	ServiceShutdownContext context.Context

	// MainShutdownFunc is triggered by watchForShutdown() after all publisher queues are exhausted
	// and is used to trigger shutdown of APIs and then main()
	MainShutdownFunc context.CancelFunc
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

	ctxWithTimeout, _ := context.WithTimeout(context.Background(), opts.Timeout)

	conn, err := dialer.DialContext(ctxWithTimeout, "tcp", opts.Brokers[0])
	if err != nil {
		return nil, fmt.Errorf("unable to create initial connection to broker '%s': %s",
			opts.Brokers[0], err)
	}

	transport := &kafka.Transport{
		Dial:        dialer.DialFunc,
		DialTimeout: opts.Timeout,
	}

	k := &Kafka{
		Conn: conn,
		Writer: &kafka.Writer{
			Addr:      kafka.TCP(opts.Brokers[0]),
			BatchSize: opts.BatchSize,
			Transport: transport,
		},
		Dialer:                 dialer,
		PublisherMutex:         &sync.RWMutex{},
		PublisherMap:           make(map[string]*Publisher),
		Options:                opts,
		ServiceShutdownContext: opts.ServiceShutdownContext,
		MainShutdownFunc:       opts.MainShutdownFunc,
		log:                    logrus.WithField("pkg", "kafka"),
	}

	// This goroutine waits for service cancel context to trigger and then loops until all publishers have pushed
	// their batch before triggering MainShutdownFunc()
	go k.watchForShutdown()

	return k, nil
}

// NewReader creates a new reader instance.
func (k *Kafka) NewReader(id, groupID, topic string) *Reader {
	readerConfig := kafka.ReaderConfig{
		Brokers: k.Options.Brokers,
		GroupID: groupID,
		Topic:   topic,
		MaxWait: k.Options.ReaderMaxWait,
		Dialer:  k.Dialer,
	}

	if k.Options.ReaderUseLastOffset {
		readerConfig.StartOffset = kafka.LastOffset
	}

	return &Reader{
		ID:     id,
		Reader: kafka.NewReader(readerConfig),
		log:    logrus.WithField("readerID", id),
	}
}

// Publish provides a simple interface for performing batch writes to Kafka.
//
// * It will automatically create a dedicated publisher for the given topic IF
//   a publisher does not already exist.
// * It will start a background publisher in a goroutine that will clear its
//   queue on an interval defined by PublishInterval const.
// * The publisher goroutine will be stopped if it is idle for longer than
//   WorkerIdleTimeout.
// * To avoid kafka from rejecting a batch containing too many messages, the
//   publisher will automatically divide the batch into "sub-batches" (whose
//   size is defined by DefaultSubBatchSize.
//
// NOTE: The internal queue for the publisher is unbounded which means that the
// longer the flush interval, the more memory the collector will consume.
func (k *Kafka) Publish(ctx context.Context, topic string, value []byte) {
	span, ctx := tracer.StartSpanFromContext(ctx, "kafka.Publish")
	defer span.Finish()

	k.getPublisherByTopic(topic).batch(ctx, value)
}

func (k *Kafka) CreateTopic(ctx context.Context, topic string) error {
	span, ctx := tracer.StartSpanFromContext(context.Background(), "kafka.CreateTopic")
	defer span.Finish()

	// Figure out number of brokers so we can set appropriate replication factor
	brokers, err := k.Conn.Brokers()
	if err != nil {
		return errors.Wrap(err, "unable to determine brokers")
	}

	replicationFactor := DefaultReplicationFactor
	numPartitions := DefaultNumPartitionsPerTopic

	// If local, we do not want to overload kafka - use sensible settings
	if len(brokers) == 1 {
		replicationFactor = 1
		numPartitions = 1
	}

	broker, err := k.Conn.Controller()
	if err != nil {
		return errors.Wrap(err, "unable to find controller")
	}

	controller, err := k.Dialer.DialContext(ctx, "tcp", net.JoinHostPort(broker.Host, strconv.Itoa(broker.Port)))
	if err != nil {
		return errors.Wrap(err, "unable to dial controller")
	}

	if err := controller.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}); err != nil {
		return errors.Wrap(err, "unable to create topic")
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
	cfg := sarama.NewConfig()

	if k.Options.UseTLS {
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	clusterAdmin, err := sarama.NewClusterAdmin(k.Options.Brokers, cfg)
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
	publisher.Looper.Quit()

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
	for {
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

		// Write the messages in the queue
		if err := p.WriteMessagesBatch(ctx, tmpQueue); err != nil {
			fullErr := fmt.Errorf("unable to write %d message(s): %s", len(p.Queue), err)
			p.log.Error(fullErr)
			span.SetTag("error", fullErr)

			return nil
		}

		return nil
	})

	p.log.Debugf("publisher id '%s' exiting", p.ID)
}

func (p *Publisher) WriteMessagesBatch(ctx context.Context, msgs []kafka.Message) error {
	p.log.Debugf("creating a batch for %d msgs", len(msgs))

	batch := buildBatch(msgs, DefaultSubBatchSize)

	for _, b := range batch {
		if err := p.Writer.WriteMessages(ctx, b...); err != nil {
			fullErr := fmt.Errorf("unable to write %d message(s): %s", len(b), err)
			p.log.Error(fullErr)

			return nil
		}
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
