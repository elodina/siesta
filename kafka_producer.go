package siesta

import (
	"log"
	"time"
)

type ProducerRecord struct{}
type RecordMetadata struct{}
type PartitionInfo struct{}
type Metric struct{}
type ProducerConfig struct {
	MetadataFetchTimeout int64
	MaxRequestSize       int
	TotalMemorySize      int
	CompressionType      string
	BatchSize            int
	LingerMs             int64
	RetryBackoffMs       int64
	BlockOnBufferFull    bool
}
type Serializer func(string) []byte

type Partitioner struct{}

func NewPartitioner() *Partitioner {
	return &Partitioner{}
}

type Metadata struct{}

func NewMetadata() *Metadata {
	return &Metadata{}
}

func (m *Metadata) requestUpdate() {
}

type RecordAccumulator struct{}

func NewRecordAccumulator(batchSize int,
	totalMemorySize int,
	compressionType string,
	lingerMs int64,
	retryBackoffMs int64,
	blockOnBufferFull bool,
	metrics map[string]Metric,
	time time.Time,
	metricTags map[string]string) *RecordAccumulator {
	return &RecordAccumulator{}
}

type Producer interface {
	// Send the given record asynchronously and return a channel which will eventually contain the response information.
	Send(ProducerRecord) <-chan RecordMetadata

	// Flush any accumulated records from the producer. Blocks until all sends are complete.
	Flush()

	// Get a list of partitions for the given topic for custom partition assignment. The partition metadata will change
	// over time so this list should not be cached.
	PartitionsFor(topic string) []PartitionInfo

	// Return a map of metrics maintained by the producer
	Metrics() map[string]Metric

	// Tries to close the producer cleanly within the specified timeout. If the close does not complete within the
	// timeout, fail any pending send requests and force close the producer.
	Close(timeout int)
}

type KafkaProducer struct {
	config                 ProducerConfig
	time                   time.Time
	partitioner            *Partitioner
	metadataFetchTimeoutMs int64
	metadata               *Metadata
	maxRequestSize         int
	totalMemorySize        int
	metrics                map[string]Metric
	compressionType        string
	accumulator            *RecordAccumulator
	metricTags             map[string]string
}

func NewKafkaProducer(config ProducerConfig, keySerializer Serializer, valueSerializer Serializer) *KafkaProducer {
	log.Println("Starting the Kafka producer")
	producer := &KafkaProducer{}
	producer.config = config
	producer.time = time.Now()
	producer.metrics = make(map[string]Metric)
	producer.partitioner = NewPartitioner()
	producer.metadataFetchTimeoutMs = config.MetadataFetchTimeout
	producer.metadata = NewMetadata()
	producer.maxRequestSize = config.MaxRequestSize
	producer.totalMemorySize = config.TotalMemorySize
	producer.compressionType = config.CompressionType
	metricTags := make(map[string]string)

	producer.accumulator = NewRecordAccumulator(config.BatchSize,
		producer.totalMemorySize,
		producer.compressionType,
		config.LingerMs,
		config.RetryBackoffMs,
		config.BlockOnBufferFull,
		producer.metrics,
		producer.time,
		metricTags)

	networkClientConfig := NetworkClientConfig{}
	client := NewNetworkClient(networkClientConfig)
	go sender(producer, client)

	log.Println("Kafka producer started")

	return producer
}

func sender(producer Producer, client *NetworkClient) {

}

func (kp *KafkaProducer) Send(ProducerRecord) <-chan RecordMetadata {
	return make(chan RecordMetadata)
}

func (kp *KafkaProducer) Flush() {}

func (kp *KafkaProducer) PartitionsFor(topic string) []PartitionInfo {
	return []PartitionInfo{}
}

func (kp *KafkaProducer) Metrics() map[string]Metric {
	return make(map[string]Metric)
}

func (kp *KafkaProducer) Close(timeout int) {}
