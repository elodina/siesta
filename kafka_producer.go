package siesta

import (
	"fmt"
	"log"
	"time"
)

type ProducerRecord struct {
	Topic string
	Key   interface{}
	Value interface{}

	metadataChan chan *RecordMetadata
	partition    int32
	encodedKey   []byte
	encodedValue []byte
}

type RecordMetadata struct {
	Offset    int64
	Topic     string
	Partition int32
	Error     error
}

type PartitionInfo struct{}
type Metric struct{}
type ProducerConfig struct {
	MetadataFetchTimeout time.Duration
	MetadataExpire       time.Duration
	MaxRequestSize       int
	TotalMemorySize      int
	CompressionType      string
	BatchSize            int
	Linger               time.Duration
	RetryBackoff         time.Duration
	BlockOnBufferFull    bool

	ClientID        string
	MaxRequests     int
	SendRoutines    int
	ReceiveRoutines int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	RequiredAcks    int
	AckTimeoutMs    int32
}

type Serializer func(interface{}) ([]byte, error)

func ByteSerializer(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}

	if array, ok := value.([]byte); ok {
		return array, nil
	}

	return nil, fmt.Errorf("Can't serialize %v", value)
}

func StringSerializer(value interface{}) ([]byte, error) {
	if str, ok := value.(string); ok {
		return []byte(str), nil
	}

	return nil, fmt.Errorf("Can't serialize %v to string", value)
}

type Producer interface {
	// Send the given record asynchronously and return a channel which will eventually contain the response information.
	Send(*ProducerRecord) <-chan *RecordMetadata

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
	config          *ProducerConfig
	time            time.Time
	partitioner     Partitioner
	keySerializer   Serializer
	valueSerializer Serializer
	metrics         map[string]Metric
	accumulator     *RecordAccumulator
	metricTags      map[string]string
	connector       Connector
	metadata        *Metadata
}

func NewKafkaProducer(config *ProducerConfig, keySerializer Serializer, valueSerializer Serializer, connector Connector) *KafkaProducer {
	log.Println("Starting the Kafka producer")
	producer := &KafkaProducer{}
	producer.config = config
	producer.time = time.Now()
	producer.metrics = make(map[string]Metric)
	producer.partitioner = NewHashPartitioner()
	producer.keySerializer = keySerializer
	producer.valueSerializer = valueSerializer
	producer.connector = connector
	producer.metadata = NetMetadata(connector, config.MetadataExpire)
	metricTags := make(map[string]string)

	networkClientConfig := NetworkClientConfig{}
	client := NewNetworkClient(networkClientConfig, connector, config)

	accumulatorConfig := &RecordAccumulatorConfig{
		batchSize:         config.BatchSize,
		totalMemorySize:   config.TotalMemorySize,
		compressionType:   config.CompressionType,
		linger:            config.Linger,
		retryBackoff:      config.RetryBackoff,
		blockOnBufferFull: config.BlockOnBufferFull,
		metrics:           producer.metrics,
		time:              producer.time,
		metricTags:        metricTags,
		networkClient:     client,
	}
	producer.accumulator = NewRecordAccumulator(accumulatorConfig)

	log.Println("Kafka producer started")

	return producer
}

func (kp *KafkaProducer) Send(record *ProducerRecord) <-chan *RecordMetadata {
	metadata := make(chan *RecordMetadata, 1)
	kp.send(record, metadata)
	return metadata
}

func (kp *KafkaProducer) send(record *ProducerRecord, metadataChan chan *RecordMetadata) {
	metadata := new(RecordMetadata)

	serializedKey, err := kp.keySerializer(record.Key)
	if err != nil {
		metadata.Error = err
		metadataChan <- metadata
		return
	}

	serializedValue, err := kp.valueSerializer(record.Value)
	if err != nil {
		metadata.Error = err
		metadataChan <- metadata
		return
	}

	record.encodedKey = serializedKey
	record.encodedValue = serializedValue

	partitions, err := kp.metadata.Get(record.Topic)
	if err != nil {
		metadata.Error = err
		metadataChan <- metadata
		return
	}

	partition, err := kp.partitioner.Partition(record, partitions)
	if err != nil {
		metadata.Error = err
		metadataChan <- metadata
		return
	}
	record.partition = partition
	record.metadataChan = metadataChan

	kp.accumulator.addChan <- record
}

func (kp *KafkaProducer) Flush() {}

func (kp *KafkaProducer) PartitionsFor(topic string) []PartitionInfo {
	return []PartitionInfo{}
}

func (kp *KafkaProducer) Metrics() map[string]Metric {
	return make(map[string]Metric)
}

// TODO return channel and remove timeout
func (kp *KafkaProducer) Close(timeout time.Duration) {
	closed := kp.accumulator.close()
	select {
	case <-closed:
	case <-time.After(timeout):
	}
}
