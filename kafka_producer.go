package siesta

import (
	"fmt"
	"hash"
	"hash/fnv"
	"log"
	"math/rand"
	"time"
)

type ProducerRecord struct {
	Topic string
	Key   interface{}
	Value interface{}

	partition    int32
	encodedKey   []byte
	encodedValue []byte
}

type RecordMetadata struct {
	Error error
}
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

type Serializer func(interface{}) ([]byte, error)

func ByteSerializer(value interface{}) ([]byte, error) {
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

type Partitioner interface {
	Partition(record *ProducerRecord, partitions []int32) (int32, error)
}

type HashPartitioner struct {
	random *RandomPartitioner
	hasher hash.Hash32
}

func NewHashPartitioner() *HashPartitioner {
	return &HashPartitioner{
		random: NewRandomPartitioner(),
		hasher: fnv.New32a(),
	}
}

func (hp *HashPartitioner) Partition(record *ProducerRecord, partitions []int32) (int32, error) {
	if record.Key == nil {
		return hp.random.Partition(record, partitions)
	} else {
		hp.hasher.Reset()
		_, err := hp.hasher.Write(record.encodedKey)
		if err != nil {
			return -1, err
		}

		hash := int32(hp.hasher.Sum32())
		if hash < 0 {
			hash = -hash
		}

		return hash % int32(len(partitions)), nil
	}
}

type RandomPartitioner struct {
	random *rand.Rand
}

func NewRandomPartitioner() *RandomPartitioner {
	return &RandomPartitioner{
		random: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (rp *RandomPartitioner) Partition(record *ProducerRecord, partitions []int32) (int32, error) {
	return rp.random.Int31n(int32(len(partitions))), nil
}

type RecordAccumulator struct {
	config        *RecordAccumulatorConfig
	networkClient *NetworkClient
	batchSize     int
	batches       map[string]map[int32][]*ProducerRecord

	addChan chan *ProducerRecord
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
	config                 ProducerConfig
	time                   time.Time
	partitioner            Partitioner
	keySerializer          Serializer
	valueSerializer        Serializer
	metadataFetchTimeoutMs int64
	metadata               *Metadata
	maxRequestSize         int
	totalMemorySize        int
	metrics                map[string]Metric
	compressionType        string
	accumulator            *RecordAccumulator
	metricTags             map[string]string
	connector              Connector
}

func NewKafkaProducer(config ProducerConfig, keySerializer Serializer, valueSerializer Serializer, connector Connector) *KafkaProducer {
	log.Println("Starting the Kafka producer")
	producer := &KafkaProducer{}
	producer.config = config
	producer.time = time.Now()
	producer.metrics = make(map[string]Metric)
	producer.partitioner = NewHashPartitioner()
	producer.keySerializer = keySerializer
	producer.valueSerializer = valueSerializer
	producer.metadataFetchTimeoutMs = config.MetadataFetchTimeout
	producer.metadata = NewMetadata()
	producer.maxRequestSize = config.MaxRequestSize
	producer.totalMemorySize = config.TotalMemorySize
	producer.compressionType = config.CompressionType
	producer.connector = connector
	metricTags := make(map[string]string)

	networkClientConfig := NetworkClientConfig{}
	client := NewNetworkClient(networkClientConfig)
	go sender(producer, client)

	accumulatorConfig := &RecordAccumulatorConfig{
		batchSize:         config.BatchSize,
		totalMemorySize:   producer.totalMemorySize,
		compressionType:   producer.compressionType,
		lingerMs:          config.LingerMs,
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

func sender(producer Producer, client *NetworkClient) {
}

func (kp *KafkaProducer) Send(record *ProducerRecord) <-chan *RecordMetadata {
	metadata := make(chan *RecordMetadata)
	go kp.send(record, metadata)
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

	partitions, err := kp.partitionsForTopic(record.Topic)
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

	kp.accumulator.addChan <- record
}

func (kp *KafkaProducer) partitionsForTopic(topic string) ([]int32, error) {
	topicMetadataResponse, err := kp.connector.GetTopicMetadata([]string{topic})
	if err != nil {
		return nil, err
	}

	for _, topicMetadata := range topicMetadataResponse.TopicsMetadata {
		if topic == topicMetadata.Topic {
			partitions := make([]int32, 0)
			for _, partitionMetadata := range topicMetadata.PartitionsMetadata {
				partitions = append(partitions, partitionMetadata.PartitionID)
			}
			return partitions, nil
		}
	}

	return nil, fmt.Errorf("Topic Metadata response did not contain metadata for topic %s", topic)
}

//func (kp *KafkaProducer) SendCallback(ProducerRecord, Callback) <-chan RecordMetadata {
//	return make(chan RecordMetadata)
//}

func (kp *KafkaProducer) Flush() {}

func (kp *KafkaProducer) PartitionsFor(topic string) []PartitionInfo {
	return []PartitionInfo{}
}

func (kp *KafkaProducer) Metrics() map[string]Metric {
	return make(map[string]Metric)
}

func (kp *KafkaProducer) Close(timeout int) {}
