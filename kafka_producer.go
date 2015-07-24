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
		_, err := hp.hasher.Write(record.Key)
		if err != nil {
			return -1, err
		}

		hash := int32(hp.hasher.Sum32())
		if hash < 0 {
			hash = -hash
		}

		return hash % len(partitions), nil
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
	return rp.random.Int31n(len(partitions))
}

type RecordAccumulator struct {
	config        *RecordAccumulatorConfig
	networkClient *NetworkClient
	batchSize     int
	batches       map[string]map[int32][]*ProducerRecord

	addChan chan *ProducerRecord
}

type RecordAccumulatorConfig struct {
	batchSize         int
	totalMemorySize   int
	compressionType   string
	lingerMs          int64
	retryBackoffMs    int64
	blockOnBufferFull bool
	metrics           map[string]Metric
	time              time.Time
	metricTags        map[string]string
	networkClient     *NetworkClient
}

func NewRecordAccumulator(config *RecordAccumulatorConfig) *RecordAccumulator {
	accumulator := &RecordAccumulator{}
	accumulator.config = config
	accumulator.batchSize = config.batchSize
	accumulator.addChan = make(chan *ProducerRecord, 100)
	accumulator.batches = make(map[string]map[int32][]*ProducerRecord)
	accumulator.networkClient = config.networkClient

	go accumulator.sender()

	return accumulator
}

func (ra *RecordAccumulator) sender() {
	for record := range ra.addChan {
		if ra.batches[record.Topic] == nil {
			ra.batches[record.Topic] = make(map[int32][]*ProducerRecord)
		}
		if ra.batches[record.Topic][record.partition] == nil {
			ra.batches[record.Topic][record.partition] = make([]*ProducerRecord, 0, ra.batchSize)
		}

		partitionBatch := ra.batches[record.Topic][record.partition]
		partitionBatch = append(partitionBatch, record)
		if len(partitionBatch) == ra.batchSize {
			request := new(ProduceRequest)
			request.RequiredAcks = 1    //TODO
			request.AckTimeoutMs = 2000 //TODO
			for _, record := range partitionBatch {
				request.AddMessage(record.Topic, record.partition, &Message{Key: record.encodedKey, Value: record.encodedValue})
			}

			ra.networkClient.send(request)

			partitionBatch = make([]*ProducerRecord, 0, ra.batchSize)
		}
	}
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
}

func NewKafkaProducer(config ProducerConfig, keySerializer Serializer, valueSerializer Serializer) *KafkaProducer {
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

	partition, err := kp.partitioner.Partition(record, []int32{0, 1, 2}) //TODO get metadata
	if err != nil {
		metadata.Error = err
		metadataChan <- metadata
		return
	}
	record.partition = partition

	kp.accumulator.addChan <- record
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
