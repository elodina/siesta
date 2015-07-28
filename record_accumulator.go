package siesta

import "time"

type RecordAccumulatorConfig struct {
	batchSize         int
	totalMemorySize   int
	compressionType   string
	linger            time.Duration
	retryBackoff      time.Duration
	blockOnBufferFull bool
	metrics           map[string]Metric
	time              time.Time
	metricTags        map[string]string
	networkClient     *NetworkClient
}

type RecordAccumulator struct {
	config        *RecordAccumulatorConfig
	networkClient *NetworkClient
	batchSize     int
	batches       map[string]map[int32][]*ProducerRecord

	addChan chan *ProducerRecord
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
		ra.batches[record.Topic][record.partition] = partitionBatch
		if len(partitionBatch) == ra.batchSize {
			go ra.networkClient.send(record.Topic, record.partition, partitionBatch)
			ra.batches[record.Topic][record.partition] = make([]*ProducerRecord, 0, ra.batchSize)
		}
	}
}

func (ra *RecordAccumulator) close() {
	close(ra.addChan)
	ra.networkClient.close()
}
