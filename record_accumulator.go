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
	flushed map[string]map[int32]chan bool
}

func NewRecordAccumulator(config *RecordAccumulatorConfig) *RecordAccumulator {
	accumulator := &RecordAccumulator{}
	accumulator.config = config
	accumulator.batchSize = config.batchSize
	accumulator.addChan = make(chan *ProducerRecord, 100)
	accumulator.batches = make(map[string]map[int32][]*ProducerRecord)
	accumulator.flushed = make(map[string]map[int32]chan bool)
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
			ra.createBatch(record.Topic, record.partition)
		}

		partitionBatch := ra.batches[record.Topic][record.partition]
		partitionBatch = append(partitionBatch, record)

		if len(partitionBatch) == 1 {
			go ra.watcher(record.Topic, record.partition)
		}

		ra.batches[record.Topic][record.partition] = partitionBatch
		if len(partitionBatch) == ra.batchSize {
			go ra.flushAndNotify(record.Topic, record.partition)
		}
	}
}

func (ra *RecordAccumulator) createBatch(topic string, partition int32) {
	ra.batches[topic][partition] = make([]*ProducerRecord, 0, ra.batchSize)
}

func (ra *RecordAccumulator) watcher(topic string, partition int32) {
	select {
	case <-ra.flushed[topic][partition]:
	case <-time.After(time.Millisecond * time.Duration(ra.config.lingerMs)):
		ra.flush(topic, partition)
	}
}

func (ra *RecordAccumulator) flushAndNotify(topic string, partition int32) {
	ra.flush(topic, partition)
	ra.flushed[topic][partition] <- true
}

func (ra *RecordAccumulator) flush(topic string, partition int32) {
	ra.networkClient.send(topic, partition, ra.batches[topic][partition])
	ra.createBatch(topic, partition)
}

func (ra *RecordAccumulator) close() {
	close(ra.addChan)
	ra.networkClient.close()
}
