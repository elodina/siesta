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
	closing chan bool
	closed  chan bool
}

func NewRecordAccumulator(config *RecordAccumulatorConfig) *RecordAccumulator {
	accumulator := &RecordAccumulator{}
	accumulator.config = config
	accumulator.batchSize = config.batchSize
	accumulator.addChan = make(chan *ProducerRecord, 100) //TODO config
	accumulator.batches = make(map[string]map[int32][]*ProducerRecord)
	accumulator.flushed = make(map[string]map[int32]chan bool)
	accumulator.networkClient = config.networkClient
	accumulator.closing = make(chan bool)
	accumulator.closed = make(chan bool)

	go accumulator.sender()

	return accumulator
}

func (ra *RecordAccumulator) sender() {
	for {
		select {
		case <-ra.closing:
			ra.cleanup()
			return
		default:
			{
				select {
				case <-ra.closing:
					ra.cleanup()
					return
				case record := <-ra.addChan:
					ra.addRecord(record)
				}
			}
		}
	}
}

func (ra *RecordAccumulator) cleanup() {
	ra.flushAll()
	close(ra.addChan)
	ra.networkClient.close()
	ra.closed <- true
}

func (ra *RecordAccumulator) addRecord(record *ProducerRecord) {
	if ra.batches[record.Topic] == nil {
		ra.batches[record.Topic] = make(map[int32][]*ProducerRecord)
		ra.flushed[record.Topic] = make(map[int32]chan bool)
	}
	if ra.batches[record.Topic][record.partition] == nil {
		ra.createBatch(record.Topic, record.partition)
	}

	partitionBatch := ra.batches[record.Topic][record.partition]
	partitionBatch = append(partitionBatch, record)

	ra.batches[record.Topic][record.partition] = partitionBatch
	if len(partitionBatch) == ra.batchSize {
		ra.toFlush(record.Topic, record.partition)
	}
}

func (ra *RecordAccumulator) createBatch(topic string, partition int32) {
	ra.flushed[topic][partition] = make(chan bool)
	ra.batches[topic][partition] = make([]*ProducerRecord, 0, ra.batchSize)
	go ra.watcher(topic, partition)
}

func (ra *RecordAccumulator) watcher(topic string, partition int32) {
	select {
	case <-ra.flushed[topic][partition]:
		ra.flush(topic, partition)
	case <-time.After(ra.config.linger):
		ra.flush(topic, partition)
	}
}

func (ra *RecordAccumulator) toFlush(topic string, partition int32) {
	ra.flushed[topic][partition] <- true
}

func (ra *RecordAccumulator) flush(topic string, partition int32) {
	if len(ra.batches[topic][partition]) > 0 {
		ra.networkClient.send(topic, partition, ra.batches[topic][partition])
	}
	ra.createBatch(topic, partition)
}

func (ra *RecordAccumulator) flushAll() {
	for topic, partitionBatches := range ra.batches {
		for partition, _ := range partitionBatches {
			ra.flush(topic, partition)
		}
	}
}

func (ra *RecordAccumulator) close() chan bool {
	ra.closing <- true
	return ra.closed
}
