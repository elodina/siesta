package siesta

import "time"

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
		if len(partitionBatch) == ra.batchSize {
			//      request := new(ProduceRequest)
			//      request.RequiredAcks = 1    //TODO
			//      request.AckTimeoutMs = 2000 //TODO
			//      for _, record := range partitionBatch {
			//        request.AddMessage(record.Topic, record.partition, &Message{Key: record.encodedKey, Value: record.encodedValue})
			//      }

			//			ra.networkClient.send(record.Topic, record.partition, request)
			go ra.networkClient.send(record.Topic, record.partition, partitionBatch)

			partitionBatch = make([]*ProducerRecord, 0, ra.batchSize)
		}
	}
}
