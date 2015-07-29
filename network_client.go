package siesta

import (
	"math"
	"net"
)

type Node struct {
	id string
}

func (n *Node) address() string {
	return n.id
}

const (
	Disconnected = iota
	Connecting
	Connected
)

type NodeState struct {
	timestamp            int64
	state                int
	lastConnectAttemptMs int64
}

type ClusterConnectionStates struct {
	nodeStates         map[string]*NodeState
	reconnectBackoffMs int64
}

func NewClusterConnectionStates() *ClusterConnectionStates {
	states := &ClusterConnectionStates{}
	states.nodeStates = make(map[string]*NodeState)
	return states
}

func (ccs *ClusterConnectionStates) connecting(nodeId string, now int64) {
	ccs.nodeStates[nodeId].state = Connecting
}

func (ccs *ClusterConnectionStates) disconnected(nodeId string) {
	ccs.nodeStates[nodeId].state = Disconnected
}

func (ccs *ClusterConnectionStates) isConnected(nodeId string) bool {
	return ccs.nodeStates[nodeId].state == Connected
}

func (ccs *ClusterConnectionStates) canConnect(nodeId string, now int64) bool {
	state := ccs.nodeStates[nodeId]
	return state.state == Disconnected && now-state.lastConnectAttemptMs >= ccs.reconnectBackoffMs
}

func (ccs *ClusterConnectionStates) connectionDelay(nodeId string, now int64) int64 {
	state := ccs.nodeStates[nodeId]
	if state.state == Disconnected {
		timeWaited := now - state.lastConnectAttemptMs
		if timeWaited < ccs.reconnectBackoffMs {
			return ccs.reconnectBackoffMs - timeWaited
		} else {
			return 0
		}
	} else {
		return math.MaxInt64
	}
}

type NetworkClient struct {
	connector               Connector
	metadata                Metadata
	connectionStates        *ClusterConnectionStates
	socketSendBuffer        int
	socketReceiveBuffer     int
	clientId                string
	nodeIndexOffset         int
	correlation             int
	metadataFetchInProgress bool
	lastNoNodeAvailableMs   int64
	selector                *Selector
	connections             map[string]*net.TCPConn
	requiredAcks            int
	ackTimeoutMs            int32
}

type NetworkClientConfig struct {
}

func NewNetworkClient(config NetworkClientConfig, connector Connector, producerConfig *ProducerConfig) *NetworkClient {
	client := &NetworkClient{}
	client.connector = connector
	client.requiredAcks = producerConfig.RequiredAcks
	client.ackTimeoutMs = producerConfig.AckTimeoutMs
	selectorConfig := NewSelectorConfig(producerConfig)
	client.selector = NewSelector(selectorConfig)
	client.connectionStates = NewClusterConnectionStates()
	client.connections = make(map[string]*net.TCPConn, 0)
	return client
}

func (nc *NetworkClient) connectionDelay(node *Node, now int64) int64 {
	return nc.connectionStates.connectionDelay(node.id, now)
}

func (nc *NetworkClient) connectionFailed(node *Node) bool {
	return nc.connectionStates.nodeStates[node.id].state == Disconnected
}

func (nc *NetworkClient) send(topic string, partition int32, batch []*ProducerRecord) {
	leader, err := nc.connector.GetLeader(topic, partition)
	if err != nil {
		for _, record := range batch {
			record.metadataChan <- &RecordMetadata{Error: err}
		}
	}

	request := new(ProduceRequest)
	request.RequiredAcks = int16(nc.requiredAcks)
	request.AckTimeoutMs = nc.ackTimeoutMs
	for _, record := range batch {
		request.AddMessage(record.Topic, record.partition, &Message{Key: record.encodedKey, Value: record.encodedValue})
	}
	responseChan := nc.selector.Send(leader, request)

	if nc.requiredAcks > 0 {
		go listenForResponse(topic, partition, batch, responseChan)
	} else {
		// acks = 0 case, just complete all requests
		for _, record := range batch {
			record.metadataChan <- &RecordMetadata{
				Offset:    -1,
				Topic:     topic,
				Partition: partition,
				Error:     ErrNoError,
			}
		}
	}
}

func listenForResponse(topic string, partition int32, batch []*ProducerRecord, responseChan <-chan *rawResponseAndError) {
	response := <-responseChan
	if response.err != nil {
		for _, record := range batch {
			record.metadataChan <- &RecordMetadata{Error: response.err}
		}
	}

	decoder := NewBinaryDecoder(response.bytes)
	produceResponse := new(ProduceResponse)
	decodingErr := produceResponse.Read(decoder)
	if decodingErr != nil {
		for _, record := range batch {
			record.metadataChan <- &RecordMetadata{Error: decodingErr.err}
		}
	}

	status := produceResponse.Status[topic][partition]
	currentOffset := status.Offset
	for _, record := range batch {
		record.metadataChan <- &RecordMetadata{
			Topic:     topic,
			Partition: partition,
			Offset:    currentOffset,
			Error:     status.Error,
		}
		currentOffset++
	}
}

func (nc *NetworkClient) close() {
	nc.selector.Close()
}
