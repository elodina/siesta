package siesta

import (
	"errors"
	"log"
	"math"
	"net"
	"time"
)

type Node struct {
	id string
}

func (n *Node) address() string {
	return n.id
}

type ClientRequest struct {
	destination string
}
type ClientResponse struct{}
type ApiKeys struct{}
type InFlightRequests struct {
	requests                         chan *ClientRequest
	maxInFlightRequestsPerConnection int
}

func (ifr *InFlightRequests) canSendMore(nodeId string) bool {
	return len(ifr.requests) < ifr.maxInFlightRequestsPerConnection
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

type KafkaClient interface {
	IsReady(Node, int64) bool
	Ready(Node, int64) bool
	ConnectionDelay(Node, int64) int64
	ConnectionFailed(Node) bool
	Send(ClientRequest)
	Poll(int64, int64) []ClientResponse
	CompleteAll(string, int64) []ClientResponse
	LeastLoadedNode(int64) Node
	InFlightRequestCount(string) int
	NextRequestHeader(ApiKeys) RequestHeader
	Wakeup()
}

type NetworkClient struct {
	metadata                Metadata
	connectionStates        *ClusterConnectionStates
	inFlightRequests        InFlightRequests
	socketSendBuffer        int
	socketReceiveBuffer     int
	clientId                string
	nodeIndexOffset         int
	correlation             int
	metadataFetchInProgress bool
	lastNoNodeAvailableMs   int64
	selector                *connectionPool
	connections             map[string]*net.TCPConn
}

type NetworkClientConfig struct {
}

func NewNetworkClient(config NetworkClientConfig) *NetworkClient {
	client := &NetworkClient{}
	client.connectionStates = NewClusterConnectionStates()
	client.connections = make(map[string]*net.TCPConn, 0)
	return &NetworkClient{}
}

func (nc *NetworkClient) Ready(node Node, now int64) bool {
	if nc.IsReady(node, now) {
		return true
	}

	if nc.connectionStates.canConnect(node.id, now) {
		nc.initiateConnect(node, now)
	}

	return false
}

func (nc *NetworkClient) connectionDelay(node *Node, now int64) int64 {
	return nc.connectionStates.connectionDelay(node.id, now)
}

func (nc *NetworkClient) connectionFailed(node *Node) bool {
	return nc.connectionStates.nodeStates[node.id].state == Disconnected
}

func (nc *NetworkClient) IsReady(node Node, now int64) bool {
	if !nc.metadataFetchInProgress && nc.metadata.timeToNextUpdate(now) == 0 {
		return false
	} else {
		return nc.isSendable(node.id)
	}
}

func (nc *NetworkClient) isSendable(nodeId string) bool {
	return nc.connectionStates.isConnected(nodeId) && nc.inFlightRequests.canSendMore(nodeId)
}

func (nc *NetworkClient) send(request *ClientRequest) error {
	nodeId := request.destination
	if !nc.isSendable(nodeId) {
		log.Printf("Attempt to send a request to node %s which is not ready.", nodeId)
		return errors.New("Node is not ready.")
	} else {
		nc.inFlightRequests.requests <- request
		//nc.selector.send(request)
		return nil
	}
}

func (nc *NetworkClient) initiateConnect(node Node, now int64) {
	var err error
	nc.connectionStates.connecting(node.id, now)
	size := 1
	keepAlive := true
	keepAlivePeriod := time.Minute
	nc.selector = newConnectionPool(node.address(), size, keepAlive, keepAlivePeriod)
	conn, err := nc.selector.connect()
	if err != nil {
		nc.connectionStates.disconnected(node.id)
		nc.metadata.requestUpdate()
	} else {
		nc.connections[node.id] = conn
	}
}
