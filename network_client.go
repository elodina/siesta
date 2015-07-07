package siesta

import (
	"net"
	"time"
)

type Node struct {
	id string
}

func (n *Node) address() string {
	return n.id
}

type ClientRequest struct{}
type ClientResponse struct{}
type ApiKeys struct{}
type InFlightRequests struct{}
type ClusterConnectionStates struct {
	connectingNodes   map[string]int64
	disconnectedNodes []string
}

func NewClusterConnectionStates() *ClusterConnectionStates {
	states := &ClusterConnectionStates{}
	states.connectingNodes = make(map[string]int64)
	states.disconnectedNodes = make([]string, 0)
	return states
}

func (ccs *ClusterConnectionStates) connecting(nodeId string, now int64) {
	ccs.connectingNodes[nodeId] = now
}

func (css *ClusterConnectionStates) disconnected(nodeId string) {
	css.disconnectedNodes = append(css.disconnectedNodes, nodeId)
}

func (ccs *ClusterConnectionStates) canConnect(nodeId string, now int64) bool {
	return true
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
	connection              *net.TCPConn
}

type NetworkClientConfig struct {
}

func NewNetworkClient(config NetworkClientConfig) *NetworkClient {
	client := &NetworkClient{}
	client.connectionStates = NewClusterConnectionStates()
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

func (nc *NetworkClient) IsReady(node Node, now int64) bool {
	return true
}

func (nc *NetworkClient) initiateConnect(node Node, now int64) {
	var err error
	nc.connectionStates.connecting(node.id, now)
	size := 1
	keepAlive := true
	keepAlivePeriod := time.Minute
	nc.selector = newConnectionPool(node.address(), size, keepAlive, keepAlivePeriod)
	nc.connection, err = nc.selector.connect()
	if err != nil {
		nc.connectionStates.disconnected(node.id)
		nc.metadata.requestUpdate()
	}
}
