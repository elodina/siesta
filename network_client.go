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

}
type Send struct{}
type ClientResponse struct{
	request ClientRequest
	received bool
	disconnected bool
}
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
	selector                Selector
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

func (nc *NetworkClient) send(request Request) error {
	nodeId := request.destination
	if !nc.isSendable(nodeId) {
		log.Printf("Attempt to send a request to node %s which is not ready.", nodeId)
		return errors.New("Node is not ready.")
	} else {
		nc.inFlightRequests.requests <- request
		nc.selector.send(request)
		return nil
	}
}

func (nc *NetworkClient) poll(timeout int64, now int64) ([]ClientResponse, error) {
	var waitForMetadataFetch int64
	timeToNextMetadataUpdate := nc.metadata.timeToNextUpdate(now)
	timeToNextReconnectAttempt := maxInt64(nc.lastNoNodeAvailableMs+nc.metadata.refreshBackoff()-now, 0)
	if nc.metadataFetchInProgress {
		waitForMetadataFetch = math.MaxInt64
	} else {
		waitForMetadataFetch = 0
	}
	metadataTimeout := maxInt64(timeToNextMetadataUpdate, timeToNextReconnectAttempt, waitForMetadataFetch)
	if metadataTimeout == 0 {
		nc.maybeUpdateMetadata(now)
	}
	err := nc.selector.poll(minInt64(timeout, metadataTimeout))
	if err != nil {
		return nil, err
	}
	// process completed actions
	responses := make([]ClientResponse, 0)
	responses = nc.handleCompletedSends(responses, now)
	responses = nc.handleCompletedReceives(responses, now)
	responses = nc.handleDisconnections(responses, now)
	nc.handleConnections()
	// invoke callbacks
	for _, response := range responses {
		if response.request.hasCallback() {
			response.request.callback(response)
		}
	}
	return responses, nil
}

func (nc *NetworkClient) handleCompletedSends(responses []ClientResponse, now int64) []ClientResponse {
	for _, send := nc.selector.completedSends() {
		request := nc.inFlightRequests.lastSent(send.destination)
		if !request.expectResponse {
			nc.inFlightRequests.completeLastSent(send.destination)
			return append(responses, &ClientResponse{request: request, received: now, disconnected: false, responseBody: []byte{}})
		}
	}
	return responses
}

func (nc *NetworkClient) handleCompletedReceives(responses []ClientResponse, now int64) []ClientResponse {
	for _, receive := nc.selector.completedReceives() {
		source := receive.source
		req := nc.inFlightRequests.completeNext(source)
		header := ResponseHeader.parse(receive.payload)
		apiKey := req.request.header.apiKey
		body := readBody(receive.payload)
		correlate(req.request.header, header)
		if apiKey == ApiKeys.Metadata.id {
			handleMetadataResponse(req.request.header, body, now)
			return responses
		} else {
			return append(responses, &ClientResponse{request: req, received: now, disconnected: false, responseBody: body})
		}
	}
}

func (nc *NetworkClient) handleMetadataResponse(header RequestHeader, body []byte, now int64) {
	nc.metadataFetchInProgress = false
	cluster := response.cluster
	if len(response.errors) > 0 {
		log.Printf("Error while fetching metadata with correlation id %s : %q", header.correlationId, response.errors)
	}
	if len(cluster.nodes) > 0 {
		nc.metadata.update(cluster, now)
	} else {
		log.Println("Ignoring empty metadata response with correlation id %s.", header.correlationId)
		nc.metadata.failedUpdate(now)
	}
}

func (nc *NetworkClient) handleDisconnections(responses []ClientResponse, now int64) []ClientResponse {
	for _, node := range nc.selector.disconnected() {
		nc.connectionStates.disconnected(node)
		log.Printf("Node %s disconnected.", node.id)
		for _, request := nc.inFlightRequests.clearAll(node) {
			requestKey := ApiKeys.forId(request.request.header.apiKey)
			if requestKey == ApiKeys.Metadata {
				nc.metadataFetchInProgress = false
			} else {
				responses = append(responses, &ClientResponse{request: request, received: now, disconnected: true, []byte{}})
			}
		}
	}
	return responses
}

func (nc *NetworkClient) handleConnections() {
	for _, node := nc.selector.connected {
		log.Printf("Completed connection to node %s", node.id)
		nc.connectionStates.connected(node)
	}
}

func (nc *NetworkClient) correlate(requestHeader RequestHeader, responseHeader ResponseHeader) error {
	if requestHeader.correlationId != responseHeader.correlationId {
		return errors.New(fmt.Sprintf("Correlation id for response (%s) does not match request (%s)",
			responseHeader.correlationId, requestHeader.correlationId))
		}
	return nil
}

func (nc *NetworkClient) metadataRequest(now int64, nodeId string, topics []string) *ClientRequest {
	metadata := &MetadataRequest{topics: topics}
	send := &RequestSend(nodeId, nc.NewRequestHeader(ApiKeys.Metadata), metadata.toStruct())
	return &ClientRequest(now, true, send, null)
}

func (nc *NetworkClient) maybeUpdateMetadata(now int64) {
	node := nc.leastLoadedNode(now);
	if node == nil {
		nc.lastNoNodeAvailableMs = now
		return
	}
	nodeConnectionId = node.id
	if nc.connectionStates.isConnected(nodeConnectionId) && nc.inFlightRequests.canSendMore(nodeConnectionId) {
		topics := nc.metadata.topics
		nc.metadataFetchInProgress = true
		metadataRequest := nc.metadataRequest(now, nodeConnectionId, topics)
		nc.selector.send(metadataRequest.request())
		nc.inFlightRequests.add(metadataRequest)
	} else if nc.connectionStates.canConnect(nodeConnectionId, now) {
		nc.initializeConnect(node, now)
	} else {
		nc.lastNoNodeAvailableMs = now
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
