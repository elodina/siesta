package siesta

type Node struct {
	id string
}
type ClientRequest struct{}
type ClientResponse struct{}
type RequestHeader struct{}
type ApiKeys struct{}
type InFlightRequests struct{}
type ClusterConnectionStates struct{}

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
	selector                Selector
	metadata                Metadata
	connectionStates        ClusterConnectionStates
	inFlightRequests        InFlightRequests
	socketSendBuffer        int
	socketReceiveBuffer     int
	clientId                string
	nodeIndexOffset         int
	correlation             int
	metadataFetchInProgress bool
	lastNoNodeAvailableMs   int64
}

func NewNetworkClient() *NetworkClient {
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
	nc.connectionStates.connecting(node.id, now)
	err := nc.selector.connect(node.id, node.address(), nc.socketSendBuffer, nc.socketReceiveBuffer)
	if err != nil {
		connectionStates.disconnected(node.id)
		metadata.requestUpdate()
	}
}
