/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package siesta

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"errors"
)

type Connector interface {
	Consume(topic string, partition int32, offset int64) ([]*Message, error)
	GetAvailableOffsets(group string, topic string, partition int32) (*OffsetResponse, error)
	GetTopicMetadata(topics []string) (*TopicMetadataResponse, error)
	Produce(message Message) error
	Close() <-chan bool

	//TODO: implement GetGroupMetadata, GetGroupAvailableOffsets, CommitGroupOffset API calls
	//GetGroupMetadata(group string)
	//GetGroupAvailableOffsets(group string)
	//CommitGroupOffset(topic string, partition int32, offset int64) error
}

type ConnectorConfig struct {
	BrokerList              []string
	ReadTimeout             time.Duration
	WriteTimeout            time.Duration
	ConnectTimeout          time.Duration
	KeepAlive               bool
	KeepAliveTimeout        time.Duration
	MaxConnections          int
	MaxConnectionsPerBroker int
	FetchSize               int32
	ClientId                string
}

type DefaultConnector struct {
	bootstrapBrokers        []string
	leaders                 map[string]map[int32]*brokerLink
	links                   []*brokerLink
	readTimeout             time.Duration
	writeTimeout            time.Duration
	connectTimeout          time.Duration
	keepAlive               bool
	keepAliveTimeout        time.Duration
	maxConnectionsPerBroker int
	fetchSize               int32
	clientId                string
	lock                    sync.Mutex

	//offset coordination part
	offsetCoordinators        map[string]int32
}

func NewDefaultConnector(config *ConnectorConfig) *DefaultConnector {
	leaders := make(map[string]map[int32]*brokerLink)
	brokers := make([]*brokerLink, 0)
	connector := &DefaultConnector{
		bootstrapBrokers:        config.BrokerList,
		leaders:                 leaders,
		links:                   brokers,
		readTimeout:             config.ReadTimeout,
		writeTimeout:            config.WriteTimeout,
		connectTimeout:          config.ConnectTimeout,
		keepAlive:               config.KeepAlive,
		keepAliveTimeout:        config.KeepAliveTimeout,
		maxConnectionsPerBroker: config.MaxConnectionsPerBroker,
		clientId:                config.ClientId,
		fetchSize:               config.FetchSize,
		offsetCoordinators:      make(map[string]int32),
	}

	return connector
}

func (this *DefaultConnector) String() string {
	return "Default Connector"
}

func (this *DefaultConnector) Consume(topic string, partition int32, offset int64) ([]*Message, error) {
	brokerLink := this.getLeader(topic, partition)
	if brokerLink == nil {
		this.refreshMetadata([]string{topic})
		brokerLink = this.getLeader(topic, partition)
		//TODO what if brokerLink is still nil?
	}

	request := new(FetchRequest)
	request.AddFetch(topic, partition, offset, this.fetchSize)
	bytes, err := this.syncSendAndReceive(brokerLink, request)
	if err != nil {
		return nil, err
	}

	decoder := NewBinaryDecoder(bytes)
	response := new(FetchResponse)
	decodingErr := response.Read(decoder)
	if decodingErr != nil {
		return nil, decodingErr.Error()
	}

	return response.GetMessages(), nil
}

func (this *DefaultConnector) GetAvailableOffsets(group string, topic string, partition int32) (int64, error) {
	coordinatorId, exists := this.offsetCoordinators[group]
	if !exists {
		err := this.refreshOffsetCoordinator(group)
		if err != nil {
			return -1, err
		}
		coordinatorId = this.offsetCoordinators[group]
	}

	Warnf(this, "Offset coordinator for group %s: %d", group, coordinatorId)

	var brokerLink *brokerLink
	for _, link := range this.links {
		if link.broker.NodeId == coordinatorId {
			brokerLink = link
			break
		}
	}

	if brokerLink == nil {
		return -1, errors.New(fmt.Sprintf("Could not find broker with node id %d", coordinatorId))
	}

	request := NewOffsetFetchRequest(group)
	request.AddOffset(topic, partition)
	bytes, err := this.syncSendAndReceive(brokerLink, request)
	if err != nil {
		return -1, err
	}
	response := new(OffsetFetchResponse)
	decodingErr := this.decode(bytes, response)
	if decodingErr != nil {
		return -1, decodingErr.Error()
	}

	//TODO this is unsafe
	return response.Offsets[topic][partition].Offset, nil
}

func (this *DefaultConnector) GetTopicMetadata(topics []string) (*TopicMetadataResponse, error) {
	for len(this.links) == 0 {
		Info(this, "No connected brokers yet, refreshing metadata")
		this.refreshMetadata(topics)
		//TODO backoff
	}

	response, err := this.sendToAllAndReturnFirstSuccessful(NewTopicMetadataRequest(topics), this.topicMetadataValidator)
	if response != nil {
		return response.(*TopicMetadataResponse), err
	} else {
		return nil, err
	}
}

func (this *DefaultConnector) Produce(message Message) error {
	panic("Not implemented yet")
}

func (this *DefaultConnector) Close() <-chan bool {
	closed := make(chan bool)
	go func() {
		for _, link := range this.links {
			link.stop <- true
		}
		closed <- true
	}()

	return closed
}

func (this *DefaultConnector) refreshMetadata(topics []string) {
	if len(this.links) == 0 {
		for i := 0; i < len(this.bootstrapBrokers); i++ {
			broker := this.bootstrapBrokers[i]
			hostPort := strings.Split(broker, ":")
			if len(hostPort) != 2 {
				panic(fmt.Sprintf("incorrect broker connection string: %s", broker))
			}

			port, err := strconv.Atoi(hostPort[1])
			if err != nil {
				panic(fmt.Sprintf("incorrect port in broker connection string: %s", broker))
			}

			this.links = append(this.links, newBrokerLink(&Broker{NodeId: -1, Host: hostPort[0], Port: int32(port)},
					this.keepAlive,
					this.keepAliveTimeout,
					this.maxConnectionsPerBroker))
		}
	}

	response, err := this.sendToAllAndReturnFirstSuccessful(NewTopicMetadataRequest(topics), this.topicMetadataValidator)
	if err != nil {
		Errorf(this, "Could not get topic metadata from all known brokers")
		return
	}
	this.refreshLeaders(response.(*TopicMetadataResponse))
}

func (this *DefaultConnector) refreshLeaders(response *TopicMetadataResponse) {
	brokers := make(map[int32]*brokerLink)
	for _, broker := range response.Brokers {
		brokers[broker.NodeId] = newBrokerLink(broker, this.keepAlive, this.keepAliveTimeout, this.maxConnectionsPerBroker)
	}

	if len(brokers) != 0 && len(response.TopicMetadata) != 0 {
		this.links = make([]*brokerLink, 0)
	}

	for _, metadata := range response.TopicMetadata {
		for _, partitionMetadata := range metadata.PartitionMetadata {
			if leader, exists := brokers[partitionMetadata.Leader]; exists {
				this.putLeader(metadata.TopicName, partitionMetadata.PartitionId, leader)
			} else {
				Warnf(this, "Topic Metadata response has no leader present for topic %s, parition %d", metadata.TopicName, partitionMetadata.PartitionId)
				//TODO: warn about incomplete broker list
			}
		}
	}
}

func (this *DefaultConnector) getLeader(topic string, partition int32) *brokerLink {
	leadersForTopic, exists := this.leaders[topic]
	if !exists {
		return nil
	}

	return leadersForTopic[partition]
}

func (this *DefaultConnector) putLeader(topic string, partition int32, leader *brokerLink) {
	Tracef(this, "putLeader for topic %s, partition %d - %s", topic, partition, leader.broker)
	if _, exists := this.leaders[topic]; !exists {
		this.leaders[topic] = make(map[int32]*brokerLink)
	}

	exists := false
	for _, link := range this.links {
		if *link.broker == *leader.broker {
			exists = true
			break
		}
	}

	if !exists {
		this.links = append(this.links, leader)
	}

	this.leaders[topic][partition] = leader
}

func (this *DefaultConnector) refreshOffsetCoordinator(group string) error {
	request := NewConsumerMetadataRequest(group)

	response, err := this.sendToAllAndReturnFirstSuccessful(request, this.consumerMetadataValidator)
	if err != nil {
		Errorf(this, "Could not get consumer metadata from all known brokers")
		return err
	}
	this.offsetCoordinators[group] = response.(*ConsumerMetadataResponse).CoordinatorId

	return nil
}

func (this *DefaultConnector) decode(bytes []byte, response Response) *DecodingError {
	decoder := NewBinaryDecoder(bytes)
	decodingErr := response.Read(decoder)
	if decodingErr != nil {
		return decodingErr
	}

	return nil
}

func (this *DefaultConnector) sendToAllAndReturnFirstSuccessful(request Request, check func([]byte) Response) (Response, error) {
	for len(this.links) == 0 {
		Info(this, "No connected brokers yet, refreshing metadata")
		this.refreshMetadata(nil)
		//TODO backoff
	}

	responses := make(chan *rawResponseAndError, len(this.links))
	for i := 0; i < len(this.links); i++ {
		link := this.links[i]
		go func() {
			bytes, err := this.syncSendAndReceive(link, request)
			responses <- &rawResponseAndError{bytes, link, err}
		}()
	}

	var response *rawResponseAndError
	for i := 0; i < len(this.links); i++ {
		response = <-responses
		if response.err == nil {
			if checkResult := check(response.bytes); checkResult != nil {
				return checkResult, nil
			} else {
				response.err = errors.New("Check result did not pass")
			}
		}

		Infof(this, "Could not process request with broker %s:%d", response.link.broker.Host, response.link.broker.Port)
	}

	return nil, response.err
}

func (this *DefaultConnector) syncSendAndReceive(link *brokerLink, request Request) ([]byte, error) {
	id, conn, err := link.getConnection()
	if err != nil {
		return nil, err
	}
	defer link.connectionPool.Return(conn)

	if err := this.send(id, conn, request); err != nil {
		return nil, err
	}

	return this.receive(conn)
}

func (this *DefaultConnector) send(correlationId int32, conn *net.TCPConn, request Request) error {
	writer := NewRequestWriter(correlationId, this.clientId, request)
	bytes := make([]byte, writer.Size())
	encoder := NewBinaryEncoder(bytes)
	writer.Write(encoder)

	conn.SetWriteDeadline(time.Now().Add(this.writeTimeout))
	_, err := conn.Write(bytes)
	return err
}

func (this *DefaultConnector) receive(conn *net.TCPConn) ([]byte, error) {
	conn.SetReadDeadline(time.Now().Add(this.writeTimeout))
	header := make([]byte, 8)
	_, err := io.ReadFull(conn, header)
	if err != nil {
		return nil, err
	}

	decoder := NewBinaryDecoder(header)
	length, err := decoder.GetInt32()
	if err != nil {
		return nil, err
	}
	response := make([]byte, length-4)
	_, err = io.ReadFull(conn, response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (this *DefaultConnector) topicMetadataValidator(bytes []byte) Response {
	//TODO check topic and partition errors
	response := new(TopicMetadataResponse)
	err := this.decode(bytes, response)
	if err != nil {
		return nil
	}

	return response
}

func (this *DefaultConnector) consumerMetadataValidator(bytes []byte) Response {
	response := new(ConsumerMetadataResponse)
	err := this.decode(bytes, response)
	if err != nil || response.Error != NoError {
		return nil
	}

	return response
}

type brokerLink struct {
	broker                    *Broker
	connectionPool            *connectionPool
	lastConnectTime           time.Time
	lastSuccessfulConnectTime time.Time
	failedAttempts            int
	correlationIds            chan int32
	stop                      chan bool
}

func newBrokerLink(broker *Broker, keepAlive bool, keepAliveTimeout time.Duration, maxConnectionsPerBroker int) *brokerLink {
	brokerConnect := fmt.Sprintf("%s:%d", broker.Host, broker.Port)
	correlationIds := make(chan int32)
	stop := make(chan bool)

	go correlationIdGenerator(correlationIds, stop)

	return &brokerLink{
		broker:         broker,
		connectionPool: newConnectionPool(brokerConnect, maxConnectionsPerBroker, keepAlive, keepAliveTimeout),
		correlationIds: correlationIds,
		stop:           stop,
	}
}

func (this *brokerLink) getConnection() (int32, *net.TCPConn, error) {
	correlationId := <-this.correlationIds
	conn, err := this.connectionPool.Borrow()
	return correlationId, conn, err
}

func correlationIdGenerator(out chan int32, stop chan bool) {
	var correlationId int32 = 0
	for {
		select {
		case out <- correlationId:
			correlationId++
		case <-stop:
			return
		}
	}
}

type rawResponseAndError struct {
	bytes     []byte
	link     *brokerLink
	err       error
}
