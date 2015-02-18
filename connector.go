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
	"time"
)

type Connector interface {
	Consume(topic string, partition int32, offset int64) ([]*Message, error)
	GetAvailableOffsets(topic string, partition int32) (*OffsetResponse, error)
	GetTopicMetadata(topic string) (*TopicMetadataResponse, error)
	Produce(message Message) error

	//TODO: implement GetGroupMetadata, GetGroupAvailableOffsets, CommitGroupOffset API calls
	//GetGroupMetadata(group string)
	//GetGroupAvailableOffsets(group string)
	//CommitGroupOffset(topic string, partition int32, offset int64) error
}

type ConnectorConfig struct {
	BrokerList              []*Broker
	ReadTimeout             time.Duration
	WriteTimeout            time.Duration
	ConnectTimeout          time.Duration
	KeepAlive               bool
	KeepAliveTimeout        time.Duration
	MaxConnections          int
	MaxConnectionsPerBroker int
	FetchSize               int32
}

type DefaultConnector struct {
	availableBrokers        map[int32]*brokerLink
	readTimeout             time.Duration
	writeTimeout            time.Duration
	connectTimeout          time.Duration
	keepAlive               bool
	keepAliveTimeout        time.Duration
	maxConnectionsPerBroker int
	fetchSize               int32
}

func NewDefaultConnector(config ConnectorConfig) Connector {
	availableBrokers := make(map[int32]*brokerLink)
	for _, broker := range config.BrokerList {
		availableBrokers[broker.NodeId] = newBrokerLink(broker, config.KeepAlive, config.KeepAliveTimeout,
			config.MaxConnectionsPerBroker)
	}
	return &DefaultConnector{
		availableBrokers:        availableBrokers,
		readTimeout:             config.ReadTimeout,
		writeTimeout:            config.WriteTimeout,
		connectTimeout:          config.ConnectTimeout,
		keepAlive:               config.KeepAlive,
		keepAliveTimeout:        config.KeepAliveTimeout,
		maxConnectionsPerBroker: config.MaxConnectionsPerBroker,
	}
}

func (this *DefaultConnector) Consume(topic string, partition int32, offset int64) ([]*Message, error) {
	panic("Not implemented yet")
}

func (this *DefaultConnector) GetAvailableOffsets(topic string, partition int32) (*OffsetResponse, error) {
	panic("Not implemented yet")
}

func (this *DefaultConnector) GetTopicMetadata(topic string) (*TopicMetadataResponse, error) {
	panic("Not implemented yet")
}

func (this *DefaultConnector) Produce(message Message) error {
	panic("Not implemented yet")
}

type brokerLink struct {
	broker                    *Broker
	connectionPool            *connectionPool
	lastConnectTime           time.Time
	lastSuccessfulConnectTime time.Time
	failedAttempts            int
}

func newBrokerLink(broker *Broker, keepAlive bool, keepAliveTimeout time.Duration, maxConnectionsPerBroker int) *brokerLink {
	brokerConnect := fmt.Sprintf("%s:%d", broker.Host, broker.Port)
	return &brokerLink{
		broker:         broker,
		connectionPool: newConnectionPool(brokerConnect, maxConnectionsPerBroker, keepAlive, keepAliveTimeout),
	}
}
