/* Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

package siesta

import "fmt"

// TopicMetadataRequest is used to get topics, their partitions, leader brokers for them and where these brokers are located.
type TopicMetadataRequest struct {
	Topics []string
}

// NewTopicMetadataRequest creates a new TopicMetadataRequest to fetch metadata for given topics.
// Passing it an empty slice will request metadata for all topics.
func NewTopicMetadataRequest(topics []string) *TopicMetadataRequest {
	return &TopicMetadataRequest{
		Topics: topics,
	}
}

func (tmr *TopicMetadataRequest) Write(encoder Encoder) {
	encoder.WriteInt32(int32(len(tmr.Topics)))
	for _, topic := range tmr.Topics {
		encoder.WriteString(topic)
	}
}

// Key returns the Kafka API key for TopicMetadataRequest.
func (tmr *TopicMetadataRequest) Key() int16 {
	return 3
}

// Version returns the Kafka request version for backwards compatibility.
func (tmr *TopicMetadataRequest) Version() int16 {
	return 0
}

// TopicMetadataResponse contains information about brokers in cluster and topics that exist.
type TopicMetadataResponse struct {
	Brokers       []*Broker
	TopicMetadata []*TopicMetadata
}

func (tmr *TopicMetadataResponse) Read(decoder Decoder) *DecodingError {
	brokersLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidBrokersLength)
	}

	tmr.Brokers = make([]*Broker, brokersLength)
	for i := int32(0); i < brokersLength; i++ {
		broker := new(Broker)
		err := broker.Read(decoder)
		if err != nil {
			return err
		}
		tmr.Brokers[i] = broker
	}

	metadataLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidMetadataLength)
	}

	tmr.TopicMetadata = make([]*TopicMetadata, metadataLength)
	for i := int32(0); i < metadataLength; i++ {
		topicMetadata := new(TopicMetadata)
		err := topicMetadata.Read(decoder)
		if err != nil {
			return err
		}
		tmr.TopicMetadata[i] = topicMetadata
	}

	return nil
}

// Broker contains information about a Kafka broker in cluster - its ID, host name and port.
type Broker struct {
	NodeID int32
	Host   string
	Port   int32
}

func (b *Broker) String() string {
	return fmt.Sprintf("%s:%d", b.Host, b.Port)
}

func (b *Broker) Read(decoder Decoder) *DecodingError {
	nodeID, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidBrokerNodeID)
	}
	b.NodeID = nodeID

	host, err := decoder.GetString()
	if err != nil {
		return NewDecodingError(err, reasonInvalidBrokerHost)
	}
	b.Host = host

	port, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidBrokerPort)
	}
	b.Port = port

	return nil
}

// TopicMetadata contains information about topic - its name, number of partitions, leaders, ISRs and errors if they occur.
type TopicMetadata struct {
	Error             error
	TopicName         string
	PartitionMetadata []*PartitionMetadata
}

func (tm *TopicMetadata) Read(decoder Decoder) *DecodingError {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reasonInvalidTopicMetadataErrorCode)
	}
	tm.Error = BrokerErrors[errCode]

	topicName, err := decoder.GetString()
	if err != nil {
		return NewDecodingError(err, reasonInvalidTopicMetadataTopicName)
	}
	tm.TopicName = topicName

	metadataLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidPartitionMetadataLength)
	}

	tm.PartitionMetadata = make([]*PartitionMetadata, metadataLength)
	for i := int32(0); i < metadataLength; i++ {
		metadata := new(PartitionMetadata)
		err := metadata.Read(decoder)
		if err != nil {
			return err
		}
		tm.PartitionMetadata[i] = metadata
	}

	return nil
}

// PartitionMetadata contains information about a topic partition - its id, leader, replicas, ISRs and error if it occurred.
type PartitionMetadata struct {
	Error       error
	PartitionID int32
	Leader      int32
	Replicas    []int32
	Isr         []int32
}

func (pm *PartitionMetadata) Read(decoder Decoder) *DecodingError {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reasonInvalidPartitionMetadataErrorCode)
	}
	pm.Error = BrokerErrors[errCode]

	partition, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidPartitionMetadataPartition)
	}
	pm.PartitionID = partition

	leader, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidPartitionMetadataLeader)
	}
	pm.Leader = leader

	replicasLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidPartitionMetadataReplicasLength)
	}

	pm.Replicas = make([]int32, replicasLength)
	for i := int32(0); i < replicasLength; i++ {
		replica, err := decoder.GetInt32()
		if err != nil {
			return NewDecodingError(err, reasonInvalidPartitionMetadataReplica)
		}
		pm.Replicas[i] = replica
	}

	isrLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidPartitionMetadataIsrLength)
	}

	pm.Isr = make([]int32, isrLength)
	for i := int32(0); i < isrLength; i++ {
		isr, err := decoder.GetInt32()
		if err != nil {
			return NewDecodingError(err, reasonInvalidPartitionMetadataIsr)
		}
		pm.Isr[i] = isr
	}

	return nil
}

var (
	reasonInvalidBrokersLength                   = "Invalid length for Brokers field"
	reasonInvalidMetadataLength                  = "Invalid length for TopicMetadata field"
	reasonInvalidBrokerNodeID                    = "Invalid broker node id"
	reasonInvalidBrokerHost                      = "Invalid broker host"
	reasonInvalidBrokerPort                      = "Invalid broker port"
	reasonInvalidTopicMetadataErrorCode          = "Invalid topic metadata error code"
	reasonInvalidTopicMetadataTopicName          = "Invalid topic metadata topic name"
	reasonInvalidPartitionMetadataLength         = "Invalid length for Partition Metadata field"
	reasonInvalidPartitionMetadataErrorCode      = "Invalid partition metadata error code"
	reasonInvalidPartitionMetadataPartition      = "Invalid partition in partition metadata"
	reasonInvalidPartitionMetadataLeader         = "Invalid leader in partition metadata"
	reasonInvalidPartitionMetadataReplicasLength = "Invalid length for Replicas field"
	reasonInvalidPartitionMetadataReplica        = "Invalid replica in partition metadata"
	reasonInvalidPartitionMetadataIsrLength      = "Invalid length for Isr field"
	reasonInvalidPartitionMetadataIsr            = "Invalid isr in partition metadata"
)
