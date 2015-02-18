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

type TopicMetadataRequest struct {
	Topics []string
}

func NewTopicMetadataRequest(topics []string) *TopicMetadataRequest {
	return &TopicMetadataRequest{
		Topics: topics,
	}
}

func (this *TopicMetadataRequest) Write(encoder Encoder) {
	encoder.WriteInt32(int32(len(this.Topics)))
	for _, topic := range this.Topics {
		encoder.WriteString(topic)
	}
}

func (this *TopicMetadataRequest) Key() int16 {
	return 3
}

func (this *TopicMetadataRequest) Version() int16 {
	return 0
}

type TopicMetadataResponse struct {
	Brokers       []*Broker
	TopicMetadata []*TopicMetadata
}

func (this *TopicMetadataResponse) Read(decoder Decoder) *DecodingError {
	brokersLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reason_InvalidBrokersLength)
	}

	this.Brokers = make([]*Broker, brokersLength)
	for i := int32(0); i < brokersLength; i++ {
		broker := new(Broker)
		err := broker.Read(decoder)
		if err != nil {
			return err
		}
		this.Brokers[i] = broker
	}

	metadataLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reason_InvalidMetadataLength)
	}

	this.TopicMetadata = make([]*TopicMetadata, metadataLength)
	for i := int32(0); i < metadataLength; i++ {
		topicMetadata := new(TopicMetadata)
		err := topicMetadata.Read(decoder)
		if err != nil {
			return err
		}
		this.TopicMetadata[i] = topicMetadata
	}

	return nil
}

type Broker struct {
	NodeId int32
	Host   string
	Port   int32
}

func (this *Broker) Read(decoder Decoder) *DecodingError {
	nodeId, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reason_InvalidBrokerNodeId)
	}
	this.NodeId = nodeId

	host, err := decoder.GetString()
	if err != nil {
		return NewDecodingError(err, reason_InvalidBrokerHost)
	}
	this.Host = host

	port, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reason_InvalidBrokerPort)
	}
	this.Port = port

	return nil
}

type TopicMetadata struct {
	Error             error
	TopicName         string
	PartitionMetadata []*PartitionMetadata
}

func (this *TopicMetadata) Read(decoder Decoder) *DecodingError {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reason_InvalidTopicMetadataErrorCode)
	}
	this.Error = BrokerErrors[errCode]

	topicName, err := decoder.GetString()
	if err != nil {
		return NewDecodingError(err, reason_InvalidTopicMetadataTopicName)
	}
	this.TopicName = topicName

	metadataLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reason_InvalidPartitionMetadataLength)
	}

	this.PartitionMetadata = make([]*PartitionMetadata, metadataLength)
	for i := int32(0); i < metadataLength; i++ {
		metadata := new(PartitionMetadata)
		err := metadata.Read(decoder)
		if err != nil {
			return err
		}
		this.PartitionMetadata[i] = metadata
	}

	return nil
}

type PartitionMetadata struct {
	Error       error
	PartitionId int32
	Leader      int32
	Replicas    []int32
	Isr         []int32
}

func (this *PartitionMetadata) Read(decoder Decoder) *DecodingError {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reason_InvalidPartitionMetadataErrorCode)
	}
	this.Error = BrokerErrors[errCode]

	partition, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reason_InvalidPartitionMetadataPartition)
	}
	this.PartitionId = partition

	leader, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reason_InvalidPartitionMetadataLeader)
	}
	this.Leader = leader

	replicasLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reason_InvalidPartitionMetadataReplicasLength)
	}

	this.Replicas = make([]int32, replicasLength)
	for i := int32(0); i < replicasLength; i++ {
		replica, err := decoder.GetInt32()
		if err != nil {
			return NewDecodingError(err, reason_InvalidPartitionMetadataReplica)
		}
		this.Replicas[i] = replica
	}

	isrLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reason_InvalidPartitionMetadataIsrLength)
	}

	this.Isr = make([]int32, isrLength)
	for i := int32(0); i < isrLength; i++ {
		isr, err := decoder.GetInt32()
		if err != nil {
			return NewDecodingError(err, reason_InvalidPartitionMetadataIsr)
		}
		this.Isr[i] = isr
	}

	return nil
}

var (
	reason_InvalidBrokersLength                   = "Invalid length for Brokers field"
	reason_InvalidMetadataLength                  = "Invalid length for TopicMetadata field"
	reason_InvalidBrokerNodeId                    = "Invalid broker node id"
	reason_InvalidBrokerHost                      = "Invalid broker host"
	reason_InvalidBrokerPort                      = "Invalid broker port"
	reason_InvalidTopicMetadataErrorCode          = "Invalid topic metadata error code"
	reason_InvalidTopicMetadataTopicName          = "Invalid topic metadata topic name"
	reason_InvalidPartitionMetadataLength         = "Invalid length for Partition Metadata field"
	reason_InvalidPartitionMetadataErrorCode      = "Invalid partition metadata error code"
	reason_InvalidPartitionMetadataPartition      = "Invalid partition in partition metadata"
	reason_InvalidPartitionMetadataLeader         = "Invalid leader in partition metadata"
	reason_InvalidPartitionMetadataReplicasLength = "Invalid length for Replicas field"
	reason_InvalidPartitionMetadataReplica        = "Invalid replica in partition metadata"
	reason_InvalidPartitionMetadataIsrLength      = "Invalid length for Isr field"
	reason_InvalidPartitionMetadataIsr            = "Invalid isr in partition metadata"
)
