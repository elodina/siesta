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

func (this *TopicMetadataResponse) Read(decoder Decoder) error {
	brokersLength, err := decoder.GetInt32()
	if err != nil {
		return err
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
		return err
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

func (this *Broker) Read(decoder Decoder) error {
	nodeId, err := decoder.GetInt32()
	if err != nil {
		return err
	}
	this.NodeId = nodeId

	host, err := decoder.GetString()
	if err != nil {
		return err
	}
	this.Host = host

	port, err := decoder.GetInt32()
	if err != nil {
		return err
	}
	this.Port = port

	return nil
}

type TopicMetadata struct {
	Error             error
	TopicName         string
	PartitionMetadata []*PartitionMetadata
}

func (this *TopicMetadata) Read(decoder Decoder) error {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return err
	}
	this.Error = BrokerErrors[errCode]

	topicName, err := decoder.GetString()
	if err != nil {
		return err
	}
	this.TopicName = topicName

	metadataLength, err := decoder.GetInt32()
	if err != nil {
		return err
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

func (this *PartitionMetadata) Read(decoder Decoder) error {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return err
	}
	this.Error = BrokerErrors[errCode]

	partition, err := decoder.GetInt32()
	if err != nil {
		return err
	}
	this.PartitionId = partition

	leader, err := decoder.GetInt32()
	if err != nil {
		return err
	}
	this.Leader = leader

	replicasLength, err := decoder.GetInt32()
	if err != nil {
		return err
	}

	this.Replicas = make([]int32, replicasLength)
	for i := int32(0); i < replicasLength; i++ {
		replica, err := decoder.GetInt32()
		if err != nil {
			return err
		}
		this.Replicas[i] = replica
	}

	isrLength, err := decoder.GetInt32()
	if err != nil {
		return err
	}

	this.Isr = make([]int32, isrLength)
	for i := int32(0); i < isrLength; i++ {
		isr, err := decoder.GetInt32()
		if err != nil {
			return err
		}
		this.Isr[i] = isr
	}

	return nil
}
