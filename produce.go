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

type ProduceRequest struct {
	RequiredAcks int16
	Timeout      int32
	Messages     map[string]map[int32][]*MessageAndOffset
}

func (this *ProduceRequest) Key() int16 {
	return 0
}

func (this *ProduceRequest) Version() int16 {
	return 0
}

func (this *ProduceRequest) Write(encoder Encoder) {
	encoder.WriteInt16(this.RequiredAcks)
	encoder.WriteInt32(this.Timeout)
	encoder.WriteInt32(int32(len(this.Messages)))

	for topic, partitionData := range this.Messages {
		encoder.WriteString(topic)
		encoder.WriteInt32(int32(len(partitionData)))

		for partition, data := range partitionData {
			encoder.WriteInt32(partition)
			encoder.Reserve(&LengthSlice{})
			for _, messageAndOffset := range data {
				messageAndOffset.Write(encoder)
			}
			encoder.UpdateReserved()
		}
	}
}

func (this *ProduceRequest) AddMessage(topic string, partition int32, message *MessageData) {
	if this.Messages == nil {
		this.Messages = make(map[string]map[int32][]*MessageAndOffset)
	}

	if this.Messages[topic] == nil {
		this.Messages[topic] = make(map[int32][]*MessageAndOffset)
	}

	this.Messages[topic][partition] = append(this.Messages[topic][partition], &MessageAndOffset{Message: message})
}

type ProduceResponse struct {
	Blocks map[string]map[int32]*ProduceResponseData
}

func (this *ProduceResponse) Read(decoder Decoder) *DecodingError {
	this.Blocks = make(map[string]map[int32]*ProduceResponseData)

	topicsLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reason_InvalidProduceTopicsLength)
	}

	for i := int32(0); i < topicsLength; i++ {
		topic, err := decoder.GetString()
		if err != nil {
			return NewDecodingError(err, reason_InvalidProduceTopic)
		}

		blocksForTopic := make(map[int32]*ProduceResponseData)
		this.Blocks[topic] = blocksForTopic

		partitionsLength, err := decoder.GetInt32()
		if err != nil {
			return NewDecodingError(err, reason_InvalidProducePartitionsLength)
		}

		for j := int32(0); j < partitionsLength; j++ {
			partition, err := decoder.GetInt32()
			if err != nil {
				return NewDecodingError(err, reason_InvalidProducePartition)
			}

			data := new(ProduceResponseData)
			errCode, err := decoder.GetInt16()
			if err != nil {
				return NewDecodingError(err, reason_InvalidProduceErrorCode)
			}
			data.Error = BrokerErrors[errCode]

			offset, err := decoder.GetInt64()
			if err != nil {
				return NewDecodingError(err, reason_InvalidProduceOffset)
			}
			data.Offset = offset

			blocksForTopic[partition] = data
		}
	}

	return nil
}

type ProduceResponseData struct {
	Error  error
	Offset int64
}

const (
	reason_InvalidProduceTopicsLength     = "Invalid topics length in ProduceResponse"
	reason_InvalidProduceTopic            = "Invalid topic in ProduceResponse"
	reason_InvalidProducePartitionsLength = "Invalid partitions length in ProduceResponse"
	reason_InvalidProducePartition        = "Invalid partition in ProduceResponse"
	reason_InvalidProduceErrorCode        = "Invalid error code in ProduceResponse"
	reason_InvalidProduceOffset           = "Invalid offset in ProduceResponse"
)
