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

type OffsetCommitRequest struct {
	ConsumerGroup string
	Offsets map[string]map[int32]*OffsetAndMetadata
}

func NewOffsetCommitRequest(group string) *OffsetCommitRequest {
	return &OffsetCommitRequest{ConsumerGroup: group}
}

func (this *OffsetCommitRequest) Key() int16 {
	return 8
}

func (this *OffsetCommitRequest) Version() int16 {
	return 0
}

func (this *OffsetCommitRequest) Write(encoder Encoder) {
	encoder.WriteString(this.ConsumerGroup)
	encoder.WriteInt32(int32(len(this.Offsets)))

	for topic, partitionOffsetAndMetadata := range this.Offsets {
		encoder.WriteString(topic)
		encoder.WriteInt32(int32(len(partitionOffsetAndMetadata)))
		for partition, offsetAndMetadata := range partitionOffsetAndMetadata {
			encoder.WriteInt32(partition)
			encoder.WriteInt64(offsetAndMetadata.Offset)
			encoder.WriteInt64(offsetAndMetadata.TimeStamp)
			encoder.WriteString(offsetAndMetadata.Metadata)
		}
	}
}

func (this *OffsetCommitRequest) AddOffset(topic string, partition int32, offset int64, timestamp int64, metadata string) {
	if this.Offsets == nil {
		this.Offsets = make(map[string]map[int32]*OffsetAndMetadata)
	}

	partitionOffsetAndMetadata, exists := this.Offsets[topic]
	if !exists {
		this.Offsets[topic] = make(map[int32]*OffsetAndMetadata)
		partitionOffsetAndMetadata = this.Offsets[topic]
	}

	partitionOffsetAndMetadata[partition] = &OffsetAndMetadata{Offset: offset, TimeStamp: timestamp, Metadata: metadata}
}

type OffsetCommitResponse struct {
	Offsets map[string]map[int32]error
}

func (this *OffsetCommitResponse) Read(decoder Decoder) *DecodingError {
	this.Offsets = make(map[string]map[int32]error)

	offsetsLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reason_InvalidOffsetsMapLength)
	}

	for i := int32(0); i < offsetsLength; i++ {
		topic, err := decoder.GetString()
		if err != nil {
			return NewDecodingError(err, reason_InvalidOffsetsTopic)
		}
		offsetsForTopic := make(map[int32]error)
		this.Offsets[topic] = offsetsForTopic

		partitionsLength, err := decoder.GetInt32()
		if err != nil {
			return NewDecodingError(err, reason_InvalidOffsetsPartitionsLength)
		}

		for j := int32(0); j < partitionsLength; j++ {
			partition, err := decoder.GetInt32()
			if err != nil {
				return NewDecodingError(err, reason_InvalidOffsetsPartition)
			}

			errCode, err := decoder.GetInt16()
			if err != nil {
				return NewDecodingError(err, reason_InvalidOffsetsErrorCode)
			}

			offsetsForTopic[partition] = BrokerErrors[errCode]
		}
	}

	return nil
}

type OffsetAndMetadata struct {
	Offset int64
	TimeStamp int64
	Metadata string
}

var (
	reason_InvalidOffsetsMapLength = "Invalid length for Offsets field"
	reason_InvalidOffsetsTopic = "Invalid topic in OffsetCommitResponse"
	reason_InvalidOffsetsPartitionsLength = "Invalid length for partitions in OffsetCommitResponse"
	reason_InvalidOffsetsPartition = "Invalid partition in OffsetCommitResponse"
	reason_InvalidOffsetsErrorCode = "Invalid error code in OffsetCommitResponse"
)
