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

type OffsetFetchRequest struct {
	ConsumerGroup string
	Offsets       map[string][]int32
}

func NewOffsetFetchRequest(group string) *OffsetFetchRequest {
	return &OffsetFetchRequest{ConsumerGroup: group}
}

func (this *OffsetFetchRequest) Key() int16 {
	return 9
}

func (this *OffsetFetchRequest) Version() int16 {
	return 0
}

func (this *OffsetFetchRequest) Write(encoder Encoder) {
	encoder.WriteString(this.ConsumerGroup)
	encoder.WriteInt32(int32(len(this.Offsets)))

	for topic, partitions := range this.Offsets {
		encoder.WriteString(topic)
		encoder.WriteInt32(int32(len(partitions)))

		for _, partition := range partitions {
			encoder.WriteInt32(partition)
		}
	}
}

func (this *OffsetFetchRequest) AddOffset(topic string, partition int32) {
	if this.Offsets == nil {
		this.Offsets = make(map[string][]int32)
	}

	this.Offsets[topic] = append(this.Offsets[topic], partition)
}

type OffsetFetchResponse struct {
	Offsets map[string]map[int32]*FetchedOffset
}

func (this *OffsetFetchResponse) Read(decoder Decoder) *DecodingError {
	offsetsLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reason_InvalidOffsetsMapLength)
	}

	this.Offsets = make(map[string]map[int32]*FetchedOffset)
	for i := int32(0); i < offsetsLength; i++ {
		topic, err := decoder.GetString()
		if err != nil {
			return NewDecodingError(err, reason_InvalidOffsetFetchResponseTopic)
		}

		offsetsForTopic := make(map[int32]*FetchedOffset)
		this.Offsets[topic] = offsetsForTopic

		partitionsLength, err := decoder.GetInt32()
		if err != nil {
			return NewDecodingError(err, reason_InvalidOffsetFetchResponsePartitionsLength)
		}

		for j := int32(0); j < partitionsLength; j++ {
			partition, err := decoder.GetInt32()
			if err != nil {
				return NewDecodingError(err, reason_InvalidOffsetFetchResponsePartition)
			}

			fetchedOffset := new(FetchedOffset)
			if err := fetchedOffset.Read(decoder); err != nil {
				return err
			}

			offsetsForTopic[partition] = fetchedOffset
		}
	}

	return nil
}

type FetchedOffset struct {
	Offset   int64
	Metadata string
	Error    error
}

func (this *FetchedOffset) Read(decoder Decoder) *DecodingError {
	offset, err := decoder.GetInt64()
	if err != nil {
		return NewDecodingError(err, reason_InvalidOffsetFetchResponseOffset)
	}
	this.Offset = offset

	//TODO metadata returned by Kafka is always empty even if was passed in OffsetCommitRequest. bug?
	metadata, err := decoder.GetString()
	if err != nil {
		return NewDecodingError(err, reason_InvalidOffsetFetchResponseMetadata)
	}
	this.Metadata = metadata

	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reason_InvalidOffsetFetchResponseErrorCode)
	}
	this.Error = BrokerErrors[errCode]

	return nil
}

var (
	reason_InvalidOffsetFetchResponseTopic            = "Invalid topic in OffsetFetchResponse"
	reason_InvalidOffsetFetchResponsePartitionsLength = "Invalid length for partition data in OffsetFetchResponse"
	reason_InvalidOffsetFetchResponsePartition        = "Invalid partition in OffsetFetchResponse"
	reason_InvalidOffsetFetchResponseOffset           = "Invalid offset in OffsetFetchResponse"
	reason_InvalidOffsetFetchResponseMetadata         = "Invalid metadata in OffsetFetchResponse"
	reason_InvalidOffsetFetchResponseErrorCode        = "Invalid error code in OffsetFetchResponse"
)
