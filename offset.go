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

type OffsetRequest struct {
	RequestInfo map[string][]*PartitionOffsetRequestInfo
}

func (this *OffsetRequest) Key() int16 {
	return 2
}

func (this *OffsetRequest) Version() int16 {
	return 0
}

func (this *OffsetRequest) AddPartitionOffsetRequestInfo(topic string, partition int32, time OffsetTime, maxNumOffsets int32) {
	if this.RequestInfo == nil {
		this.RequestInfo = make(map[string][]*PartitionOffsetRequestInfo)
	}

	this.RequestInfo[topic] = append(this.RequestInfo[topic], &PartitionOffsetRequestInfo{Partition: partition, Time: time, MaxNumOffsets: maxNumOffsets})
}

func (this *OffsetRequest) Write(encoder Encoder) {
	//Normal client consumers should always specify ReplicaId as -1 as they have no node id
	encoder.WriteInt32(-1)
	encoder.WriteInt32(int32(len(this.RequestInfo)))

	for topic, partitionOffsetInfos := range this.RequestInfo {
		encoder.WriteString(topic)
		encoder.WriteInt32(int32(len(partitionOffsetInfos)))
		for _, info := range partitionOffsetInfos {
			encoder.WriteInt32(info.Partition)
			encoder.WriteInt64(int64(info.Time))
			encoder.WriteInt32(info.MaxNumOffsets)
		}
	}
}

type OffsetResponse struct {
	Offsets map[string]map[int32]*PartitionOffsets
}

func (this *OffsetResponse) Read(decoder Decoder) *DecodingError {
	offsetsLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reason_InvalidOffsetsLength)
	}

	this.Offsets = make(map[string]map[int32]*PartitionOffsets)
	for i := int32(0); i < offsetsLength; i++ {
		topic, err := decoder.GetString()
		if err != nil {
			return NewDecodingError(err, reason_InvalidOffsetTopic)
		}
		offsetsForTopic := make(map[int32]*PartitionOffsets)
		this.Offsets[topic] = offsetsForTopic

		partitionOffsetsLength, err := decoder.GetInt32()
		if err != nil {
			return NewDecodingError(err, reason_InvalidPartitionOffsetsLength)
		}

		for j := int32(0); j < partitionOffsetsLength; j++ {
			partition, err := decoder.GetInt32()
			if err != nil {
				return NewDecodingError(err, reason_InvalidPartitionOffsetsPartition)
			}

			partitionOffsets := new(PartitionOffsets)
			decodingErr := partitionOffsets.Read(decoder)
			if decodingErr != nil {
				return decodingErr
			}
			this.Offsets[topic][partition] = partitionOffsets
		}
	}

	return nil
}

type OffsetTime int64

const LatestTime OffsetTime = -1
const EarliestTime OffsetTime = -2

type PartitionOffsetRequestInfo struct {
	Partition     int32
	Time          OffsetTime
	MaxNumOffsets int32
}

type PartitionOffsets struct {
	Error     error
	Offsets   []int64
}

func (this *PartitionOffsets) Read(decoder Decoder) *DecodingError {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reason_InvalidPartitionOffsetsErrorCode)
	}
	this.Error = BrokerErrors[errCode]

	offsetsLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reason_InvalidPartitionOffsetsOffsetsLength)
	}
	this.Offsets = make([]int64, offsetsLength)
	for i := int32(0); i < offsetsLength; i++ {
		offset, err := decoder.GetInt64()
		if err != nil {
			return NewDecodingError(err, reason_InvalidPartitionOffset)
		}
		this.Offsets[i] = offset
	}

	return nil
}

var (
	reason_InvalidOffsetsLength                 = "Invalid length for Offsets field"
	reason_InvalidOffsetTopic                   = "Invalid topic in offset map"
	reason_InvalidPartitionOffsetsLength        = "Invalid length for partition offsets field"
	reason_InvalidPartitionOffsetsPartition     = "Invalid partition in partition offset"
	reason_InvalidPartitionOffsetsErrorCode     = "Invalid error code in partition offset"
	reason_InvalidPartitionOffsetsOffsetsLength = "Invalid length for offsets field in partition offset"
	reason_InvalidPartitionOffset               = "Invalid offset in partition offset"
)
