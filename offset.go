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
	Offsets map[string][]*PartitionOffsets
}

func (this *OffsetResponse) Read(decoder Decoder) error {
	offsetsLength, err := decoder.GetInt32()
	if err != nil {
		return err
	}

	this.Offsets = make(map[string][]*PartitionOffsets)
	for i := int32(0); i < offsetsLength; i++ {
		topic, err := decoder.GetString()
		if err != nil {
			return err
		}

		partitionOffsetsLength, err := decoder.GetInt32()
		if err != nil {
			return err
		}

		for j := int32(0); j < partitionOffsetsLength; j++ {
			partitionOffsets := new(PartitionOffsets)
			err := partitionOffsets.Read(decoder)
			if err != nil {
				return err
			}
			this.Offsets[topic] = append(this.Offsets[topic], partitionOffsets)
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
	Partition int32
	Error     error
	Offsets   []int64
}

func (this *PartitionOffsets) Read(decoder Decoder) error {
	partition, err := decoder.GetInt32()
	if err != nil {
		return err
	}
	this.Partition = partition

	errCode, err := decoder.GetInt16()
	if err != nil {
		return err
	}
	this.Error = BrokerErrors[errCode]

	offsetsLength, err := decoder.GetInt32()
	if err != nil {
		return err
	}
	this.Offsets = make([]int64, offsetsLength)
	for i := int32(0); i < offsetsLength; i++ {
		offset, err := decoder.GetInt64()
		if err != nil {
			return err
		}
		this.Offsets[i] = offset
	}

	return nil
}
