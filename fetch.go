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

type FetchRequest struct {
	MaxWaitTime int32
	MinBytes    int32
	RequestInfo map[string][]*PartitionFetchInfo
}

func (this *FetchRequest) Write(encoder Encoder) {
	//Normal client consumers should always specify ReplicaId as -1 as they have no node id
	encoder.WriteInt32(-1)
	encoder.WriteInt32(this.MaxWaitTime)
	encoder.WriteInt32(this.MinBytes)
	encoder.WriteInt32(int32(len(this.RequestInfo)))

	for topic, partitionFetchInfos := range this.RequestInfo {
		encoder.WriteString(topic)
		encoder.WriteInt32(int32(len(partitionFetchInfos)))
		for _, info := range partitionFetchInfos {
			encoder.WriteInt32(info.Partition)
			encoder.WriteInt64(info.FetchOffset)
			encoder.WriteInt32(info.FetchSize)
		}
	}
}

func (this *FetchRequest) Key() int16 {
	return 1
}

func (this *FetchRequest) Version() int16 {
	return 0
}

func (this *FetchRequest) AddFetch(topic string, partition int32, offset int64, fetchSize int32) {
	if this.RequestInfo == nil {
		this.RequestInfo = make(map[string][]*PartitionFetchInfo)
	}

	this.RequestInfo[topic] = append(this.RequestInfo[topic], &PartitionFetchInfo{Partition: partition, FetchOffset: offset, FetchSize: fetchSize})
}

type FetchResponse struct {
	Blocks map[string]map[int32]*FetchResponseData
}

func (this *FetchResponse) Read(decoder Decoder) *DecodingError {
	this.Blocks = make(map[string]map[int32]*FetchResponseData)

	blocksLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reason_InvalidBlocksLength)
	}

	for i := int32(0); i < blocksLength; i++ {
		topic, err := decoder.GetString()
		if err != nil {
			return NewDecodingError(err, reason_InvalidBlockTopic)
		}
		this.Blocks[topic] = make(map[int32]*FetchResponseData)

		fetchResponseDataLength, err := decoder.GetInt32()
		if err != nil {
			return NewDecodingError(err, reason_InvalidFetchResponseDataLength)
		}
		for j := int32(0); j < fetchResponseDataLength; j++ {
			partition, err := decoder.GetInt32()
			if err != nil {
				return NewDecodingError(err, reason_InvalidFetchResponseDataPartition)
			}

			fetchResponseData := new(FetchResponseData)
			decodingErr := fetchResponseData.Read(decoder)
			if decodingErr != nil {
				return decodingErr
			}

			this.Blocks[topic][partition] = fetchResponseData
		}
	}

	return nil
}

func (this *FetchResponse) GetMessages() ([]*Message, error) {
	messages := make([]*Message, 0)

	collector := func(topic string, partition int32, offset int64, key []byte, value []byte) {
		messages = append(messages, &Message{
			Topic:     topic,
			Partition: partition,
			Offset:    offset,
			Key:       key,
			Value:     value,
		})
	}

	err := this.CollectMessages(collector)
	return messages, err
}

// CollectMessages traverses this FetchResponse and applies a collector function to each message
// giving the possibility to avoid response -> siesta.Message -> other.Message conversion if necessary.
func (this *FetchResponse) CollectMessages(collector func(topic string, partition int32, offset int64, key []byte, value []byte)) error {
	for topic, partitionAndData := range this.Blocks {
		for partition, data := range partitionAndData {
			if data.Error != NoError {
				return data.Error
			}
			for _, messageAndOffset := range data.Messages {
				if messageAndOffset.Message.Nested != nil {
					for _, nested := range messageAndOffset.Message.Nested {
						collector(topic, partition, nested.Offset, nested.Message.Key, nested.Message.Value)
					}
				} else {
					collector(topic, partition, messageAndOffset.Offset, messageAndOffset.Message.Key, messageAndOffset.Message.Value)
				}
			}
		}
	}

	return nil
}

type PartitionFetchInfo struct {
	Partition   int32
	FetchOffset int64
	FetchSize   int32
}

type FetchResponseData struct {
	Error               error
	HighwaterMarkOffset int64
	Messages            []*MessageAndOffset
}

func (this *FetchResponseData) Read(decoder Decoder) *DecodingError {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reason_InvalidFetchResponseDataErrorCode)
	}
	this.Error = BrokerErrors[errCode]

	highwaterMarkOffset, err := decoder.GetInt64()
	if err != nil {
		return NewDecodingError(err, reason_InvalidFetchResponseDataHighwaterMarkOffset)
	}
	this.HighwaterMarkOffset = highwaterMarkOffset

	if _, err = decoder.GetInt32(); err != nil {
		return NewDecodingError(err, reason_InvalidMessageSetLength)
	}

	messages, decodingErr := ReadMessageSet(decoder)
	if decodingErr != nil {
		return decodingErr
	}
	this.Messages = messages

	return nil
}

var (
	reason_InvalidBlocksLength                         = "Invalid length for Blocks field"
	reason_InvalidBlockTopic                           = "Invalid topic in block"
	reason_InvalidFetchResponseDataLength              = "Invalid length for FetchResponseData field"
	reason_InvalidFetchResponseDataPartition           = "Invalid partition in FetchResponseData"
	reason_InvalidFetchResponseDataErrorCode           = "Invalid error code in FetchResponseData"
	reason_InvalidFetchResponseDataHighwaterMarkOffset = "Invalid highwater mark offset in FetchResponseData"
	reason_InvalidMessageSetLength                     = "Invalid MessageSet length"
)
