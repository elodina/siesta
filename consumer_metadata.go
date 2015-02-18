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

type ConsumerMetadataRequest struct {
	ConsumerGroup string
}

func NewConsumerMetadataRequest(group string) *ConsumerMetadataRequest {
	return &ConsumerMetadataRequest{ConsumerGroup: group}
}

func (this *ConsumerMetadataRequest) Key() int16 {
	return 10
}

func (this *ConsumerMetadataRequest) Version() int16 {
	return 0
}

func (this *ConsumerMetadataRequest) Write(encoder Encoder) {
	encoder.WriteString(this.ConsumerGroup)
}

type ConsumerMetadataResponse struct {
	Error           error
	CoordinatorId   int32
	CoordinatorHost string
	CoordinatorPort int32
}

func (this *ConsumerMetadataResponse) Read(decoder Decoder) *DecodingError {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reason_InvalidConsumerMetadataErrorCode)
	}
	this.Error = BrokerErrors[errCode]

	coordId, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reason_InvalidConsumerMetadataCoordinatorId)
	}
	this.CoordinatorId = coordId

	coordHost, err := decoder.GetString()
	if err != nil {
		return NewDecodingError(err, reason_InvalidConsumerMetadataCoordinatorHost)
	}
	this.CoordinatorHost = coordHost

	coordPort, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reason_InvalidConsumerMetadataCoordinatorPort)
	}
	this.CoordinatorPort = coordPort

	return nil
}

var (
	reason_InvalidConsumerMetadataErrorCode       = "Invalid error code in consumer metadata"
	reason_InvalidConsumerMetadataCoordinatorId   = "Invalid coordinator id in consumer metadata"
	reason_InvalidConsumerMetadataCoordinatorHost = "Invalid coordinator host in consumer metadata"
	reason_InvalidConsumerMetadataCoordinatorPort = "Invalid coordinator port in consumer metadata"
)
