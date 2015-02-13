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

type Request interface {
	Write(Encoder)
}

type TopicMetadataRequest struct {
	Version       int16
	CorrelationId int32
	ClientId      string
	Topics        []string
}

func NewTopicMetadataRequest(topics []string, correlationId int32) *TopicMetadataRequest {
	return &TopicMetadataRequest{
		Version:       0,
		CorrelationId: correlationId,
		ClientId:      "",
		Topics:        topics,
	}
}

func (this *TopicMetadataRequest) Write(encoder Encoder) {
	encoder.WriteInt16(this.Version)
	encoder.WriteInt32(this.CorrelationId)
	encoder.WriteString(this.ClientId)
	encoder.WriteInt32(int32(len(this.Topics)))
	for _, topic := range this.Topics {
		encoder.WriteString(topic)
	}
}
