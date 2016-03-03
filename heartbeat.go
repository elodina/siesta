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

type HeartbeatRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
}

// Key returns the Kafka API key for HeartbeatRequest.
func (*HeartbeatRequest) Key() int16 {
	return 12
}

// Version returns the Kafka request version for backwards compatibility.
func (*HeartbeatRequest) Version() int16 {
	return 0
}

func (hr *HeartbeatRequest) Write(encoder Encoder) {
	encoder.WriteString(hr.GroupID)
	encoder.WriteInt32(hr.GenerationID)
	encoder.WriteString(hr.MemberID)
}

type HeartbeatResponse struct {
	Error error
}

func (hr *HeartbeatResponse) Read(decoder Decoder) *DecodingError {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reasonInvalidHeartbeatResponseErrorCode)
	}

	hr.Error = BrokerErrors[errCode]
	return nil
}

var reasonInvalidHeartbeatResponseErrorCode = "Invalid error code in HeartbeatResponse"
