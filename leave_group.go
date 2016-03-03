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

type LeaveGroupRequest struct {
	GroupID  string
	MemberID string
}

// Key returns the Kafka API key for LeaveGroupRequest.
func (*LeaveGroupRequest) Key() int16 {
	return 13
}

// Version returns the Kafka request version for backwards compatibility.
func (*LeaveGroupRequest) Version() int16 {
	return 0
}

func (lgr *LeaveGroupRequest) Write(encoder Encoder) {
	encoder.WriteString(lgr.GroupID)
	encoder.WriteString(lgr.MemberID)
}

type LeaveGroupResponse struct {
	Error error
}

func (lgr *LeaveGroupResponse) Read(decoder Decoder) *DecodingError {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reasonInvalidLeaveGroupResponseErrorCode)
	}
	lgr.Error = BrokerErrors[errCode]
	return nil
}

var reasonInvalidLeaveGroupResponseErrorCode = "Invalid error code in LeaveGroupResponse"
