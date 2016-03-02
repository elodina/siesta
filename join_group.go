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

type JoinGroupRequest struct {
	GroupID        string
	SessionTimeout int32
	MemberID       string
	ProtocolType   string
	GroupProtocols []*GroupProtocol
}

// Key returns the Kafka API key for JoinGroupRequest.
func (jgr *JoinGroupRequest) Key() int16 {
	return 11
}

// Version returns the Kafka request version for backwards compatibility.
func (jgr *JoinGroupRequest) Version() int16 {
	return 0
}

func (jgr *JoinGroupRequest) Write(encoder Encoder) {
	encoder.WriteString(jgr.GroupID)
	encoder.WriteInt32(jgr.SessionTimeout)
	encoder.WriteString(jgr.MemberID)
	encoder.WriteString(jgr.ProtocolType)

	encoder.WriteInt32(int32(len(jgr.GroupProtocols)))

	for _, protocol := range jgr.GroupProtocols {
		encoder.WriteString(protocol.ProtocolName)
		encoder.WriteBytes(protocol.ProtocolMetadata)
	}
}

type GroupProtocol struct {
	ProtocolName     string
	ProtocolMetadata []byte
}

type JoinGroupResponse struct {
	Error         error
	GenerationID  int32
	GroupProtocol string
	LeaderID      string
	MemberID      string
	Members       map[string][]byte
}

func (jgr *JoinGroupResponse) Read(decoder Decoder) *DecodingError {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reasonInvalidJoinGroupResponseErrorCode)
	}
	jgr.Error = BrokerErrors[errCode]

	generationID, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidJoinGroupResponseGenerationID)
	}
	jgr.GenerationID = generationID

	groupProtocol, err := decoder.GetString()
	if err != nil {
		return NewDecodingError(err, reasonInvalidJoinGroupResponseGroupProtocol)
	}
	jgr.GroupProtocol = groupProtocol

	leaderID, err := decoder.GetString()
	if err != nil {
		return NewDecodingError(err, reasonInvalidJoinGroupResponseLeaderID)
	}
	jgr.LeaderID = leaderID

	memberID, err := decoder.GetString()
	if err != nil {
		return NewDecodingError(err, reasonInvalidJoinGroupResponseMemberID)
	}
	jgr.MemberID = memberID

	membersLength, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidJoinGroupResponseMembersLength)
	}

	if membersLength == 0 {
		return nil
	}

	jgr.Members = make(map[string][]byte, membersLength)
	for i := 0; i < int(membersLength); i++ {
		memberID, err := decoder.GetString()
		if err != nil {
			return NewDecodingError(err, reasonInvalidJoinGroupResponseMembersMemberID)
		}

		memberMetadata, err := decoder.GetBytes()
		if err != nil {
			return NewDecodingError(err, reasonInvalidJoinGroupResponseMembersMemberMetadata)
		}

		jgr.Members[memberID] = memberMetadata
	}

	return nil
}

var (
	reasonInvalidJoinGroupResponseErrorCode             = "Invalid error code in JoinGroupResponse"
	reasonInvalidJoinGroupResponseGenerationID          = "Invalid generation id in JoinGroupResponse"
	reasonInvalidJoinGroupResponseGroupProtocol         = "Invalid group protocol in JoinGroupResponse"
	reasonInvalidJoinGroupResponseLeaderID              = "Invalid leader id in JoinGroupResponse"
	reasonInvalidJoinGroupResponseMemberID              = "Invalid member id in JoinGroupResponse"
	reasonInvalidJoinGroupResponseMembersLength         = "Invalid members length in JoinGroupResponse"
	reasonInvalidJoinGroupResponseMembersMemberID       = "Invalid member id in members array in JoinGroupResponse"
	reasonInvalidJoinGroupResponseMembersMemberMetadata = "Invalid member metadata in members array in JoinGroupResponse"
)
