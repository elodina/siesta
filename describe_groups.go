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

type DescribeGroupsRequest struct {
	Groups []string
}

// Key returns the Kafka API key for DescribeGroupsRequest.
func (*DescribeGroupsRequest) Key() int16 {
	return 15
}

// Version returns the Kafka request version for backwards compatibility.
func (*DescribeGroupsRequest) Version() int16 {
	return 0
}

func (dgr *DescribeGroupsRequest) Write(encoder Encoder) {
	encoder.WriteInt32(int32(len(dgr.Groups)))
	for _, group := range dgr.Groups {
		encoder.WriteString(group)
	}
}

type DescribeGroupsResponse struct {
	Groups []*GroupDescription
}

func (dgr *DescribeGroupsResponse) Read(decoder Decoder) *DecodingError {
	groupsLen, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidDescribeGroupsResponseGroupsLength)
	}

	dgr.Groups = make([]*GroupDescription, groupsLen)
	for i := int32(0); i < groupsLen; i++ {
		groupDescription := new(GroupDescription)
		err := groupDescription.Read(decoder)
		if err != nil {
			return err
		}
		dgr.Groups[i] = groupDescription
	}

	return nil
}

type GroupDescription struct {
	Error        error
	GroupID      string
	State        string
	ProtocolType string
	Protocol     string
	Members      []*GroupMemberDescription
}

func (gd *GroupDescription) Read(decoder Decoder) *DecodingError {
	errCode, err := decoder.GetInt16()
	if err != nil {
		return NewDecodingError(err, reasonInvalidDescribeGroupsResponseGroupError)
	}
	gd.Error = BrokerErrors[errCode]

	gd.GroupID, err = decoder.GetString()
	if err != nil {
		return NewDecodingError(err, reasonInvalidDescribeGroupsResponseGroupID)
	}

	gd.State, err = decoder.GetString()
	if err != nil {
		return NewDecodingError(err, reasonInvalidDescribeGroupsResponseGroupState)
	}

	gd.ProtocolType, err = decoder.GetString()
	if err != nil {
		return NewDecodingError(err, reasonInvalidDescribeGroupsResponseGroupProtocolType)
	}

	gd.Protocol, err = decoder.GetString()
	if err != nil {
		return NewDecodingError(err, reasonInvalidDescribeGroupsResponseGroupProtocol)
	}

	membersLen, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reasonInvalidDescribeGroupsResponseGroupMembersLength)
	}

	gd.Members = make([]*GroupMemberDescription, membersLen)
	for i := int32(0); i < membersLen; i++ {
		member := new(GroupMemberDescription)
		err := member.Read(decoder)
		if err != nil {
			return err
		}

		gd.Members[i] = member
	}

	return nil
}

type GroupMemberDescription struct {
	MemberID         string
	ClientID         string
	ClientHost       string
	MemberMetadata   []byte
	MemberAssignment []byte
}

func (gmd *GroupMemberDescription) Read(decoder Decoder) *DecodingError {
	var err error
	gmd.MemberID, err = decoder.GetString()
	if err != nil {
		return NewDecodingError(err, reasonInvalidDescribeGroupsResponseGroupMemberID)
	}

	gmd.ClientID, err = decoder.GetString()
	if err != nil {
		return NewDecodingError(err, reasonInvalidDescribeGroupsResponseGroupMemberClientID)
	}

	gmd.ClientHost, err = decoder.GetString()
	if err != nil {
		return NewDecodingError(err, reasonInvalidDescribeGroupsResponseGroupMemberClientHost)
	}

	gmd.MemberMetadata, err = decoder.GetBytes()
	if err != nil {
		return NewDecodingError(err, reasonInvalidDescribeGroupsResponseGroupMemberMetadata)
	}

	gmd.MemberAssignment, err = decoder.GetBytes()
	if err != nil {
		return NewDecodingError(err, reasonInvalidDescribeGroupsResponseGroupMemberAssignment)
	}

	return nil
}

const (
	reasonInvalidDescribeGroupsResponseGroupsLength          = "Invalid groups length in DescribeGroupsResponse"
	reasonInvalidDescribeGroupsResponseGroupError            = "Invalid group error code in DescribeGroupsResponse"
	reasonInvalidDescribeGroupsResponseGroupID               = "Invalid group id in DescribeGroupsResponse"
	reasonInvalidDescribeGroupsResponseGroupState            = "Invalid group state in DescribeGroupsResponse"
	reasonInvalidDescribeGroupsResponseGroupProtocolType     = "Invalid group protocol type in DescribeGroupsResponse"
	reasonInvalidDescribeGroupsResponseGroupProtocol         = "Invalid group protocol in DescribeGroupsResponse"
	reasonInvalidDescribeGroupsResponseGroupMembersLength    = "Invalid group members length in DescribeGroupsResponse"
	reasonInvalidDescribeGroupsResponseGroupMemberID         = "Invalid group member id in DescribeGroupsResponse"
	reasonInvalidDescribeGroupsResponseGroupMemberClientID   = "Invalid group member client id in DescribeGroupsResponse"
	reasonInvalidDescribeGroupsResponseGroupMemberClientHost = "Invalid group member client host in DescribeGroupsResponse"
	reasonInvalidDescribeGroupsResponseGroupMemberMetadata   = "Invalid group member metadata in DescribeGroupsResponse"
	reasonInvalidDescribeGroupsResponseGroupMemberAssignment = "Invalid group member assignment in DescribeGroupsResponse"
)
