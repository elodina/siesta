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

import "testing"

var emptyLeaveGroupRequestBytes = []byte{0x00, 0x00, 0x00, 0x00}
var goodLeaveGroupRequestBytes = []byte{0x00, 0x05, 'g', 'r', 'o', 'u', 'p', 0x00, 0x06, 'm', 'e', 'm', 'b', 'e', 'r'}

var errorLeaveGroupResponseBytes = []byte{0x00, 15}
var goodLeaveGroupResponseBytes = []byte{0x00, 0x00}

func TestLeaveGroupRequest(t *testing.T) {
	emptyRequest := new(LeaveGroupRequest)
	testRequest(t, emptyRequest, emptyLeaveGroupRequestBytes)

	goodRequest := new(LeaveGroupRequest)
	goodRequest.GroupID = "group"
	goodRequest.MemberID = "member"
	testRequest(t, goodRequest, goodLeaveGroupRequestBytes)
}

func TestLeaveGroupResponse(t *testing.T) {
	errorLeaveGroupResponse := new(LeaveGroupResponse)
	decode(t, errorLeaveGroupResponse, errorLeaveGroupResponseBytes)
	assert(t, errorLeaveGroupResponse.Error, ErrConsumerCoordinatorNotAvailableCode)

	goodLeaveGroupResponse := new(LeaveGroupResponse)
	decode(t, goodLeaveGroupResponse, goodLeaveGroupResponseBytes)
	assert(t, goodLeaveGroupResponse.Error, ErrNoError)
}
