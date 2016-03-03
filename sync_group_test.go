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

var emptySyncGroupRequestBytes = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
var goodSyncGroupRequestBytes = []byte{0x00, 0x03, 'f', 'o', 'o', 0x00, 0x00, 0x00, 0x03, 0x00, 0x03, 'b', 'a', 'r', 0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 'f', 'o', 'o', 0x00, 0x00, 0x00, 0x01, 0x01}

var errorSyncGroupResponseBytes = []byte{0x00, 27, 0x00, 0x00, 0x00, 0x00}
var goodSyncGroupResponseBytes = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01}

func TestSyncGroupRequest(t *testing.T) {
	emptySyncGroupRequest := new(SyncGroupRequest)
	testRequest(t, emptySyncGroupRequest, emptySyncGroupRequestBytes)

	goodSyncGroupRequest := new(SyncGroupRequest)
	goodSyncGroupRequest.GroupID = "foo"
	goodSyncGroupRequest.GenerationID = 3
	goodSyncGroupRequest.MemberID = "bar"
	goodSyncGroupRequest.GroupAssignment = map[string][]byte{
		"foo": []byte{1},
	}
	testRequest(t, goodSyncGroupRequest, goodSyncGroupRequestBytes)
}

func TestSyncGroupResponse(t *testing.T) {
	errorSyncGroupResponse := new(SyncGroupResponse)
	decode(t, errorSyncGroupResponse, errorSyncGroupResponseBytes)
	assert(t, errorSyncGroupResponse.Error, ErrRebalanceInProgress)

	goodSyncGroupResponse := new(SyncGroupResponse)
	decode(t, goodSyncGroupResponse, goodSyncGroupResponseBytes)
	assert(t, goodSyncGroupResponse.Error, ErrNoError)
	assert(t, goodSyncGroupResponse.MemberAssignment, []byte{1})
}
