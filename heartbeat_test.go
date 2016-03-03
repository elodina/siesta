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

var emptyHeartbeatRequestBytes = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
var goodHeartbeatRequestBytes = []byte{0x00, 0x05, 'g', 'r', 'o', 'u', 'p', 0x00, 0x00, 0x00, 0x03, 0x00, 0x06, 'm', 'e', 'm', 'b', 'e', 'r'}

var errorHeartbeatResponseBytes = []byte{0x00, 25}
var goodHeartbeatResponseBytes = []byte{0x00, 0x00}

func TestHeartbeatRequest(t *testing.T) {
	emptyHeartbeatRequest := new(HeartbeatRequest)
	testRequest(t, emptyHeartbeatRequest, emptyHeartbeatRequestBytes)

	goodHeartbeatRequest := new(HeartbeatRequest)
	goodHeartbeatRequest.GroupID = "group"
	goodHeartbeatRequest.GenerationID = 3
	goodHeartbeatRequest.MemberID = "member"
	testRequest(t, goodHeartbeatRequest, goodHeartbeatRequestBytes)
}

func TestHeartbeatResponse(t *testing.T) {
	errorHeartbeatResponse := new(HeartbeatResponse)
	decode(t, errorHeartbeatResponse, errorHeartbeatResponseBytes)
	assert(t, errorHeartbeatResponse.Error, ErrUnknownMemberID)

	goodHeartbeatResponse := new(HeartbeatResponse)
	decode(t, goodHeartbeatResponse, goodHeartbeatResponseBytes)
	assert(t, goodHeartbeatResponse.Error, ErrNoError)
}
