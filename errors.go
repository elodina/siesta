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

import "errors"

//signals that an end of file or stream has been reached unexpectedly
var EOF = errors.New("End of file reached")

//happens when given value to decode as string has either negative or undecodable length
var InvalidStringLength = errors.New("Invalid string length")

//happens when given value to decode as bytes has either negative or undecodable length
var InvalidBytesLength = errors.New("Invalid bytes length")

var NoError = errors.New("No error - it worked!")

var Unknown = errors.New("An unexpected server error")

var OffsetOutOfRange = errors.New("The requested offset is outside the range of offsets maintained by the server for the given topic/partition.")

var InvalidMessage = errors.New("Message contents does not match its CRC")

var UnknownTopicOrPartition = errors.New("This request is for a topic or partition that does not exist on this broker.")

var InvalidMessageSize = errors.New("The message has a negative size")

var LeaderNotAvailable = errors.New("In the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes.")

var NotLeaderForPartition = errors.New("You've just attempted to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.")

var RequestTimedOut = errors.New("Request exceeds the user-specified time limit in the request.")

var BrokerNotAvailable = errors.New("Broker is likely not alive.")

var ReplicaNotAvailable = errors.New("Replica is expected on a broker, but is not (this can be safely ignored).")

var MessageSizeTooLarge = errors.New("You've just attempted to produce a message of size larger than broker is allowed to accept.")

var StaleControllerEpochCode = errors.New("Broker-to-broker communication fault.")

var OffsetMetadataTooLargeCode = errors.New("You've jsut specified a string larger than configured maximum for offset metadata.")

var OffsetsLoadInProgressCode = errors.New("Offset loading is in progress. (Usually happens after a leader change for that offsets topic partition).")

var ConsumerCoordinatorNotAvailableCode = errors.New("Offsets topic has not yet been created.")

var NotCoordinatorForConsumerCode = errors.New("There is no coordinator for this consumer.")

var BrokerErrors = map[int]error{
	-1: Unknown,
	0:  NoError,
	1:  OffsetOutOfRange,
	2:  InvalidMessage,
	3:  UnknownTopicOrPartition,
	4:  InvalidMessageSize,
	5:  LeaderNotAvailable,
	6:  NotLeaderForPartition,
	7:  RequestTimedOut,
	8:  BrokerNotAvailable,
	9:  ReplicaNotAvailable,
	10: MessageSizeTooLarge,
	11: StaleControllerEpochCode,
	12: OffsetMetadataTooLargeCode,
	14: OffsetsLoadInProgressCode,
	15: ConsumerCoordinatorNotAvailableCode,
	16: NotCoordinatorForConsumerCode,
}
