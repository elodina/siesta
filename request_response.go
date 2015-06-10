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

// RequestWriter is used to decouple the message header/metadata writing from the actual message.
// It is able to accept a request and encode/write it according to Kafka Wire Protocol format
// adding the correlation id and client id to the request.
type RequestWriter struct {
	correlationID int32
	clientID      string
	request       Request
}

// NewRequestWriter creates a new RequestWriter holding the correlation id, client id and the actual request.
func NewRequestWriter(correlationID int32, clientID string, request Request) *RequestWriter {
	return &RequestWriter{
		correlationID: correlationID,
		clientID:      clientID,
		request:       request,
	}
}

// Size returns the size in bytes needed to write this request, including the length field. This value will be used when allocating memory for a byte array.
func (rw *RequestWriter) Size() int32 {
	encoder := NewSizingEncoder()
	rw.Write(encoder)
	return encoder.Size()
}

// Write writes this RequestWriter into a given Encoder.
func (rw *RequestWriter) Write(encoder Encoder) {
	// write the size of request excluding the length field with length 4
	encoder.WriteInt32(encoder.Size() - 4)
	encoder.WriteInt16(rw.request.Key())
	encoder.WriteInt16(rw.request.Version())
	encoder.WriteInt32(rw.correlationID)
	encoder.WriteString(rw.clientID)
	rw.request.Write(encoder)
}

// Request is a generic interface for any request issued to Kafka. Must be able to identify and write itself.
type Request interface {
	// Writes the Request to the given Encoder.
	Write(Encoder)

	// Returns the Kafka API key for this Request.
	Key() int16

	// Returns the Kafka request version for backwards compatibility.
	Version() int16
}

// Response is a generic interface for any response received from Kafka. Must be able to read itself.
type Response interface {
	// Read the Response from the given Decoder. May return a DecodingError if the response is invalid.
	Read(Decoder) *DecodingError
}

// DecodingError is an error that also holds the information about why it happened.
type DecodingError struct {
	err    error
	reason string
}

// NewDecodingError creates a new DecodingError with a given error message and reason.
func NewDecodingError(err error, reason string) *DecodingError {
	return &DecodingError{err, reason}
}

// Error returns the error message for this DecodingError.
func (de *DecodingError) Error() error {
	return de.err
}

// Reason returns the reason for this DecodingError.
func (de *DecodingError) Reason() string {
	return de.reason
}
