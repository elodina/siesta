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

type RequestWriter struct {
	correlationId int32
	clientId      string
	request       Request
}

func NewRequestWriter(correlationId int32, clientId string, request Request) *RequestWriter {
	return &RequestWriter{
		correlationId: correlationId,
		clientId:      clientId,
		request:       request,
	}
}

// Returns the size in bytes needed to write this request, including the length field. This value will be used when allocating memory for a byte array.
func (this *RequestWriter) Size() int32 {
	encoder := NewSizingEncoder()
	this.Write(encoder)
	return encoder.Size()
}

// Writes itself into a given Encoder.
func (this *RequestWriter) Write(encoder Encoder) {
	// write the size of request excluding the length field with length 4
	encoder.WriteInt32(encoder.Size() - 4)
	encoder.WriteInt16(this.request.Key())
	encoder.WriteInt16(this.request.Version())
	encoder.WriteInt32(this.correlationId)
	encoder.WriteString(this.clientId)
	this.request.Write(encoder)
}

type Request interface {
	Write(Encoder)
	Key() int16
	Version() int16
}

type Response interface {
	Read(Decoder) error
}
