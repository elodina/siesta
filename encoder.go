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

import (
	"encoding/binary"
)

type Encoder interface {
	WriteInt8(int8)
	WriteInt16(int16)
	WriteInt32(int32)
	WriteInt64(int64)
	WriteBytes([]byte)
	WriteString(string)
}

type BinaryEncoder struct {
	buffer []byte
	pos    int
}

func NewBinaryEncoder(buffer []byte) *BinaryEncoder {
	return &BinaryEncoder{
		buffer: buffer,
	}
}

func (this *BinaryEncoder) WriteInt8(value int8) {
	this.buffer[this.pos] = byte(value)
	this.pos += 1
	//	this.writer.Write([]byte{byte(value)})
}

func (this *BinaryEncoder) WriteInt16(value int16) {
	//	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(this.buffer[this.pos:], uint16(value))
	//	this.writer.Write(buf)
	this.pos += 2
}

func (this *BinaryEncoder) WriteInt32(value int32) {
	//	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(this.buffer[this.pos:], uint32(value))
	//	this.writer.Write(buf)
	this.pos += 4
}

func (this *BinaryEncoder) WriteInt64(value int64) {
	//	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(this.buffer[this.pos:], uint64(value))
	//	this.writer.Write(buf)
	this.pos += 8
}

func (this *BinaryEncoder) WriteString(value string) {
	this.WriteInt16(int16(len(value)))
	copy(this.buffer[this.pos:], value)
	this.pos += len(value)
	//	this.writer.Write([]byte(value))
}

func (this *BinaryEncoder) WriteBytes(value []byte) {
	this.WriteInt32(int32(len(value)))
	//	this.writer.Write(value)
	copy(this.buffer[this.pos:], value)
	this.pos += len(value)
}
