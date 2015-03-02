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

// Decoder is able to decode a Kafka wire protocol message into actual data.
type Decoder interface {
	// Gets an int8 from this decoder. Returns EOF if end of stream is reached.
	GetInt8() (int8, error)

	// Gets an int16 from this decoder. Returns EOF if end of stream is reached.
	GetInt16() (int16, error)

	// Gets an int32 from this decoder. Returns EOF if end of stream is reached.
	GetInt32() (int32, error)

	// Gets an int64 from this decoder. Returns EOF if end of stream is reached.
	GetInt64() (int64, error)

	// Gets a []byte from this decoder. Returns EOF if end of stream is reached.
	GetBytes() ([]byte, error)

	// Gets a string from this decoder. Returns EOF if end of stream is reached.
	GetString() (string, error)

	// Tells how many bytes left unread in this decoder.
	Remaining() int
}

// BinaryDecoder implements Decoder and is able to decode a Kafka wire protocol message into actual data.
type BinaryDecoder struct {
	raw []byte
	pos int
}

// Creates a new BinaryDecoder that will decode a given []byte.
func NewBinaryDecoder(raw []byte) *BinaryDecoder {
	return &BinaryDecoder{
		raw: raw,
	}
}

// Gets an int8 from this decoder. Returns EOF if end of stream is reached.
func (this *BinaryDecoder) GetInt8() (int8, error) {
	if this.Remaining() < 1 {
		this.pos = len(this.raw)
		return -1, EOF
	}
	value := int8(this.raw[this.pos])
	this.pos += 1
	return value, nil
}

// Gets an int16 from this decoder. Returns EOF if end of stream is reached.
func (this *BinaryDecoder) GetInt16() (int16, error) {
	if this.Remaining() < 2 {
		this.pos = len(this.raw)
		return -1, EOF
	}
	value := int16(binary.BigEndian.Uint16(this.raw[this.pos:]))
	this.pos += 2
	return value, nil
}

// Gets an int32 from this decoder. Returns EOF if end of stream is reached.
func (this *BinaryDecoder) GetInt32() (int32, error) {
	if this.Remaining() < 4 {
		this.pos = len(this.raw)
		return -1, EOF
	}
	value := int32(binary.BigEndian.Uint32(this.raw[this.pos:]))
	this.pos += 4
	return value, nil
}

// Gets an int64 from this decoder. Returns EOF if end of stream is reached.
func (this *BinaryDecoder) GetInt64() (int64, error) {
	if this.Remaining() < 8 {
		this.pos = len(this.raw)
		return -1, EOF
	}
	value := int64(binary.BigEndian.Uint64(this.raw[this.pos:]))
	this.pos += 8
	return value, nil
}

// Gets a []byte from this decoder. Returns EOF if end of stream is reached.
func (this *BinaryDecoder) GetBytes() ([]byte, error) {
	l, err := this.GetInt32()

	if err != nil || l < -1 {
		return nil, EOF
	}

	length := int(l)

	switch {
	case length == -1:
		return nil, nil
	case length == 0:
		return make([]byte, 0), nil
	case length > this.Remaining():
		this.pos = len(this.raw)
		return nil, EOF
	}
	value := this.raw[this.pos : this.pos+length]
	this.pos += length
	return value, nil
}

// Gets a string from this decoder. Returns EOF if end of stream is reached.
func (this *BinaryDecoder) GetString() (string, error) {
	l, err := this.GetInt16()

	if err != nil || l < -1 {
		return "", EOF
	}

	length := int(l)

	switch {
	case length < 1:
		return "", nil
	case length > this.Remaining():
		this.pos = len(this.raw)
		return "", EOF
	}
	value := string(this.raw[this.pos : this.pos+length])
	this.pos += length
	return value, nil
}

// Tells how many bytes left unread in this decoder.
func (this *BinaryDecoder) Remaining() int {
	return len(this.raw) - this.pos
}
