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
	"hash/crc32"
)

// Decoder is able to encode actual data into a Kafka wire protocol byte sequence.
type Encoder interface {
	// Writes an int8 to this encoder.
	WriteInt8(int8)

	// Writes an int16 to this encoder.
	WriteInt16(int16)

	// Writes an int32 to this encoder.
	WriteInt32(int32)

	// Writes an int64 to this encoder.
	WriteInt64(int64)

	// Writes a []byte to this encoder.
	WriteBytes([]byte)

	// Writes a string to this encoder.
	WriteString(string)

	// Returns the size in bytes written to this encoder.
	Size() int32

	// Reserves a place for an updatable slice.
	// This is used as an optimization for length and crc fields.
	// The encoder reserves a place for this data and updates it later instead of pre-calculating it and doing redundant work.
	Reserve(UpdatableSlice)

	// Tells the last reserved slice to be updated with new data.
	UpdateReserved()
}

// BinaryEncoder implements Decoder and is able to encode actual data into a Kafka wire protocol byte sequence.
type BinaryEncoder struct {
	buffer []byte
	pos    int

	stack []UpdatableSlice
}

// Creates a new BinaryEncoder that will write into a given []byte.
func NewBinaryEncoder(buffer []byte) *BinaryEncoder {
	return &BinaryEncoder{
		buffer: buffer,
		stack:  make([]UpdatableSlice, 0),
	}
}

// Writes an int8 to this encoder.
func (this *BinaryEncoder) WriteInt8(value int8) {
	this.buffer[this.pos] = byte(value)
	this.pos += 1
}

// Writes an int16 to this encoder.
func (this *BinaryEncoder) WriteInt16(value int16) {
	binary.BigEndian.PutUint16(this.buffer[this.pos:], uint16(value))
	this.pos += 2
}

// Writes an int32 to this encoder.
func (this *BinaryEncoder) WriteInt32(value int32) {
	binary.BigEndian.PutUint32(this.buffer[this.pos:], uint32(value))
	this.pos += 4
}

// Writes an int64 to this encoder.
func (this *BinaryEncoder) WriteInt64(value int64) {
	binary.BigEndian.PutUint64(this.buffer[this.pos:], uint64(value))
	this.pos += 8
}

// Writes a string to this encoder.
func (this *BinaryEncoder) WriteString(value string) {
	this.WriteInt16(int16(len(value)))
	copy(this.buffer[this.pos:], value)
	this.pos += len(value)
}

// Writes a []string to this encoder.
func (this *BinaryEncoder) WriteBytes(value []byte) {
	this.WriteInt32(int32(len(value)))
	copy(this.buffer[this.pos:], value)
	this.pos += len(value)
}

// Returns the size in bytes written to this encoder.
func (this *BinaryEncoder) Size() int32 {
	return int32(len(this.buffer))
}

// Reserves a place for an updatable slice.
func (this *BinaryEncoder) Reserve(slice UpdatableSlice) {
	slice.SetPosition(this.pos)
	this.stack = append(this.stack, slice)
	this.pos += slice.GetReserveLength()
}

// Tells the last reserved slice to be updated with new data.
func (this *BinaryEncoder) UpdateReserved() {
	stackLength := len(this.stack) - 1
	slice := this.stack[stackLength]
	this.stack = this.stack[:stackLength]

	slice.Update(this.buffer[slice.GetPosition():this.pos])
}

// SizingEncoder is used to determine the size for []byte that will hold the actual encoded data.
// This is used as an optimization as it is cheaper to run once and determine the size instead of growing the slice dynamically.
type SizingEncoder struct {
	size int
}

// Creates a new SizingEncoder
func NewSizingEncoder() *SizingEncoder {
	return &SizingEncoder{}
}

// Writes an int8 to this encoder.
func (this *SizingEncoder) WriteInt8(int8) {
	this.size += 1
}

// Writes an int16 to this encoder.
func (this *SizingEncoder) WriteInt16(int16) {
	this.size += 2
}

// Writes an int32 to this encoder.
func (this *SizingEncoder) WriteInt32(int32) {
	this.size += 4
}

// Writes an int64 to this encoder.
func (this *SizingEncoder) WriteInt64(int64) {
	this.size += 8
}

// Writes a string to this encoder.
func (this *SizingEncoder) WriteString(value string) {
	this.WriteInt16(int16(len(value)))
	this.size += len(value)
}

// Writes a []byte to this encoder.
func (this *SizingEncoder) WriteBytes(value []byte) {
	this.WriteInt32(int32(len(value)))
	this.size += len(value)
}

// Returns the size in bytes written to this encoder.
func (this *SizingEncoder) Size() int32 {
	return int32(this.size)
}

// Reserves a place for an updatable slice.
func (this *SizingEncoder) Reserve(slice UpdatableSlice) {
	this.size += slice.GetReserveLength()
}

// Tells the last reserved slice to be updated with new data.
func (this *SizingEncoder) UpdateReserved() {
	//do nothing
}

// UpdatableSlice is an interface that is used when the encoder has to write the value based on bytes that are not yet written (e.g. calculate the CRC of the message).
type UpdatableSlice interface {
	// Returns the length to reserve for this slice.
	GetReserveLength() int

	// Set the current position within the encoder to be updated later.
	SetPosition(int)

	// Get the position within the encoder to be updated later.
	GetPosition() int

	// Update this slice. At this point all necessary data should be written to encoder.
	Update([]byte)
}

// LengthSlice is used to determine the length of upcoming message.
type LengthSlice struct {
	pos   int
	slice []byte
}

// Returns the length to reserve for this slice.
func (this *LengthSlice) GetReserveLength() int {
	return 4
}

// Set the current position within the encoder to be updated later.
func (this *LengthSlice) SetPosition(pos int) {
	this.pos = pos
}

// Get the position within the encoder to be updated later.
func (this *LengthSlice) GetPosition() int {
	return this.pos
}

// Update this slice. At this point all necessary data should be written to encoder.
func (this *LengthSlice) Update(slice []byte) {
	binary.BigEndian.PutUint32(slice, uint32(len(slice)-this.GetReserveLength()))
}

// CrcSlice is used to calculate the CRC32 value of the message.
type CrcSlice struct {
	pos int
}

// Returns the length to reserve for this slice.
func (this *CrcSlice) GetReserveLength() int {
	return 4
}

// Set the current position within the encoder to be updated later.
func (this *CrcSlice) SetPosition(pos int) {
	this.pos = pos
}

// Update this slice. At this point all necessary data should be written to encoder.
func (this *CrcSlice) GetPosition() int {
	return this.pos
}

// Update this slice. At this point all necessary data should be written to encoder.
func (this *CrcSlice) Update(slice []byte) {
	//TODO https://github.com/Shopify/sarama/issues/255 - maybe port the mentioned CRC algo?
	crc := crc32.ChecksumIEEE(slice[this.GetReserveLength():])
	binary.BigEndian.PutUint32(slice, crc)
}
