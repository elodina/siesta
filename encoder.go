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

type Encoder interface {
	WriteInt8(int8)
	WriteInt16(int16)
	WriteInt32(int32)
	WriteInt64(int64)
	WriteBytes([]byte)
	WriteString(string)

	// Returns the size in bytes of this encoder
	Size() int32
	Reserve(UpdatableSlice)
	UpdateReserved()
}

type BinaryEncoder struct {
	buffer []byte
	pos    int

	stack []UpdatableSlice
}

func NewBinaryEncoder(buffer []byte) *BinaryEncoder {
	return &BinaryEncoder{
		buffer: buffer,
		stack:  make([]UpdatableSlice, 0),
	}
}

func (this *BinaryEncoder) WriteInt8(value int8) {
	this.buffer[this.pos] = byte(value)
	this.pos += 1
}

func (this *BinaryEncoder) WriteInt16(value int16) {
	binary.BigEndian.PutUint16(this.buffer[this.pos:], uint16(value))
	this.pos += 2
}

func (this *BinaryEncoder) WriteInt32(value int32) {
	binary.BigEndian.PutUint32(this.buffer[this.pos:], uint32(value))
	this.pos += 4
}

func (this *BinaryEncoder) WriteInt64(value int64) {
	binary.BigEndian.PutUint64(this.buffer[this.pos:], uint64(value))
	this.pos += 8
}

func (this *BinaryEncoder) WriteString(value string) {
	this.WriteInt16(int16(len(value)))
	copy(this.buffer[this.pos:], value)
	this.pos += len(value)
}

func (this *BinaryEncoder) WriteBytes(value []byte) {
	this.WriteInt32(int32(len(value)))
	copy(this.buffer[this.pos:], value)
	this.pos += len(value)
}

func (this *BinaryEncoder) Size() int32 {
	return int32(len(this.buffer))
}

func (this *BinaryEncoder) Reserve(slice UpdatableSlice) {
	length := slice.GetReserveLength()
	slice.SetPosition(this.pos + length)
	slice.Store(this.buffer[this.pos : this.pos+length])
	this.stack = append(this.stack, slice)
	this.pos += length
}

func (this *BinaryEncoder) UpdateReserved() {
	stackLength := len(this.stack) - 1
	slice := this.stack[stackLength]
	this.stack = this.stack[:stackLength]

	slice.Update(this.buffer[slice.GetPosition():this.pos])
}

type SizingEncoder struct {
	size int
}

func NewSizingEncoder() *SizingEncoder {
	return &SizingEncoder{}
}

func (this *SizingEncoder) WriteInt8(int8) {
	this.size += 1
}

func (this *SizingEncoder) WriteInt16(int16) {
	this.size += 2
}

func (this *SizingEncoder) WriteInt32(int32) {
	this.size += 4
}

func (this *SizingEncoder) WriteInt64(int64) {
	this.size += 8
}

func (this *SizingEncoder) WriteString(value string) {
	this.WriteInt16(int16(len(value)))
	this.size += len(value)
}

func (this *SizingEncoder) WriteBytes(value []byte) {
	this.WriteInt32(int32(len(value)))
	this.size += len(value)
}

func (this *SizingEncoder) Size() int32 {
	return int32(this.size)
}

func (this *SizingEncoder) Reserve(slice UpdatableSlice) {
	this.size += slice.GetReserveLength()
}

func (this *SizingEncoder) UpdateReserved() {
	//do nothing
}

type UpdatableSlice interface {
	GetReserveLength() int
	SetPosition(int)
	GetPosition() int
	Store([]byte)
	Update([]byte)
}

type LengthSlice struct {
	pos   int
	slice []byte
}

func (this *LengthSlice) GetReserveLength() int {
	return 4
}

func (this *LengthSlice) SetPosition(pos int) {
	this.pos = pos
}

func (this *LengthSlice) GetPosition() int {
	return this.pos
}

func (this *LengthSlice) Store(slice []byte) {
	this.slice = slice
}

func (this *LengthSlice) Update(rest []byte) {
	binary.BigEndian.PutUint32(this.slice, uint32(len(rest)))
}

type CrcSlice struct {
	pos   int
	slice []byte
}

func (this *CrcSlice) GetReserveLength() int {
	return 4
}

func (this *CrcSlice) SetPosition(pos int) {
	this.pos = pos
}

func (this *CrcSlice) GetPosition() int {
	return this.pos
}

func (this *CrcSlice) Store(slice []byte) {
	this.slice = slice
}

func (this *CrcSlice) Update(rest []byte) {
	//TODO https://github.com/Shopify/sarama/issues/255 - maybe port the mentioned CRC algo?
	crc := crc32.ChecksumIEEE(rest)
	binary.BigEndian.PutUint32(this.slice, crc)
}
