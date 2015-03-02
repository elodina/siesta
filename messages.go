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
	"bytes"
	"compress/gzip"
	"io/ioutil"
)

type CompressionCodec int

const (
	CompressionNone   CompressionCodec = 0
	CompressionGZip   CompressionCodec = 1
	CompressionSnappy CompressionCodec = 2
	CompressionLZ4    CompressionCodec = 3
)

const compressionCodecMask int8 = 3

type MessageAndOffset struct {
	Offset  int64
	Message *MessageData
}

func (this *MessageAndOffset) Read(decoder Decoder) *DecodingError {
	offset, err := decoder.GetInt64()
	if err != nil {
		return NewDecodingError(err, reason_InvalidMessageAndOffsetOffset)
	}
	this.Offset = offset

	_, err = decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reason_InvalidMessageLength)
	}

	message := new(MessageData)
	decodingErr := message.Read(decoder)
	if decodingErr != nil {
		return decodingErr
	}
	this.Message = message

	return nil
}

func (this *MessageAndOffset) Write(encoder Encoder) {
	encoder.WriteInt64(this.Offset)
	encoder.Reserve(&LengthSlice{})
	this.Message.Write(encoder)
	encoder.UpdateReserved()
}

func ReadMessageSet(decoder Decoder) ([]*MessageAndOffset, *DecodingError) {
	messages := make([]*MessageAndOffset, 0)
	for decoder.Remaining() > 0 {
		messageAndOffset := new(MessageAndOffset)
		err := messageAndOffset.Read(decoder)
		if err != nil {
			if err.Error() != EOF {
				return nil, err
			}
			continue
		}
		messages = append(messages, messageAndOffset)
	}

	return messages, nil
}

type MessageData struct {
	Crc        int32
	MagicByte  int8
	Attributes int8
	Key        []byte
	Value      []byte

	Nested []*MessageAndOffset
}

func (this *MessageData) Read(decoder Decoder) *DecodingError {
	crc, err := decoder.GetInt32()
	if err != nil {
		return NewDecodingError(err, reason_InvalidMessageCRC)
	}
	this.Crc = crc

	magic, err := decoder.GetInt8()
	if err != nil {
		return NewDecodingError(err, reason_InvalidMessageMagicByte)
	}
	this.MagicByte = magic

	attributes, err := decoder.GetInt8()
	if err != nil {
		return NewDecodingError(err, reason_InvalidMessageAttributes)
	}
	this.Attributes = attributes

	key, err := decoder.GetBytes()
	if err != nil {
		return NewDecodingError(err, reason_InvalidMessageKey)
	}
	this.Key = key

	value, err := decoder.GetBytes()
	if err != nil {
		return NewDecodingError(err, reason_InvalidMessageValue)
	}
	this.Value = value

	compressionCodec := CompressionCodec(this.Attributes & compressionCodecMask)
	switch compressionCodec {
	case CompressionNone:
	case CompressionGZip:
		{
			if this.Value == nil {
				return NewDecodingError(NoDataToUncompress, reason_NoGzipData)
			}
			reader, err := gzip.NewReader(bytes.NewReader(this.Value))
			if err != nil {
				return NewDecodingError(err, reason_MalformedGzipData)
			}
			if this.Value, err = ioutil.ReadAll(reader); err != nil {
				return NewDecodingError(err, reason_MalformedGzipData)
			}

			messages, decodingErr := ReadMessageSet(NewBinaryDecoder(this.Value))
			if decodingErr != nil {
				return decodingErr
			}
			this.Nested = messages
		}
	case CompressionSnappy:
		panic("Not implemented yet")
	case CompressionLZ4:
		panic("Not implemented yet")
	}

	return nil
}

//TODO compress and write if needed
func (this *MessageData) Write(encoder Encoder) {
	encoder.Reserve(&CrcSlice{})
	encoder.WriteInt8(this.MagicByte)
	encoder.WriteInt8(this.Attributes)
	encoder.WriteBytes(this.Key)
	encoder.WriteBytes(this.Value)
	encoder.UpdateReserved()
}

type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

var (
	reason_InvalidMessageAndOffsetOffset = "Invalid offset in MessageAndOffset"
	reason_InvalidMessageLength          = "Invalid Message length"
	reason_InvalidMessageCRC             = "Invalid Message CRC"
	reason_InvalidMessageMagicByte       = "Invalid Message magic byte"
	reason_InvalidMessageAttributes      = "Invalid Message attributes"
	reason_InvalidMessageKey             = "Invalid Message key"
	reason_InvalidMessageValue           = "Invalid Message value"
	reason_NoGzipData                    = "No data to uncompress for GZip encoded message"
	reason_MalformedGzipData             = "Malformed GZip encoded message"
)
