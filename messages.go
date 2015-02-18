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

type MessageData struct {
	Crc        int32
	MagicByte  int8
	Attributes int8
	Key        []byte
	Value      []byte
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

	//TODO decompression logic should be here, but we'll get back to this later

	return nil
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
	reason_InvalidMessageLength = "Invalid Message length"
	reason_InvalidMessageCRC = "Invalid Message CRC"
	reason_InvalidMessageMagicByte = "Invalid Message magic byte"
	reason_InvalidMessageAttributes = "Invalid Message attributes"
	reason_InvalidMessageKey = "Invalid Message key"
	reason_InvalidMessageValue = "Invalid Message value"
)
