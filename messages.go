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

func (this *MessageAndOffset) Read(decoder Decoder) error {
	offset, err := decoder.GetInt64()
	if err != nil {
		return err
	}
	this.Offset = offset

	_, err = decoder.GetInt32()
	if err != nil {
		return err
	}

	message := new(MessageData)
	err = message.Read(decoder)
	if err != nil {
		return err
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

func (this *MessageData) Read(decoder Decoder) error {
	crc, err := decoder.GetInt32()
	if err != nil {
		return err
	}
	this.Crc = crc

	magic, err := decoder.GetInt8()
	if err != nil {
		return err
	}
	this.MagicByte = magic

	attributes, err := decoder.GetInt8()
	if err != nil {
		return err
	}
	this.Attributes = attributes

	key, err := decoder.GetBytes()
	if err != nil {
		return err
	}
	this.Key = key

	value, err := decoder.GetBytes()
	if err != nil {
		return err
	}
	this.Value = value

	//TODO decompression logic should be here, but we'll get back to this later

	return nil
}

type Message struct {
	Topic string
	Partition int32
	Offset int64
	Key []byte
	Value []byte
}
