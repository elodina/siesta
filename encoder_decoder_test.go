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
	"math/rand"
	"testing"
)

var numValues = 100

func TestInt8EncodingDecoding(t *testing.T) {
	buffer := make([]byte, numValues)
	encoder := NewBinaryEncoder(buffer)

	randValues := make([]int8, numValues)
	for i := 0; i < numValues; i++ {
		randValue := int8(rand.Int())
		encoder.WriteInt8(randValue)
		randValues[i] = randValue
	}

	decoder := NewBinaryDecoder(buffer)
	for i := 0; i < numValues; i++ {
		value, err := decoder.GetInt8()
		checkErr(t, err)
		assert(t, value, randValues[i])
	}
}

func TestInt16EncodingDecoding(t *testing.T) {
	buffer := make([]byte, numValues*2)
	encoder := NewBinaryEncoder(buffer)

	randValues := make([]int16, numValues)
	for i := 0; i < numValues; i++ {
		randValue := int16(rand.Int())
		encoder.WriteInt16(randValue)
		randValues[i] = randValue
	}

	decoder := NewBinaryDecoder(buffer)
	for i := 0; i < numValues; i++ {
		value, err := decoder.GetInt16()
		checkErr(t, err)
		assert(t, value, randValues[i])
	}
}

func TestInt32EncodingDecoding(t *testing.T) {
	buffer := make([]byte, numValues*4)
	encoder := NewBinaryEncoder(buffer)

	randValues := make([]int32, numValues)
	for i := 0; i < numValues; i++ {
		randValue := int32(rand.Int())
		encoder.WriteInt32(randValue)
		randValues[i] = randValue
	}

	decoder := NewBinaryDecoder(buffer)
	for i := 0; i < numValues; i++ {
		value, err := decoder.GetInt32()
		checkErr(t, err)
		assert(t, value, randValues[i])
	}
}

func TestInt64EncodingDecoding(t *testing.T) {
	buffer := make([]byte, numValues*8)
	encoder := NewBinaryEncoder(buffer)

	randValues := make([]int64, numValues)
	for i := 0; i < numValues; i++ {
		randValue := int64(rand.Int())
		encoder.WriteInt64(randValue)
		randValues[i] = randValue
	}

	decoder := NewBinaryDecoder(buffer)
	for i := 0; i < numValues; i++ {
		value, err := decoder.GetInt64()
		checkErr(t, err)
		assert(t, value, randValues[i])
	}
}

func TestStringEncodingDecoding(t *testing.T) {
	buffer := make([]byte, numValues*2+((numValues*(numValues+1))/2))
	encoder := NewBinaryEncoder(buffer)

	randValues := make([]string, numValues)
	for i := 0; i < numValues; i++ {
		randValue := randomString(i)
		encoder.WriteString(randValue)
		randValues[i] = randValue
	}

	decoder := NewBinaryDecoder(buffer)
	for i := 0; i < numValues; i++ {
		value, err := decoder.GetString()
		checkErr(t, err)
		assert(t, value, randValues[i])
	}
}

func TestBytesEncodingDecoding(t *testing.T) {
	buffer := make([]byte, numValues*4+((numValues*(numValues+1))/2))
	encoder := NewBinaryEncoder(buffer)

	randValues := make([][]byte, numValues)
	for i := 0; i < numValues; i++ {
		randValue := randomBytes(i)
		encoder.WriteBytes(randValue)
		randValues[i] = randValue
	}

	decoder := NewBinaryDecoder(buffer)
	for i := 0; i < numValues; i++ {
		value, err := decoder.GetBytes()
		checkErr(t, err)
		assert(t, value, randValues[i])
	}
}
