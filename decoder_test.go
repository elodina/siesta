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

import "testing"

var encodedString = []byte{0x00, 0x03, 'f', 'o', 'o'}

func TestInt8Decoding(t *testing.T) {
	bytes := make([]byte, 0)
	for i := -100; i < 100; i++ {
		bytes = append(bytes, byte(i))
	}
	decoder := NewBinaryDecoder(bytes)
	for i := -100; i < 100; i++ {
		value, err := decoder.GetInt8()
		checkErr(t, err)

		assert(t, value, int8(i))
	}
}

func TestStringDecoding(t *testing.T) {
	decoder := NewBinaryDecoder(encodedString)
	foo, err := decoder.GetString()
	if err != nil {
		t.Error(err)
	}

	assert(t, foo, "foo")
}
