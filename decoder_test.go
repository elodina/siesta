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
