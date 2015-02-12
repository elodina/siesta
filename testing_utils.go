package siesta

import (
	"reflect"
	"testing"
)

func assert(t *testing.T, actual interface{}, expected interface{}) {
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, actual %v", expected, actual)
	}
}

func checkErr(t *testing.T, err error) {
	if err != nil {
		t.Error(err)
	}
}
