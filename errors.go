package siesta

import "errors"

//signals that an end of file or stream has been reached unexpectedly
var EOF = errors.New("End of file reached")

//happens when given value to decode as string has either negative or undecodable length
var InvalidStringLength = errors.New("Invalid string length")

//happens when given value to decode as bytes has either negative or undecodable length
var InvalidBytesLength = errors.New("Invalid bytes length")
