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

import "errors"

//signals that an end of file or stream has been reached unexpectedly
var EOF = errors.New("End of file reached")

//happens when given value to decode as string has either negative or undecodable length
var InvalidStringLength = errors.New("Invalid string length")

//happens when given value to decode as bytes has either negative or undecodable length
var InvalidBytesLength = errors.New("Invalid bytes length")
