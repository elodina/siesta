/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package siesta

import (
	"fmt"
	"testing"
)

func TestDefaultConnectorConsume(t *testing.T) {
	t.Skip()

	connector := testConnector()
	messages, err := connector.Consume("siesta-1", 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	for _, message := range messages {
		fmt.Printf("offset: %d, value: %s\n", message.Offset, string(message.Value))
	}
	<-connector.Close()
}

func TestDefaultConnectorTopicMetadata(t *testing.T) {
	t.Skip()

	connector := testConnector()
	_, err := connector.GetTopicMetadata([]string{"test-2"})
	assertFatal(t, err, nil)

	<-connector.Close()
}
