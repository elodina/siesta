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
	"sort"
	"sync"
	"time"
)

type Metadata struct {
	Brokers         *Brokers
	connector       Connector
	metadataTTL     time.Duration
	metadata        map[string]map[int32]int32
	metadataExpires map[string]time.Time
	metadataLock    sync.RWMutex
}

func NewMetadata(connector Connector, brokers *Brokers, metadataTTL time.Duration) *Metadata {
	return &Metadata{
		Brokers:         brokers,
		connector:       connector,
		metadataTTL:     metadataTTL,
		metadata:        make(map[string]map[int32]int32),
		metadataExpires: make(map[string]time.Time),
	}
}

func (m *Metadata) Leader(topic string, partition int32) (int32, error) {
	topicMetadata, err := m.TopicMetadata(topic)
	if err != nil {
		return -1, err
	}

	leader, exists := topicMetadata[partition]
	if !exists {
		err := m.Refresh([]string{topic})
		if err != nil {
			m.Invalidate(topic)
			return -1, err
		}

		topicMetadata, err = m.TopicMetadata(topic)
		if err != nil {
			m.Invalidate(topic)
			return -1, err
		}

		leader, exists = topicMetadata[partition]
		if !exists {
			m.Invalidate(topic)
			return -1, ErrFailedToGetLeader
		}
	}

	return leader, nil
}

func (m *Metadata) TopicMetadata(topic string) (map[int32]int32, error) {
	topicMetadata, ttl := m.topicMetadata(topic)

	if topicMetadata == nil || ttl.Add(m.metadataTTL).Before(time.Now()) {
		err := m.refreshIfExpired(topic, ttl)
		if err != nil {
			return nil, err
		}

		topicMetadata, _ = m.topicMetadata(topic)
		if topicMetadata != nil {
			return topicMetadata, nil
		}

		return nil, ErrFailedToGetMetadata
	}

	return topicMetadata, nil
}

func (m *Metadata) PartitionsFor(topic string) ([]int32, error) {
	topicMetadata, err := m.TopicMetadata(topic)
	if err != nil {
		return nil, err
	}

	partitions := make([]int32, 0, len(topicMetadata))
	for partition := range topicMetadata {
		partitions = append(partitions, partition)
	}

	sort.Sort(Int32Slice(partitions))
	return partitions, nil
}

func (m *Metadata) Refresh(topics []string) error {
	Logger.Info("Refreshing metadata for topics %v", topics)
	m.metadataLock.Lock()
	defer m.metadataLock.Unlock()

	return m.refresh(topics)
}

func (m *Metadata) Invalidate(topic string) {
	m.metadataLock.Lock()
	defer m.metadataLock.Unlock()

	m.metadataExpires[topic] = time.Unix(0, 0)
}

func (m *Metadata) topicMetadata(topic string) (map[int32]int32, time.Time) {
	m.metadataLock.RLock()
	defer m.metadataLock.RUnlock()

	metadataCopy := make(map[int32]int32)
	for k, v := range m.metadata[topic] {
		metadataCopy[k] = v
	}
	return metadataCopy, m.metadataExpires[topic]
}

func (m *Metadata) refreshIfExpired(topic string, ttl time.Time) error {
	m.metadataLock.Lock()
	defer m.metadataLock.Unlock()

	if ttl.Add(m.metadataTTL).Before(time.Now()) {
		return m.refresh([]string{topic})
	}

	return nil
}

func (m *Metadata) refresh(topics []string) error {
	topicMetadataResponse, err := m.connector.GetTopicMetadata(topics)
	if err != nil {
		return err
	}

	for _, broker := range topicMetadataResponse.Brokers {
		m.Brokers.Update(broker)
	}

	for _, topicMetadata := range topicMetadataResponse.TopicsMetadata {
		if topicMetadata.Error != ErrNoError {
			return topicMetadata.Error
		}

		partitionLeaders := make(map[int32]int32)
		for _, partitionMetadata := range topicMetadata.PartitionsMetadata {
			if partitionMetadata.Error != ErrNoError {
				return partitionMetadata.Error
			}

			partitionLeaders[partitionMetadata.PartitionID] = partitionMetadata.Leader
		}

		m.metadata[topicMetadata.Topic] = partitionLeaders
		m.metadataExpires[topicMetadata.Topic] = time.Now()
		Logger.Debug("Received metadata: partitions %v for topic %s", partitionLeaders, topicMetadata.Topic)
	}

	return nil
}

type Int32Slice []int32

func (p Int32Slice) Len() int           { return len(p) }
func (p Int32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
