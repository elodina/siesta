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
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type BrokerConnection struct {
	broker *Broker
	pool   sync.Pool
}

func NewBrokerConnection(broker *Broker, keepAliveTimeout time.Duration) *BrokerConnection {
	return &BrokerConnection{
		broker: broker,
		pool: sync.Pool{
			New: func() interface{} {
				addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", broker.Host, broker.Port))
				if err != nil {
					return err
				}
				conn, err := net.DialTCP("tcp", nil, addr)
				if err != nil {
					return err
				}

				err = conn.SetKeepAlive(true)
				if err != nil {
					return err
				}
				err = conn.SetKeepAlivePeriod(keepAliveTimeout)
				if err != nil {
					return err
				}

				return conn
			},
		},
	}
}

func (bc *BrokerConnection) GetConnection() (*net.TCPConn, error) {
	maybeConn := bc.pool.Get()
	err, ok := maybeConn.(error)
	if ok {
		return nil, err
	}

	return maybeConn.(*net.TCPConn), nil
}

func (bc *BrokerConnection) ReleaseConnection(conn *net.TCPConn) {
	bc.pool.Put(conn)
}

type Brokers struct {
	brokers          map[int32]*BrokerConnection
	correlationIDs   *CorrelationIDGenerator
	keepAliveTimeout time.Duration
	lock             sync.RWMutex
}

func NewBrokers(keepAliveTimeout time.Duration) *Brokers {
	return &Brokers{
		brokers:          make(map[int32]*BrokerConnection),
		correlationIDs:   new(CorrelationIDGenerator),
		keepAliveTimeout: keepAliveTimeout,
	}
}

func (b *Brokers) Get(id int32) *BrokerConnection {
	b.lock.RLock()
	defer b.lock.RUnlock()

	return b.brokers[id]
}

func (b *Brokers) GetAll() []*BrokerConnection {
	b.lock.RLock()
	defer b.lock.RUnlock()

	brokers := make([]*BrokerConnection, 0, len(b.brokers))
	for _, broker := range b.brokers {
		brokers = append(brokers, broker)
	}

	return brokers
}

func (b *Brokers) Add(broker *Broker) {
	if broker == nil {
		Logger.Warn("Brokers.Add received a nil broker, ignoring")
		return
	}

	b.lock.Lock()
	b.brokers[broker.ID] = NewBrokerConnection(broker, b.keepAliveTimeout)
	b.lock.Unlock()
}

func (b *Brokers) Update(broker *Broker) {
	if broker == nil {
		Logger.Warn("Brokers.Update received a nil broker, ignoring")
		return
	}

	b.lock.Lock()
	defer b.lock.Unlock()

	oldBroker := b.brokers[broker.ID]
	// if the broker with given id does not yet exist or changed location - update it, otherwise do nothing
	if oldBroker == nil || oldBroker.broker.Host != broker.Host || oldBroker.broker.Port != broker.Port {
		b.brokers[broker.ID] = NewBrokerConnection(broker, b.keepAliveTimeout)
		return
	}
}

func (b *Brokers) Remove(id int32) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.brokers[id] == nil {
		Logger.Debug("Tried to remove inexisting broker, ignoring")
		return
	}

	delete(b.brokers, id)
}

func (b *Brokers) NextCorrelationID() int32 {
	return b.correlationIDs.NextCorrelationID()
}

type CorrelationIDGenerator struct {
	id int32
}

func (c *CorrelationIDGenerator) NextCorrelationID() int32 {
	return atomic.AddInt32(&c.id, 1)
}
