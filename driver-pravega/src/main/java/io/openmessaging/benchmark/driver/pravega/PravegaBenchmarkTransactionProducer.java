/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.openmessaging.benchmark.driver.pravega;

import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.micrometer.core.instrument.Timer;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class PravegaBenchmarkTransactionProducer implements BenchmarkProducer {
    private static final Logger log = LoggerFactory.getLogger(PravegaBenchmarkProducer.class);

    private TransactionalEventStreamWriter<ByteBuffer> transactionWriter;

    private final boolean includeTimestampInEvent;

    // If null, a transaction has not been started.
    @GuardedBy("this")
    private Transaction<ByteBuffer> transaction;
    private long txnTime;
    private Timer transactionCommittingTimer;
    private Timer transactionCommittedTimer;
    private final int eventsPerTransaction;
    private int eventCount = 0;
    private ByteBuffer timestampAndPayload;

    public PravegaBenchmarkTransactionProducer(String streamName, EventStreamClientFactory clientFactory,
                                               boolean includeTimestampInEvent, boolean enableConnectionPooling, int eventsPerTransaction, Timer transactionCommiting, Timer transactionCommittedTimer) {
        log.info("PravegaBenchmarkProducer: BEGIN: streamName={}", streamName);
        this.transactionCommittingTimer = transactionCommiting;
        this.transactionCommittedTimer = transactionCommittedTimer;
        txnTime = (long) 0.0;

        final String writerId = UUID.randomUUID().toString();
        transactionWriter = clientFactory.createTransactionalEventWriter(writerId, streamName,
                new ByteBufferSerializer(),
                EventWriterConfig.builder().enableConnectionPooling(enableConnectionPooling).build());
        this.eventsPerTransaction = eventsPerTransaction;
        this.includeTimestampInEvent = includeTimestampInEvent;
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        try {
            if (transaction == null) {
                transaction = transactionWriter.beginTxn();
                txnTime = System.currentTimeMillis();
            }
            if (includeTimestampInEvent) {
                if (timestampAndPayload == null || timestampAndPayload.limit() != Long.BYTES + payload.length) {
                    timestampAndPayload = ByteBuffer.allocate(Long.BYTES + payload.length);
                } else {
                    timestampAndPayload.position(0);
                }
                timestampAndPayload.putLong(System.currentTimeMillis()).put(payload).flip();
                writeEvent(key, timestampAndPayload);
            } else {
                writeEvent(key, ByteBuffer.wrap(payload));
            }
            if (++eventCount >= eventsPerTransaction) {
                eventCount = 0;
                transaction.commit();
                txnTime = System.currentTimeMillis() - txnTime;
                // 1. Record OPEN <-> COMMITING
                transactionCommittingTimer.record(txnTime, TimeUnit.MILLISECONDS);

                // 2. Record COMMITING <-> COMMITED
                transactionCommittedTimer.record(() -> {
                    while(transaction.checkStatus() != Transaction.Status.COMMITTED) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });
                transaction = null;
            }
        } catch (TxnFailedException e) {
            throw new RuntimeException("Transaction Write data failed ", e);
        }
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        return future;
    }

    private void writeEvent(Optional<String> key, ByteBuffer payload) throws TxnFailedException {
        if (key.isPresent()) {
            transaction.writeEvent(key.get(), payload);
        } else {
            transaction.writeEvent(payload);
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (this) {
            if (transaction != null) {
                transaction.abort();
                transaction = null;
            }
        }
        transactionWriter.close();
    }
}
