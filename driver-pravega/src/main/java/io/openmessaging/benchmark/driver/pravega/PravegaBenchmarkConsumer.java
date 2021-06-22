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

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PravegaBenchmarkConsumer implements BenchmarkConsumer {
    private static final Logger log = LoggerFactory.getLogger(PravegaBenchmarkConsumer.class);

    private final ExecutorService executor;
    private final EventStreamReader<ByteBuffer> reader;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private ConcurrentHashMap<String, List<Long>> txnsWithEvents;


    public PravegaBenchmarkConsumer(String streamName, String scopeName, String subscriptionName, ConsumerCallback consumerCallback,
                                    EventStreamClientFactory clientFactory, ReaderGroupManager readerGroupManager,
                                    boolean includeTimestampInEvent, ConcurrentHashMap txnsWithEvents) {
        this.txnsWithEvents = txnsWithEvents;
        log.info("PravegaBenchmarkConsumer: BEGIN: subscriptionName={}, streamName={}", subscriptionName, streamName);
        // Create reader group if it doesn't already exist.
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scopeName, streamName))
                .build();
        readerGroupManager.createReaderGroup(subscriptionName, readerGroupConfig);
        // Create reader.
        reader = clientFactory.createReader(
                UUID.randomUUID().toString(),
                subscriptionName,
                new ByteBufferSerializer(),
                ReaderConfig.builder().disableTimeWindows(true).build());
        // Start a thread to read events.
        this.executor = Executors.newSingleThreadExecutor();
        this.executor.submit(() -> {
           while (!closed.get()) {
               try {
                   final ByteBuffer event = reader.readNextEvent(1000).getEvent();
                   final long eventInTxnReadTs = System.nanoTime();
                   if (event != null) {
                       long eventTimestamp = TimeUnit.MICROSECONDS.toMillis(Long.MAX_VALUE);;
//                       if (includeTimestampInEvent) {
//                           eventTimestamp = event.getLong();
//                       } else {
//                           // This will result in an invalid end-to-end latency measurement of 0 seconds.
//                           eventTimestamp = TimeUnit.MICROSECONDS.toMillis(Long.MAX_VALUE);
//                       }
                       final String test = new String(event.array(), event.arrayOffset() + event.position(), event.remaining(), StandardCharsets.UTF_16);
                       final String[] payloadComponents = test.split("---");
                       if (payloadComponents.length < 2) {
                           continue;
                       }
                       final String txnId = payloadComponents[0];
                       final long txnWriteTs = Long.parseLong(payloadComponents[1]);
                       final long readDurationMs = (eventInTxnReadTs - txnWriteTs) / (long) 1000000;
                       this.txnsWithEvents.compute(txnId, (k,v) -> {
                           if (v == null) {
                               v = new LinkedList<Long>();
                           }
                           v.add(readDurationMs);
                           if (v.size() == 1000) {
                               double summary = 0.0;
                               for (Iterator<Long> i = v.iterator(); i.hasNext();) {
                                   long currentTime = i.next();
                                   summary += currentTime;
                               }
                               double average = (summary) / v.size();
                               log.info("READTXN---ID---" + txnId + "---AVG---" + average);

                           }
                           return v;
                       });
//                       List<Long> readEventsInTxn = this.txnsWithEvents.putIfAbsent(txnId, Arrays.asList(readDurationMs));
//                       if (readEventsInTxn != null) {
//                           readEventsInTxn.add(txnWriteTs);
//                           // TODO: make the value configurable
//                           if (readEventsInTxn.size() == 1000) {
//                               double summary = 0.0;
//                               for (Iterator<Long> i = readEventsInTxn.iterator(); i.hasNext();) {
//                                   long currentTime = i.next();
//                                   summary += currentTime;
//                               }
//                               double average = (summary) / readEventsInTxn.size();
//                               log.info("READTXN---ID---" + txnId + "---AVG---" + average);
//
//                           }
//                       }
//                       log.info("Transaction " + payloadComponents[0] + "---" + readDurationMs);
                       byte[] payload = new byte[event.remaining()];
//                       String payloadStr = new String(payload, StandardCharsets.UTF_16);
//                       event.get(payload);

                       consumerCallback.messageReceived(payload, eventTimestamp);
                   }
               } catch (ReinitializationRequiredException e) {
                   log.error("Exception during read", e);
                   throw e;
               }
           }
        });
    }

    @Override
    public void close() throws Exception {
        closed.set(true);
        this.executor.shutdown();
        this.executor.awaitTermination(1, TimeUnit.MINUTES);
        reader.close();
    }
}
