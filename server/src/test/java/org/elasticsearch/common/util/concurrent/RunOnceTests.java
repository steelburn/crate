/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util.concurrent;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class RunOnceTests extends ESTestCase {

    @Test
    public void testRunOnce() {
        final AtomicInteger counter = new AtomicInteger(0);
        final RunOnce runOnce = new RunOnce(counter::incrementAndGet);
        assertThat(runOnce.hasRun()).isFalse();

        runOnce.run();
        assertThat(runOnce.hasRun()).isTrue();
        assertThat(counter.get()).isEqualTo(1);

        runOnce.run();
        assertThat(runOnce.hasRun()).isTrue();
        assertThat(counter.get()).isEqualTo(1);
    }

    @Test
    public void testRunOnceConcurrently() throws InterruptedException {
        final AtomicInteger counter = new AtomicInteger(0);
        final RunOnce runOnce = new RunOnce(counter::incrementAndGet);

        final Thread[] threads = new Thread[between(3, 10)];
        final CountDownLatch latch = new CountDownLatch(1 + threads.length);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                latch.countDown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                runOnce.run();
            });
            threads[i].start();
        }

        latch.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
        assertThat(runOnce.hasRun()).isTrue();
        assertThat(counter.get()).isEqualTo(1);
    }

    @Test
    public void testRunOnceWithAbstractRunnable() {
        final AtomicInteger onRun = new AtomicInteger(0);
        final AtomicInteger onFailure = new AtomicInteger(0);
        final AtomicInteger onAfter = new AtomicInteger(0);

        final RunOnce runOnce = new RunOnce(new RejectableRunnable() {
            @Override
            public void doRun() throws Exception {
                onRun.incrementAndGet();
                throw new RuntimeException("failure");
            }

            @Override
            public void onFailure(Exception e) {
                onFailure.incrementAndGet();
            }

            @Override
            public void onAfter() {
                onAfter.incrementAndGet();
            }
        });

        final int iterations = randomIntBetween(1, 10);
        for (int i = 0; i < iterations; i++) {
            runOnce.run();
            assertThat(onRun.get()).isEqualTo(1);
            assertThat(onFailure.get()).isEqualTo(1);
            assertThat(onAfter.get()).isEqualTo(1);
            assertThat(runOnce.hasRun()).isTrue();
        }
    }
}
