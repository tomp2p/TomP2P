/*
 * Copyright 2013 Thomas Bocek
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package net.tomp2p.futures;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.tomp2p.peers.Number160;

import org.junit.Test;

/**
 * Test the correctness and the performance of futures.
 * 
 * @author Thomas Bocek
 * 
 */
public class TestFutures {

    private final int nr = 10;
    private static final int RONUDS = 20000000;
    private static final int SUB = 1;
    private int steps = RONUDS / SUB;
    private final Set<Integer> done = new HashSet<Integer>();
    private final ExecutorService e = Executors.newFixedThreadPool(10);

    /**
     * Tests the performance of sequential processing as a base.
     */
    @Test
    public void testPerformanceSingle() {
        Number160[] number160s = new Number160[nr];
        Random rnd = new Random(1);
        for (int i = 0; i < nr; i++) {
            number160s[i] = new Number160(rnd);
        }
        // start test single-thread
        long start = System.currentTimeMillis();
        for (int i = 0; i < nr; i++) {
            for (int j = 0; j < RONUDS; j++) {
                number160s[i] = number160s[i].xor(new Number160(new Random(j * 31)));
                // System.err.println("number160s[" + i + "]=" + number160s[i]);
            }
        }

        // end test single-thread
        long stop = System.currentTimeMillis();
        System.out.println("XOR performance: single-thread: " + (stop - start) + "ms");
        for (int i = 0; i < nr; i++) {
            System.out.println("==> number160s[" + i + "]=" + number160s[i]);
        }
    }

    /**
     * Tests the performance of sequential processing as a base.
     * 
     * @throws InterruptedException
     */
    @Test
    public void testPerformanceMulti() throws InterruptedException {

        Number160[] number160s = new Number160[nr];
        AtomicReferenceArray<FutureTest> array = new AtomicReferenceArray<FutureTest>(new FutureTest[nr]);
        Random rnd = new Random(1);
        for (int i = 0; i < nr; i++) {
            number160s[i] = new Number160(rnd);
        }
        long start = System.currentTimeMillis();
        FutureDone<Void> futureDone = new FutureDone<Void>();
        recursive(array, number160s, 0, RONUDS / SUB, 0, futureDone);
        futureDone.awaitUninterruptibly();

        // end test single-thread
        long stop = System.currentTimeMillis();
        System.out.println("XOR performance: multi-thread: " + (stop - start) + "ms");
        for (int i = 0; i < nr; i++) {
            System.out.println("==> number160s[" + i + "]=" + number160s[i]);
        }
    }

    private void recursive(final AtomicReferenceArray<FutureTest> array, final Number160[] number160s, final int start,
            final int rounds, final int counter, final FutureDone<Void> futureDone) {
        int active = 0;
        for (int i = 0; i < nr; i++) {
            if (array.get(i) == null) {
                if (!done.contains(i)) {
                    array.set(i, startFuture(number160s[i], start, rounds, counter, i));
                    active++;
                }
            } else {
                active++;
            }
        }
        if (active == 0) {
            System.err.println("we are done!");
            futureDone.done();
        }

        FutureForkJoin<FutureTest> fork = new FutureForkJoin<FutureTest>(1, false, array);
        fork.addListener(new BaseFutureAdapter<FutureForkJoin<FutureTest>>() {

            @Override
            public void operationComplete(final FutureForkJoin<FutureTest> future) throws Exception {
                if (future.isFailed()) {
                    return;
                }
                if (future.last().getCounter() >= SUB - 1) {
                    done.add(future.last().getI());
                    // return;
                }
                number160s[future.last().getI()] = future.last().getResult();
                // System.err.println("start over with start" + future.getLast().getStart() + "/round"
                // + future.getLast().getRounds()+ " for "+future.getLast().getI()+
                // "counter="+future.getLast().getCounter());
                recursive(array, number160s, future.last().getStart() + steps, future.last().getRounds() + steps,
                        future.last().getCounter() + 1, futureDone);
            }
        });
    }

    private FutureTest startFuture(final Number160 number, final int start, final int rounds, final int counter,
            final int ii) {
        final FutureTest futureTest = new FutureTest(ii, start, rounds);
        Runnable r = new Runnable() {

            @Override
            public void run() {
                // System.err.println("start theard (" + Thread.currentThread().getName() + ") " + number + "/" + start
                // + "-" + rounds);
                Number160 result = number;
                for (int i = start; i < rounds; i++) {
                    result = result.xor(new Number160(new Random(i * 31)));
                    // System.err.println("number160s[" + ii + "]=" + result + "start="+start);
                }
                futureTest.setDone(result, counter);
            }
        };
        e.submit(r);
        return futureTest;
    }
}
