/*
 * Copyright 2009 Thomas Bocek
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

import java.util.ArrayList;
import java.util.List;

/**
 * FutureLateJoin is similar to FutureForkJoin. The main difference is that with
 * this class you don't need to specify all the futures in advance. You can just
 * tell how many futures you expect and add them later on.
 * 
 * @author Thomas Bocek
 * @param <K>
 */
public class FutureLateJoin<K extends BaseFuture> extends BaseFutureImpl<FutureLateJoin<K>> implements BaseFuture {
    private final int nrMaxFutures;

    private final int minSuccess;

    private final List<K> futuresDone;
    private final List<K> futuresSubmitted;

    private K lastSuceessFuture;

    private int successCount = 0;

    /**
     * Create this future and set the minSuccess to the number of expected
     * futures.
     * 
     * @param nrMaxFutures
     *            The number of expected futures.
     */
    public FutureLateJoin(final int nrMaxFutures) {
        this(nrMaxFutures, nrMaxFutures);
    }

    /**
     * Create this future.
     * 
     * @param nrMaxFutures
     *            The number of expected futures.
     * @param minSuccess
     *            The number of expected successful futures.
     */
    public FutureLateJoin(final int nrMaxFutures, final int minSuccess) {
        this.nrMaxFutures = nrMaxFutures;
        this.minSuccess = minSuccess;
        this.futuresDone = new ArrayList<K>(nrMaxFutures);
        this.futuresSubmitted = new ArrayList<K>(nrMaxFutures);
        self(this);
    }

    /**
     * Add a future when ready. This is why its called FutureLateJoin, since you
     * can add futures later on.
     * 
     * @param future
     *            The future to be added.
     * @return True if the future was added to the futurelist, false if the
     *         latejoin future is already finished and the future was not added.
     */
    public boolean add(final K future) {
        synchronized (lock) {
            if (completed) {
                return false;
            }
            futuresSubmitted.add(future);
            future.addListener(new BaseFutureAdapter<K>() {
                @Override
                public void operationComplete(final K future) throws Exception {
                    boolean done = false;
                    synchronized (lock) {
                        if (!completed) {
                            if (future.isSuccess()) {
                                successCount++;
                                lastSuceessFuture = future;
                            }
                            futuresDone.add(future);
                            done = checkDone();
                        }
                    }
                    if (done) {
                        notifyListeners();
                    }
                }
            });
            return true;
        }
    }

    /**
     * Check if the can set this future to done.
     * 
     * @return True if we are done.
     */
    private boolean checkDone() {
        if (futuresDone.size() >= nrMaxFutures || successCount >= minSuccess) {
            if (!setCompletedAndNotify()) {
                return false;
            }
            boolean isSuccess = successCount >= minSuccess;
            type = isSuccess ? FutureType.OK : FutureType.FAILED;
            reason = isSuccess ? "Minimal number of futures received" : "Minimal number of futures *not* received ("
                    + successCount + " of " + minSuccess + " reached)";
            return true;
        }
        return false;
    }

    /**
     * Returns the finished futures.
     * 
     * @return All the futures that are done.
     */
    public List<K> getFuturesDone() {
        synchronized (lock) {
            return futuresDone;
        }
    }

    /**
     * @return the last successful finished future.
     */
    public K getLastSuceessFuture() {
        synchronized (lock) {
            return lastSuceessFuture;
        }
    }
    
    /**
     * @return the all submitted futures.
     */
    public List<K> futuresSubmitted() {
        synchronized (lock) {
            return futuresSubmitted;
        }
    }
}
