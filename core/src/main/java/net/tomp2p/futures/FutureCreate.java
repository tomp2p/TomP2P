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

/**
 * DHT and Tracker operations may occur repeatedly. For example a put may be
 * configured to be called every 60 seconds. These futures generated out of
 * those repetitions are stored in {@link FutureDHT} and {@link FutureTracker}.
 * Typically, those are backed by a caching map with a fixed size and an LRU
 * eviction policy.
 * 
 * @author Thomas Bocek
 * @param <K>
 */
public interface FutureCreate<K extends BaseFuture> {
    /**
     * Called if a future has been created without user interaction
     * 
     * @param future
     *            The created future
     */
    public void repeated(K future);
}
