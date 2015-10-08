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
package net.tomp2p.tracker;

import net.tomp2p.p2p.RequestConfiguration;

public class TrackerConfiguration implements RequestConfiguration {
    final private int maxFailure;

    final private int parallel;

    final private int atLeastSuccessfulRequests;

    final private int atLeastEntriesFromTrackers;

    // for tracker get, max full tracker is not relevant
    final private int maxFullTrackers;

    final private int maxPrimaryTrackers;

    // set to force either UDP or TCP
    final private boolean forceUPD;

    final private boolean forceTCP;

    public TrackerConfiguration(int maxFailure, int parallel, int atLeastSuccessfulRequests,
            int atLeastEntriesFromTrackers) {
        this(maxFailure, parallel, atLeastSuccessfulRequests, atLeastEntriesFromTrackers, 20, 5);
    }

    public TrackerConfiguration(int maxFailure, int parallel, int atLeastSuccessfulRequests,
            int atLeastEntriesFromTrackers, int maxFullTrackers, int maxPrimaryTrackers) {
        this(maxFailure, parallel, atLeastSuccessfulRequests, atLeastEntriesFromTrackers, maxFullTrackers,
                maxPrimaryTrackers, false, false);
    }

    public TrackerConfiguration(int maxFailure, int parallel, int atLeastSuccessfulRequests,
            int atLeastEntriesFromTrackers, int maxFullTrackers, int maxPrimaryTrackers, boolean forceUDP,
            boolean forceTCP) {
        if (maxFailure < 0 || parallel < 0 || atLeastSuccessfulRequests < 0 || atLeastEntriesFromTrackers < 0)
            throw new IllegalArgumentException("need to be larger or equals zero");
        this.maxFailure = maxFailure;
        this.parallel = parallel;
        this.atLeastSuccessfulRequests = atLeastSuccessfulRequests;
        this.atLeastEntriesFromTrackers = atLeastEntriesFromTrackers;
        this.maxFullTrackers = maxFullTrackers;
        this.maxPrimaryTrackers = maxPrimaryTrackers;
        this.forceUPD = forceUDP;
        this.forceTCP = forceTCP;
    }

    public int maxFailure() {
        return maxFailure;
    }

    public int parallel() {
        return parallel;
    }

    public int atLeastSuccessfulRequests() {
        return atLeastSuccessfulRequests;
    }

    public int atLeastEntriesFromTrackers() {
        return atLeastEntriesFromTrackers;
    }

    public int maxFullTrackers() {
        return maxFullTrackers;
    }

    public int maxPrimaryTrackers() {
        return maxPrimaryTrackers;
    }

    public boolean isForceUPD() {
        return forceUPD;
    }

    public boolean isForceTCP() {
        return forceTCP;
    }
}
