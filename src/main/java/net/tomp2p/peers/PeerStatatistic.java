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
package net.tomp2p.peers;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import net.tomp2p.utils.Timings;

public class PeerStatatistic {
    private final AtomicLong lastSeenOnline = new AtomicLong(0);

    private final long created = Timings.currentTimeMillis();

    private final AtomicInteger successfullyChecked = new AtomicInteger(0);

    private final CircularBuffer rtt = new CircularBuffer(10, 3 * 1000);

    public void setLastSeenOnline(long lastSeenOnline) {
        this.lastSeenOnline.set(lastSeenOnline);
    }

    public long getLastSeenOnline() {
        return lastSeenOnline.get();
    }

    public void successfullyChecked() {
        successfullyChecked.incrementAndGet();
    }

    public int getSuccessfullyChecked() {
        return successfullyChecked.get();
    }

    public long getCreated() {
        return created;
    }

    public long onlineTime() {
        return lastSeenOnline.get() - created;
    }
    
    public void addRTT(long time) {
        rtt.add(time);
    }
    
    public long getMeanRTT() {
        return rtt.mean();
    }
}
