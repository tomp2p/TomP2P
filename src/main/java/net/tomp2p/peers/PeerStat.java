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

public class PeerStat {
    private volatile long lastSeenOnline = 0;

    private volatile int checked = 0;

    private volatile long created = 0;

    public void setLastSeenOnline(long lastSeenOnline) {
        this.lastSeenOnline = lastSeenOnline;
    }

    public long getLastSeenOnline() {
        return lastSeenOnline;
    }

    public void incChecked() {
        synchronized (this) {
            this.checked++;
        }
    }

    public int getChecked() {
        synchronized (this) {
            return checked;
        }
    }

    public void setCreated(long created) {
        this.created = created;
    }

    public long getCreated() {
        return created;
    }

    public long onlineTime() {
        synchronized (this) {
            return lastSeenOnline - created;
        }
    }
}