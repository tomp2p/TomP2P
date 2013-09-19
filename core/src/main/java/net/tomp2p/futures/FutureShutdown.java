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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.tomp2p.peers.PeerAddress;

/**
 * The future object for the shutdown() including routing.
 * 
 * @author Thomas Bocek
 */
public class FutureShutdown extends BaseFutureImpl<FutureShutdown> implements FutureDHT {

    private final Map<PeerAddress, Boolean> status = new HashMap<PeerAddress, Boolean>();
    private final List<FutureResponse> futures = new ArrayList<FutureResponse>();

    /**
     * Creates a new future for the shutdown operation.
     */
    public FutureShutdown() {
        self(this);
    }

    @Override
    public void addRequests(final FutureResponse futureResponse) {
        synchronized (lock) {
            futures.add(futureResponse);
        }
    }

    /**
     * Set future as finished and notify listeners.
     */
    public void setDone() {
        synchronized (lock) {
            if (!setCompletedAndNotify()) {
                return;
            }
            this.type = BaseFuture.FutureType.OK;
        }
        notifyListerenrs();
    }

    /**
     * Report the intermediate status.
     * 
     * @param recipient
     *            The recipient of the RPC quit message
     * @param success
     *            The status if the quit message was successful
     */
    public void report(final PeerAddress recipient, final boolean success) {
        synchronized (lock) {
            status.put(recipient, success);
        }
    }
}
