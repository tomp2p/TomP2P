/*
 * Copyright 2012 Thomas Bocek
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

package net.tomp2p.p2p.builder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureCreator;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;

public class RemoveBuilder extends DHTBuilder<RemoveBuilder> {
    private Collection<Number160> contentKeys;

    private Number160 contentKey;

    //
    private int repetitions = 5;

    private boolean all = false;

    private boolean returnResults = false;

    public RemoveBuilder(Peer peer, Number160 locationKey) {
        super(peer, locationKey);
        self(this);
    }

    public Collection<Number160> getContentKeys() {
        return contentKeys;
    }

    public RemoveBuilder setContentKeys(Collection<Number160> contentKeys) {
        this.contentKeys = contentKeys;
        return this;
    }

    public Number160 getContentKey() {
        return contentKey;
    }

    public RemoveBuilder setContentKey(Number160 contentKey) {
        this.contentKey = contentKey;
        return this;
    }

    public int getRepetitions() {
        return repetitions;
    }

    public RemoveBuilder setRepetitions(int repetitions) {
        this.repetitions = repetitions;
        return this;
    }

    public boolean isAll() {
        return all;
    }

    public RemoveBuilder setAll(boolean all) {
        this.all = all;
        return this;
    }

    public RemoveBuilder setAll() {
        this.all = true;
        return this;
    }

    public boolean isReturnResults() {
        return returnResults;
    }

    public RemoveBuilder setReturnResults(boolean returnResults) {
        this.returnResults = returnResults;
        return this;
    }

    public RemoveBuilder setReturnResults() {
        this.returnResults = true;
        return this;
    }

    @Override
    public FutureDHT start() {
        if (peer.isShutdown()) {
            return FUTURE_DHT_SHUTDOWN;
        }
        preBuild("remove-builder");
        if (all) {
            contentKeys = null;
        } else if (contentKeys == null && !all) {
            contentKeys = new ArrayList<Number160>(1);
            if (contentKey == null) {
                contentKey = Number160.ZERO;
            }
            contentKeys.add(contentKey);
        }
        final FutureDHT futureDHT = peer.getDistributedHashMap().remove(locationKey, domainKey, contentKeys,
                routingConfiguration, requestP2PConfiguration, returnResults, signMessage, manualCleanup, futureCreate,
                futureChannelCreator, peer.getConnectionBean().getConnectionReservation());
        if (directReplication) {
            if (defaultDirectReplication == null) {
                defaultDirectReplication = new DefaultDirectReplication();
            }
            Runnable runner = new Runnable() {
                private int counter = 0;

                @Override
                public void run() {
                    if (counter < repetitions) {
                        FutureDHT futureDHTReplication = defaultDirectReplication.create();
                        futureDHT.repeated(futureDHTReplication);
                        counter++;
                        ScheduledFuture<?> tmp = peer.getConnectionBean().getScheduler()
                                .getScheduledExecutorServiceReplication()
                                .schedule(this, refreshSeconds, TimeUnit.SECONDS);
                        setupCancel(futureDHT, tmp);
                    }
                }
            };
            ScheduledFuture<?> tmp = peer.getConnectionBean().getScheduler().getScheduledExecutorServiceReplication()
                    .schedule(runner, refreshSeconds, TimeUnit.SECONDS);
            setupCancel(futureDHT, tmp);
        }
        return futureDHT;
    }

    private class DefaultDirectReplication implements FutureCreator<FutureDHT> {
        @Override
        public FutureDHT create() {
            final FutureChannelCreator futureChannelCreator = peer.reserve(routingConfiguration,
                    requestP2PConfiguration, "remove-builder-direct-replication");
            final FutureDHT futureDHT = peer.getDistributedHashMap().remove(locationKey, domainKey, contentKeys,
                    routingConfiguration, requestP2PConfiguration, returnResults, signMessage, manualCleanup,
                    futureCreate, futureChannelCreator, peer.getConnectionBean().getConnectionReservation());
            return futureDHT;
        }
    }
}