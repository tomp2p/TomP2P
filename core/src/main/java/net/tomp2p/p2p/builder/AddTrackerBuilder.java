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

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureCreate;
import net.tomp2p.futures.FutureCreator;
import net.tomp2p.futures.FutureLateJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureTracker;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;

public class AddTrackerBuilder extends TrackerBuilder<AddTrackerBuilder> {
    private Data attachement;

    private int trackerTimeoutSec = 60;

    private int pexWaitSec = 0;

    private SimpleBloomFilter<Number160> bloomFilter;

    private FutureCreate<BaseFuture> futureCreate;

    private FutureCreator<FutureTracker> defaultDirectReplication;

    private FutureCreator<FutureLateJoin<FutureResponse>> defaultPEX;

    private boolean tcpPEX = false;
    
    private boolean primary = false;
    
    private PeerAddress peerAddressToAnnounce;

    public AddTrackerBuilder(Peer peer, Number160 locationKey) {
        super(peer, locationKey);
        self(this);
    }

    public Data getAttachement() {
        return attachement;
    }

    public AddTrackerBuilder setAttachement(Data attachement) {
        this.attachement = attachement;
        return this;
    }

    public int getTrackerTimeoutSec() {
        return trackerTimeoutSec;
    }

    public AddTrackerBuilder setTrackerTimeoutSec(int trackerTimeoutSec) {
        this.trackerTimeoutSec = trackerTimeoutSec;
        return this;
    }

    public int getPexWaitSec() {
        return pexWaitSec;
    }

    public AddTrackerBuilder setPexWaitSec(int pexWaitSec) {
        this.pexWaitSec = pexWaitSec;
        return this;
    }

    public SimpleBloomFilter<Number160> getBloomFilter() {
        return bloomFilter;
    }

    public AddTrackerBuilder setBloomFilter(SimpleBloomFilter<Number160> bloomFilter) {
        this.bloomFilter = bloomFilter;
        return this;
    }

    public FutureCreate<BaseFuture> getFutureCreate() {
        return futureCreate;
    }

    public AddTrackerBuilder setFutureCreate(FutureCreate<BaseFuture> futureCreate) {
        this.futureCreate = futureCreate;
        return this;
    }

    public FutureCreator<FutureTracker> getDefaultDirectReplication() {
        return defaultDirectReplication;
    }

    public AddTrackerBuilder setDefaultDirectReplication(FutureCreator<FutureTracker> defaultDirectReplication) {
        this.defaultDirectReplication = defaultDirectReplication;
        return this;
    }

    public FutureCreator<FutureLateJoin<FutureResponse>> getDefaultPEX() {
        return defaultPEX;
    }

    public AddTrackerBuilder setDefaultPEX(FutureCreator<FutureLateJoin<FutureResponse>> defaultPEX) {
        this.defaultPEX = defaultPEX;
        return this;
    }

    public boolean isTcpPEX() {
        return tcpPEX;
    }

    public AddTrackerBuilder setTcpPEX() {
        this.tcpPEX = true;
        return this;
    }

    public AddTrackerBuilder setTcpPEX(boolean tcpPEX) {
        this.tcpPEX = tcpPEX;
        return this;
    }
    
    public PeerAddress peerAddressToAnnounce() {
        return peerAddressToAnnounce;
    }

    public AddTrackerBuilder peerAddressToAnnounce(PeerAddress peerAddressToAnnounce) {
        this.peerAddressToAnnounce = peerAddressToAnnounce;
        return this;
    }

    @Override
    public FutureTracker start() {
        if (peer.isShutdown()) {
            return FUTURE_TRACKER_SHUTDOWN;
        }
        preBuild("add-tracker-build");

        if (bloomFilter == null) {
            bloomFilter = new SimpleBloomFilter<Number160>(1024, 1024);
        }
        // add myself to my local tracker, since we use a mesh we are part of
        // the tracker mesh as well.
        peer.getPeerBean()
                .trackerStorage()
                .put(locationKey, domainKey, peer.getPeerAddress(), keyPair() == null? null: keyPair().getPublic(),
                        attachement);
        final FutureTracker futureTracker = peer.getDistributedTracker().add(this);

        
        /*if (pexWaitSec > 0) {
            if (defaultPEX == null) {
                defaultPEX = new DefaultPEX();
            }
            Runnable runner = new Runnable() {
                @Override
                public void run() {
                    FutureLateJoin<FutureResponse> future = defaultPEX.create();
                    futureTracker.repeated(future);
                }
            };
            ScheduledFuture<?> tmp = peer.getConnectionBean().getScheduler().getScheduledExecutorServiceReplication()
                    .scheduleAtFixedRate(runner, pexWaitSec, pexWaitSec, TimeUnit.SECONDS);
            setupCancel(futureTracker, tmp);
        }*/
        return futureTracker;
    }

    /*protected void setupCancel(final FutureCleanup futureCleanup, final ScheduledFuture<?> future) {
        peer.getScheduledFutures().add(future);
        futureCleanup.addCleanup(new Cancel() {
            @Override
            public void cancel() {
                future.cancel(true);
                peer.getScheduledFutures().remove(future);
            }
        });
    }

    private class DefaultDirectReplication implements FutureCreator<FutureTracker> {
        @Override
        public FutureTracker create() {
            int conn = Math.max(routingConfiguration.getParallel(), trackerConfiguration.getParallel());
            final FutureChannelCreator futureChannelCreator = peer.getConnectionBean().getConnectionReservation()
                    .reserve(conn);
            final FutureTracker futureTracker = peer.getDistributedTracker().addToTracker(locationKey, domainKey,
                    getAttachement(), routingConfiguration, trackerConfiguration, isMessageSign(), futureCreate,
                    getBloomFilter(), futureChannelCreator, peer.getConnectionBean().getConnectionReservation());
            return futureTracker;
        }
    }*/

    /*private class DefaultPEX implements FutureCreator<FutureLateJoin<FutureResponse>> {
        @Override
        public FutureLateJoin<FutureResponse> create() {
            final FutureChannelCreator futureChannelCreator = peer.getConnectionBean().getConnectionReservation()
                    .reserve(TrackerStorage.TRACKER_SIZE);
            FutureLateJoin<FutureResponse> futureLateJoin = peer.getDistributedTracker().startPeerExchange(locationKey,
                    domainKey, futureChannelCreator, peer.getConnectionBean().getConnectionReservation(), tcpPEX);
            return futureLateJoin;
        }
    }*/

    public boolean isPrimary() {
        return primary;
    }
}
