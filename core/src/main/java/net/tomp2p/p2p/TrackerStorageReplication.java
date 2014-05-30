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
package net.tomp2p.p2p;

import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.PeerExchangeRPC;
import net.tomp2p.storage.TrackerStorage;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class that will handle the synchronization of the tracker data.
 * 
 * @author Thomas Bocek
 * 
 */
public class TrackerStorageReplication implements ResponsibilityListener {
    private static final Logger LOG = LoggerFactory.getLogger(TrackerStorageReplication.class);

    private final PeerExchangeRPC peerExchangeRPC;

    private final TrackerStorage trackerStorage;

    private final Peer peer;

    /**
     * @param peer
     *            This peer
     * @param peerExchangeRPC
     *            The RPC to send informaiotn
     * @param trackerStorage
     *            The memory based tracker storage
     */
    public TrackerStorageReplication(final Peer peer, final PeerExchangeRPC peerExchangeRPC,
            final TrackerStorage trackerStorage) {
        this.peer = peer;
        this.peerExchangeRPC = peerExchangeRPC;
        this.trackerStorage = trackerStorage;
    }

    @Override
    public void meResponsible(final Number160 locationKey) {
        // the other has to send us the data, so do nothing
    }

    @Override
    public void otherResponsible(final Number160 locationKey, final PeerAddress other, final boolean delayed) {
        // do pex here, but with mesh peers!
        LOG.debug("other peer became responsibel and we thought we were responsible, so move the data to this peer");
        peerExchange(locationKey, other);
    }

	@Override
    public void meResponsible(final Number160 locationKey, final PeerAddress newPeer) {
		LOG.debug("I'm responsible and a new peer joined");
		peerExchange(locationKey, newPeer);
    }

	private void peerExchange(final Number160 locationKey, final PeerAddress newPeer) {
	    for (final Number160 domainKey : trackerStorage.responsibleDomains(locationKey)) {
            FutureChannelCreator futureChannelCreator = peer.getConnectionBean().reservation().create(1, 0);
            futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
                @Override
                public void operationComplete(final FutureChannelCreator future) throws Exception {
                    if (future.isSuccess()) {
                        FutureResponse futureResponse = peerExchangeRPC.peerExchange(newPeer, locationKey,
                                domainKey, true, future.channelCreator(),
                                new DefaultConnectionConfiguration());
                        Utils.addReleaseListener(future.channelCreator(), futureResponse);
                        peer.notifyAutomaticFutures(futureResponse);
                    } else {
                        LOG.error("otherResponsible failed {}", future.getFailedReason());
                    }
                }
            });
        }
    }
}
