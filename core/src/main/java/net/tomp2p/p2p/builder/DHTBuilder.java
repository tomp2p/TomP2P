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

import java.security.KeyPair;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.peers.Number160;

/**
 * Every DHT builder has those methods in common.
 * 
 * @author Thomas Bocek
 * 
 * @param <K>
 */
public abstract class DHTBuilder<K extends DHTBuilder<K>> extends DefaultConnectionConfiguration implements
        BasicBuilder<K>, ConnectionConfiguration, SignatureBuilder<K> {
    // changed this to zero as for the content key its also zero

    protected final Peer peer;

    protected final Number160 locationKey;

    protected Number160 domainKey;

    protected Number160 versionKey;

    protected RoutingConfiguration routingConfiguration;

    protected RequestP2PConfiguration requestP2PConfiguration;

    protected FutureChannelCreator futureChannelCreator;

    // private int idleTCPSeconds = ConnectionBean.DEFAULT_TCP_IDLE_SECONDS;
    // private int idleUDPSeconds = ConnectionBean.DEFAULT_UDP_IDLE_SECONDS;
    // private int connectionTimeoutTCPMillis = ConnectionBean.DEFAULT_CONNECTION_TIMEOUT_TCP;

    private boolean protectDomain = false;
    // private boolean signMessage = false;
    private KeyPair keyPair = null;
    private boolean streaming = false;
    // private boolean forceUDP = false;
    // private boolean forceTCP = false;

    private K self;

    public DHTBuilder(Peer peer, Number160 locationKey) {
        this.peer = peer;
        this.locationKey = locationKey;
    }

    public void self(K self) {
        this.self = self;
    }

    /**
     * @return The location key
     */
    public Number160 getLocationKey() {
        return locationKey;
    }

    public Number160 getDomainKey() {
        return domainKey;
    }

    public K setDomainKey(Number160 domainKey) {
        this.domainKey = domainKey;
        return self;
    }

    public Number160 getVersionKey() {
        return versionKey;
    }

    public K setVersionKey(Number160 versionKey) {
        this.versionKey = versionKey;
        return self;
    }

    /**
     * @return The configuration for the routing options
     */
    public RoutingConfiguration getRoutingConfiguration() {
        return routingConfiguration;
    }

    /**
     * @param routingConfiguration
     *            The configuration for the routing options
     * @return This object
     */
    public K setRoutingConfiguration(final RoutingConfiguration routingConfiguration) {
        this.routingConfiguration = routingConfiguration;
        return self;
    }

    /**
     * @return The P2P request configuration options
     */
    public RequestP2PConfiguration getRequestP2PConfiguration() {
        return requestP2PConfiguration;
    }

    /**
     * @param requestP2PConfiguration
     *            The P2P request configuration options
     * @return This object
     */
    public K setRequestP2PConfiguration(final RequestP2PConfiguration requestP2PConfiguration) {
        this.requestP2PConfiguration = requestP2PConfiguration;
        return self;
    }

    /**
     * @return The future of the created channel
     */
    public FutureChannelCreator getFutureChannelCreator() {
        return futureChannelCreator;
    }

    /**
     * @param futureChannelCreator
     *            The future of the created channel
     * @return This object
     */
    public K setFutureChannelCreator(FutureChannelCreator futureChannelCreator) {
        this.futureChannelCreator = futureChannelCreator;
        return self;
    }

    /**
     * @return Set to true if the domain should be set to protected. This means that this domain is flagged an a public
     *         key is stored for this entry. An update or removal can only be made with the matching private key.
     */
    public boolean isProtectDomain() {
        return protectDomain;
    }

    /**
     * @param protectDomain
     *            Set to true if the domain should be set to protected. This means that this domain is flagged an a
     *            public key is stored for this entry. An update or removal can only be made with the matching private
     *            key.
     * @return This class
     */
    public K setProtectDomain(final boolean protectDomain) {
        this.protectDomain = protectDomain;
        return self;
    }

    /**
     * @return Set to true if the domain should be set to protected. This means that this domain is flagged an a public
     *         key is stored for this entry. An update or removal can only be made with the matching private key.
     */
    public K setProtectDomain() {
        this.protectDomain = true;
        return self;
    }

    /* (non-Javadoc)
     * @see net.tomp2p.p2p.builder.SignatureBuilder#isSignMessage()
     */
    @Override
    public boolean isSign() {
        return keyPair != null;
    }

    /* (non-Javadoc)
     * @see net.tomp2p.p2p.builder.SignatureBuilder#setSignMessage(boolean)
     */
    @Override
    public K sign(final boolean signMessage) {
        if (signMessage) {
            setSign();
        } else {
            this.keyPair = null;
        }
        return self;
    }

    /* (non-Javadoc)
     * @see net.tomp2p.p2p.builder.SignatureBuilder#setSignMessage()
     */
    @Override
    public K setSign() {
        this.keyPair = peer.getPeerBean().keyPair();
        return self;
    }
    
    /* (non-Javadoc)
     * @see net.tomp2p.p2p.builder.SignatureBuilder#keyPair(java.security.KeyPair)
     */
    @Override
    public K keyPair(KeyPair keyPair) {
        this.keyPair = keyPair;
        return self;
    }

    /* (non-Javadoc)
     * @see net.tomp2p.p2p.builder.SignatureBuilder#keyPair()
     */
    @Override
    public KeyPair keyPair() {
        return keyPair;
    }

    /**
     * @return True if streaming should be used
     */
    public boolean isStreaming() {
        return streaming;
    }

    /**
     * Set streaming. If streaming is set to true, than the data can be added after {@link #start()} has been called.
     * 
     * @param streaming
     *            True if streaming should be used
     * @return This class
     */
    public K setStreaming(final boolean streaming) {
        this.streaming = streaming;
        return self;
    }

    /**
     * Set streaming to true. See {@link #setStreaming(boolean)}
     * 
     * @return This class
     */
    public K setStreaming() {
        this.streaming = true;
        return self;
    }

    

    protected void preBuild(String name) {
        if (domainKey == null) {
            domainKey = Number160.ZERO;
        }
        if (versionKey == null) {
            versionKey = Number160.ZERO;
        }
        if (routingConfiguration == null) {
            routingConfiguration = new RoutingConfiguration(5, 10, 2);
        }
        if (requestP2PConfiguration == null) {
            requestP2PConfiguration = new RequestP2PConfiguration(3, 5, 3);
        }
        int size = peer.getPeerBean().peerMap().size() + 1;
        requestP2PConfiguration = requestP2PConfiguration.adjustMinimumResult(size);
        if (futureChannelCreator == null) {
            futureChannelCreator = peer.getConnectionBean().reservation()
                    .create(routingConfiguration, requestP2PConfiguration, this);
        }
    }

    public RoutingBuilder createBuilder(RequestP2PConfiguration requestP2PConfiguration,
            RoutingConfiguration routingConfiguration) {
        RoutingBuilder routingBuilder = new RoutingBuilder();
        routingBuilder.setParallel(routingConfiguration.getParallel());
        routingBuilder.setMaxNoNewInfo(routingConfiguration.getMaxNoNewInfo(requestP2PConfiguration
                .getMinimumResults()));
        routingBuilder.setMaxDirectHits(routingConfiguration.getMaxDirectHits());
        routingBuilder.setMaxFailures(routingConfiguration.getMaxFailures());
        routingBuilder.setMaxSuccess(routingConfiguration.getMaxSuccess());
        return routingBuilder;
    }

    // public abstract FutureDHT start();
}
