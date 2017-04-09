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

package net.tomp2p.connection;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;


/**
 * The configuration for the server.
 * 
 * @author Thomas Bocek
 * 
 */
public class ChannelServerConfiguration implements ConnectionConfiguration {

    private boolean behindFirewall = false;
    private boolean disableBind = false;

    private int idleTCPMillis = ConnectionBean.DEFAULT_TCP_IDLE_MILLIS;
    private int idleTCPSlowMillis = ConnectionBean.DEFAULT_TCP_IDLE_SLOW_MILLIS;
    private int idleUDPMillis = ConnectionBean.DEFAULT_UDP_IDLE_MILLIS;
    private int connectionTimeoutTCPMillis = ConnectionBean.DEFAULT_CONNECTION_TIMEOUT_TCP;
    private int heartBeatSeconds = ConnectionBean.DEFAULT_HEARTBEAT_SECONDS;

    //interface bindings
    private Bindings bindings = null;

    private SignatureFactory signatureFactory = null;

    private boolean forceTCP;
    private boolean forceUDP;
    
    private Ports portsForwarding;
    private Ports ports;
    
    private int maxTCPIncomingConnections = 1000;
    private int maxUDPIncomingConnections = 1000;
    
    private int heartBeatMillis = PeerConnection.HEART_BEAT_MILLIS;
    
    private boolean enablePool = false;
    private boolean enableHeap = false;

    /**
     * @return True if this peer is behind a firewall and cannot be accessed directly
     */
    public boolean isBehindFirewall() {
        return behindFirewall;
    }

    /**
     * @param behindFirewall
     *            Set to true if this peer is behind a firewall and not directly accessible
     * @return This class
     */
    public ChannelServerConfiguration behindFirewall(final boolean behindFirewall) {
        this.behindFirewall = behindFirewall;
        return this;
    }

    /**
     * Sets peer to be behind a firewall and cannot be accessed directly.
     * 
     * @return This class
     */
    public ChannelServerConfiguration behindFirewall() {
        this.behindFirewall = true;
        return this;
    }

    /**
     * @return True if the bind to ports should be omitted
     */
    public boolean isDisableBind() {
        return disableBind;
    }

    /**
     * Set to true if the bind to ports should be omitted
     * @param disableBind
     *
     * @return This class
     */
    public ChannelServerConfiguration disableBind(final boolean disableBind) {
        this.disableBind = disableBind;
        return this;
    }

    /**
     * Sets that the bind to ports should be omitted.
     * 
     * @return This class
     */
    public ChannelServerConfiguration disableBind() {
        this.disableBind = true;
        return this;
    }

    /**
     * @return The time that a connection can be idle before it is considered not active for short-lived connections
     */
    public int idleTCPMillis() {
        return idleTCPMillis;
    }

    /**
     * @param idleTCPSeconds
     *            The time that a connection can be idle before its considered not active for short-lived connections
     * @return This class
     */
    public ChannelServerConfiguration idleTCPMillis(final int idleTCPMillis) {
        this.idleTCPMillis = idleTCPMillis;
        return this;
    }
    
    /**
     * @return The time that a connection can be idle before it is considered not active for short-lived connections
     */
    public int idleTCPSlowMillis() {
        return idleTCPSlowMillis;
    }

    /**
     * @param idleTCPSeconds
     *            The time that a connection can be idle before its considered not active for short-lived connections
     * @return This class
     */
    public ChannelServerConfiguration idleTCPSlowMillis(final int idleTCPSlowMillis) {
        this.idleTCPSlowMillis = idleTCPSlowMillis;
        return this;
    }

    /**
     * @return The time that a connection can be idle before its considered not active for short-lived connections
     */
    public int idleUDPMillis() {
        return idleUDPMillis;
    }

    /**
     * @param idleUDPSeconds
     *            The time that a connection can be idle before its considered not active for short-lived connections
     * @return This class
     */
    public ChannelServerConfiguration idleUDPMillis(final int idleUDPMillis) {
        this.idleUDPMillis = idleUDPMillis;
        return this;
    }

    /**
     * @return The factory for the signature
     */
    public SignatureFactory signatureFactory() {
        return signatureFactory;
    }

    /**
     * @param signatureFactory
     *            Set the factory for the signature
     * @return This class
     */
    public ChannelServerConfiguration signatureFactory(final SignatureFactory signatureFactory) {
        this.signatureFactory = signatureFactory;
        return this;
    }

    @Override
    public int connectionTimeoutTCPMillis() {
        return connectionTimeoutTCPMillis;
    }

    public ChannelServerConfiguration connectionTimeoutTCPMillis(final int connectionTimeoutTCPMillis) {
        this.connectionTimeoutTCPMillis = connectionTimeoutTCPMillis;
        return this;
    }

    @Override
    public boolean isForceTCP() {
        return forceTCP;
    }

    public ChannelServerConfiguration forceTCP(boolean forceTCP) {
        this.forceTCP = forceTCP;
        return this;
    }

    public ChannelServerConfiguration forceTCP() {
        this.forceTCP = true;
        return this;
    }

    @Override
    public boolean isForceUDP() {
        return forceUDP;
    }
    
    public ChannelServerConfiguration forceUDP(boolean forceUDP) {
        this.forceUDP = forceUDP;
        return this;
    }

    public ChannelServerConfiguration forceUDP() {
        this.forceUDP = true;
        return this;
    }

    public Ports portsForwarding() {
        return portsForwarding;
    }

    public ChannelServerConfiguration portsForwarding(Ports portsForwarding) {
        this.portsForwarding = portsForwarding;
        return this;
    }
    
    public Ports ports() {
        return ports;
    }

    public ChannelServerConfiguration ports(Ports ports) {
        this.ports = ports;
        return this;
    }

    public ChannelServerConfiguration bindings(Bindings bindings) {
        this.bindings = bindings;
        return this;
    }
    
    public Bindings bindings() {
        return bindings;
    }
    
    
    public int maxTCPIncomingConnections() {
        return maxTCPIncomingConnections;
    }

    public ChannelServerConfiguration maxTCPIncomingConnections(final int maxTCPIncomingConnections) {
        this.maxTCPIncomingConnections = maxTCPIncomingConnections;
        return this;
    }
    
    public int maxUDPIncomingConnections() {
        return maxUDPIncomingConnections;
    }

    public ChannelServerConfiguration maxUDPIncomingConnections(final int maxUDPIncomingConnections) {
        this.maxUDPIncomingConnections = maxUDPIncomingConnections;
        return this;
    }

	public int heartBeatMillis() {
	    return heartBeatMillis;
    }
	
	public ChannelServerConfiguration heartBeatMillis(int heartBeatMillis) {
	    this.heartBeatMillis = heartBeatMillis;
	    return this;
    }

	public ChannelServerConfiguration heartBeatSeconds(final int heartBeatSeconds) {
		this.heartBeatSeconds = heartBeatSeconds;
		return this;
	}
	
	@Override
	public int heartBeatSeconds() {
		return heartBeatSeconds;
	}
	
	public ChannelServerConfiguration byteBufPool() {
		byteBufPool(true);
		return this;
	}
	
	public ChannelServerConfiguration byteBufPool(boolean enablePool) {
		this.enablePool = enablePool;
		return this;
	}
	
	public ChannelServerConfiguration byteBufHeap() {
		byteBufHeap(true);
		return this;
	}
	
	public ChannelServerConfiguration byteBufHeap(boolean enableHeap) {
		this.enableHeap = enableHeap;
		return this;
	}
	
	
	public ChannelServerConfiguration byteBufAllocator(boolean enablePool, boolean enableHeap) {
		this.enableHeap = enableHeap;
		this.enablePool = enablePool;
		return this;
	}

	public ByteBufAllocator byteBufAllocator() {
		return UnpooledByteBufAllocator.DEFAULT;
	}
}
