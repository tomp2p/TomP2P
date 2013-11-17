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


/**
 * The the configuration for the server.
 * 
 * @author Thomas Bocek
 * 
 */
public class ChannelServerConficuration implements ConnectionConfiguration {

    private boolean behindFirewall = false;
    private boolean disableBind = false;

    private int idleTCPSeconds = ConnectionBean.DEFAULT_TCP_IDLE_SECONDS;
    private int idleUDPSeconds = ConnectionBean.DEFAULT_UDP_IDLE_SECONDS;
    private int connectionTimeoutTCPMillis = ConnectionBean.DEFAULT_CONNECTION_TIMEOUT_TCP;

    private PipelineFilter pipelineFilter = null;

    private Bindings bindings = null;

    private int tcpPort = -1;

    private int udpPort = -1;

    private SignatureFactory signatureFactory = null;

    private boolean forceTCP;
    private boolean forceUDP;

    /**
     * @return the bindings
     */
    public Bindings getBindings() {
        return bindings;
    }

    /**
     * @param bindings
     *            the bindings to set
     */
    public void setBindings(final Bindings bindings) {
        this.bindings = bindings;
    }

    /**
     * @return the tcpPort
     */
    public int getTcpPort() {
        return tcpPort;
    }

    /**
     * @param tcpPort
     *            the tcpPort to set
     */
    public void setTcpPort(final int tcpPort) {
        this.tcpPort = tcpPort;
    }

    /**
     * @return the udpPort
     */
    public int getUdpPort() {
        return udpPort;
    }

    /**
     * @param udpPort
     *            the udpPort to set
     */
    public void setUdpPort(final int udpPort) {
        this.udpPort = udpPort;
    }

    /**
     * @return True if this peer is behind a firewall and cannot be accessed directly
     */
    public boolean isBehindFirewall() {
        return behindFirewall;
    }

    /**
     * @param behindFirewall
     *            Set to true if this peer is behind a firewall and cannot be accessed directly
     * @return This class
     */
    public ChannelServerConficuration setBehindFirewall(final boolean behindFirewall) {
        this.behindFirewall = behindFirewall;
        return this;
    }

    /**
     * Set peer to be behind a firewall and cannot be accessed directly.
     * 
     * @return This class
     */
    public ChannelServerConficuration setBehindFirewall() {
        this.behindFirewall = true;
        return this;
    }

    /**
     * @return True if the bind to ports should be omited
     */
    public boolean disableBind() {
        return disableBind;
    }

    /**
     * @param disableBind
     *            Set to true if the bind to ports should be omited
     * @return This class
     */
    public ChannelServerConficuration disableBind(final boolean disableBind) {
        this.disableBind = disableBind;
        return this;
    }

    /**
     * Set the bind to ports should be omited.
     * 
     * @return This class
     */
    public ChannelServerConficuration setDisableBind() {
        this.disableBind = true;
        return this;
    }

    /**
     * @return The time that a connection can be idle before its considered not active for short-lived connections
     */
    public int idleTCPSeconds() {
        return idleTCPSeconds;
    }

    /**
     * @param idleTCPSeconds
     *            The time that a connection can be idle before its considered not active for short-lived connections
     * @return This class
     */
    public ChannelServerConficuration idleTCPSeconds(final int idleTCPSeconds) {
        this.idleTCPSeconds = idleTCPSeconds;
        return this;
    }

    /**
     * @return The time that a connection can be idle before its considered not active for short-lived connections
     */
    public int idleUDPSeconds() {
        return idleUDPSeconds;
    }

    /**
     * @param idleUDPSeconds
     *            The time that a connection can be idle before its considered not active for short-lived connections
     * @return This class
     */
    public ChannelServerConficuration idleUDPSeconds(final int idleUDPSeconds) {
        this.idleUDPSeconds = idleUDPSeconds;
        return this;
    }

    /**
     * @return Set the filter for the pipeline, where the user can add / remove or change filters
     */
    public PipelineFilter pipelineFilter() {
        return pipelineFilter;
    }

    /**
     * @param pipelineFilter
     *            Set the filter for the pipeline, where the user can add / remove or change filters
     * @return This class
     */
    public ChannelServerConficuration pipelineFilter(final PipelineFilter pipelineFilter) {
        this.pipelineFilter = pipelineFilter;
        return this;
    }

    /**
     * @return Set the factory for the signature
     */
    public SignatureFactory signatureFactory() {
        return signatureFactory;
    }

    /**
     * @param signatureFactory
     *            Set the factory for the signature
     * @return This class
     */
    public ChannelServerConficuration signatureFactory(final SignatureFactory signatureFactory) {
        this.signatureFactory = signatureFactory;
        return this;
    }

    @Override
    public int connectionTimeoutTCPMillis() {
        return connectionTimeoutTCPMillis;
    }

    public ChannelServerConficuration connectionTimeoutTCPMillis(final int connectionTimeoutTCPMillis) {
        this.connectionTimeoutTCPMillis = connectionTimeoutTCPMillis;
        return this;
    }

    @Override
    public boolean isForceTCP() {
        return forceTCP;
    }

    public ChannelServerConficuration setForceTCP(boolean forceTCP) {
        this.forceTCP = forceTCP;
        return this;
    }

    public ChannelServerConficuration setForceTCP() {
        this.forceTCP = true;
        return this;
    }

    @Override
    public boolean isForceUDP() {
        return forceUDP;
    }
    
    public ChannelServerConficuration setForceUDP(boolean forceUDP) {
        this.forceUDP = forceUDP;
        return this;
    }

    public ChannelServerConficuration setForceUDP() {
        this.forceUDP = true;
        return this;
    }

    
}
