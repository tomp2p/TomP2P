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
 * The ConnectionConfiguration with default settings.
 * 
 * @author Thomas Bocek
 * 
 */
public class DefaultConnectionConfiguration implements ConnectionConfiguration {

    private boolean forceUDP = false;
    private boolean forceTCP = false;
    private int idleTCPSeconds = ConnectionBean.DEFAULT_TCP_IDLE_SECONDS;
    private int idleUDPSeconds = ConnectionBean.DEFAULT_UDP_IDLE_SECONDS;
    private int connectionTimeoutTCPMillis = ConnectionBean.DEFAULT_CONNECTION_TIMEOUT_TCP;
    private int slowResponseTimeoutSeconds = ConnectionBean.DEFAULT_SLOW_RESPONSE_TIMEOUT_SECONDS;

    @Override
    public int idleTCPSeconds() {
        return idleTCPSeconds;
    }

    /**
     * @param idleTCPSeconds
     *            The time that a connection can be idle before its considered not active for short-lived connections
     * @return This class
     */
    public DefaultConnectionConfiguration idleTCPSeconds(final int idleTCPSeconds) {
        this.idleTCPSeconds = idleTCPSeconds;
        return this;
    }

    @Override
    public int idleUDPSeconds() {
        return idleUDPSeconds;
    }

    /**
     * @param idleUDPSeconds
     *            The time that a connection can be idle before its considered not active for short-lived connections
     * @return This class
     */
    public DefaultConnectionConfiguration idleUDPSeconds(final int idleUDPSeconds) {
        this.idleUDPSeconds = idleUDPSeconds;
        return this;
    }

    @Override
    public int connectionTimeoutTCPMillis() {
        return connectionTimeoutTCPMillis;
    }

    /**
     * @param connectionTimeoutTCPMillis
     *            The time a TCP connection is allowed to be established
     * @return This class
     */
    public DefaultConnectionConfiguration connectionTimeoutTCPMillis(final int connectionTimeoutTCPMillis) {
        this.connectionTimeoutTCPMillis = connectionTimeoutTCPMillis;
        return this;
    }

    @Override
    public boolean isForceTCP() {
        return forceTCP;
    }

    /**
     * @param forceTCP
     *            Set to true if the communication should be TCP, default is UDP for routing
     * @return This class
     */
    public DefaultConnectionConfiguration forceTCP(final boolean forceTCP) {
        this.forceTCP = forceTCP;
        return this;
    }

    /**
     * Set to true if the communication should be TCP, default is UDP for routing.
     * 
     * @return This class
     */
    public DefaultConnectionConfiguration forceTCP() {
        this.forceTCP = true;
        return this;
    }

    @Override
    public boolean isForceUDP() {
        return forceUDP;
    }

    /**
     * @param forceUDP
     *            Set to true if the communication should be UDP, default is TCP for request
     * @return This class
     */
    public DefaultConnectionConfiguration forceUDP(final boolean forceUDP) {
        this.forceUDP = forceUDP;
        return this;
    }

    /**
     * Set to true if the communication should be UDP, default is TCP for request.
     * 
     * @return This class
     */
    public DefaultConnectionConfiguration forceUDP() {
        this.forceUDP = true;
        return this;
    }
    

	@Override
	public int slowResponseTimeoutSeconds() {
		return slowResponseTimeoutSeconds;
	}
	
	/**
	 * @param slowResponseTimeoutSeconds the amount of seconds a requester waits for the final answer of a
	 *            slow peer. If the slow peer does not answer within this time, the request fails.
	 * @return This class
	 */
	public DefaultConnectionConfiguration slowResponseTimeoutSeconds(final int slowResponseTimeoutSeconds) {
		this.slowResponseTimeoutSeconds = slowResponseTimeoutSeconds;
		return this;
	}
}
