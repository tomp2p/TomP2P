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
 * The connection configuration with the default settings.
 * 
 * @author Thomas Bocek
 * 
 */
public class DefaultConnectionConfiguration implements ConnectionConfiguration {

	protected boolean forceUDP = false;
    protected boolean forceTCP = false;
    protected int idleTCPMillis = ConnectionBean.DEFAULT_TCP_IDLE_MILLIS;
    protected int idleUDPMillis = ConnectionBean.DEFAULT_UDP_IDLE_MILLIS;
    protected int connectionTimeoutTCPMillis = ConnectionBean.DEFAULT_CONNECTION_TIMEOUT_TCP;
    protected int slowResponseTimeoutSeconds = ConnectionBean.DEFAULT_SLOW_RESPONSE_TIMEOUT_SECONDS;

    @Override
    public int idleTCPMillis() {
        return idleTCPMillis;
    }

    /**
     * @param idleTCPSeconds
     *            The time that a connection can be idle before its considered not active for short-lived connections
     * @return This class
     */
    public DefaultConnectionConfiguration idleTCPMillis(final int idleTCPMillis) {
        this.idleTCPMillis = idleTCPMillis;
        return this;
    }

    @Override
    public int idleUDPMillis() {
        return idleUDPMillis;
    }

    /**
     * @param idleUDPSeconds
     *            The time that a connection can be idle before its considered not active for short-lived connections
     * @return This class
     */
    public DefaultConnectionConfiguration idleUDPMillis(final int idleUDPMillis) {
        this.idleUDPMillis = idleUDPMillis;
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
        return forceTCP(true);
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
        return forceUDP(true);
    }
    

	@Override
	public int slowResponseTimeoutSeconds() {
		return slowResponseTimeoutSeconds;
	}
	
	/**
	 * @param slowResponseTimeoutSeconds the amount of seconds a requester waits for the final answer of a
	 *            slow peer. If the slow peer does not answer within this time, the request fails. Make sure
	 *            that this timeout is smaller than the maximum buffer age at the relay peer. Otherwise, a
	 *            timeout is very likely.
	 * @return This class
	 */
	public DefaultConnectionConfiguration slowResponseTimeoutSeconds(final int slowResponseTimeoutSeconds) {
		this.slowResponseTimeoutSeconds = slowResponseTimeoutSeconds;
		return this;
	}
}
