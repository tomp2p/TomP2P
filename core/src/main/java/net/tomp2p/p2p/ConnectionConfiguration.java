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

public class ConnectionConfiguration {
    // discover timeout
    private int discoverTimeoutSec = 5;

    // private int maxNrBeforeExclude = 2;
    // The default is not to assume that you are behind firewall
    private boolean behindFirewall = false;

    private int trackerTimoutSeconds = 60;

    private boolean disableBind = false;

    // disabel or enable the limitation of tracker results. If set to true, the
    // tracker will return 35 entries. If set
    // to false, it will return all of them.
    private boolean limitTracker = true;

    // connection configuration
    // idle needs to be larger than timeout for TCP

    // doing tests on localhost, we open 2 * maxOpenConnection
    private int maxOpenConnection = 300;

    private int maxCreating = 100;

    // these values depend on how many connections we create
    private int idleTCPMillis = (maxOpenConnection + maxCreating) * 20;

    private int idleUDPMillis = (maxOpenConnection + maxCreating) * 10;

    private int connectTimeouMillis = (maxOpenConnection + maxCreating) * 10;

    // force TCP or UDP
    private boolean forceTrackerTCP = false;

    private boolean forceStorageUDP = false;

    public void discoverTimeoutSec(int discoverTimeoutSec) {
        this.discoverTimeoutSec = discoverTimeoutSec;
    }

    public int discoverTimeoutSec() {
        return discoverTimeoutSec;
    }

    /**
     * By setting this flag, the peer assumes that it is behind a firewall and
     * will announce itself as unreachable. As soon as this peer receives an
     * incoming message from its advertised address, the peer marks itself as
     * reachable. To receive an incoming message, the peer has to call
     * {@link Peer#discover(net.tomp2p.peers.PeerAddress)} to mark itself as
     * reachable.
     * 
     * @param behindFirewall
     *            If set to true, peer is assumed to be behind firewall and is
     *            unreable.
     */
    public void behindFirewall(boolean behindFirewall) {
        this.behindFirewall = behindFirewall;
    }

    public boolean isBehindFirewall() {
        return behindFirewall;
    }

    public int trackerTimoutSeconds() {
        return trackerTimoutSeconds;
    }

    public void setTrackerTimoutSeconds(int trackerTimoutSeconds) {
        this.trackerTimoutSeconds = trackerTimoutSeconds;
    }

    public boolean isDisableBind() {
        return disableBind;
    }

    public void disableBind(boolean disableBind) {
        this.disableBind = disableBind;
    }

    public boolean isLimitTracker() {
        return limitTracker;
    }

    public void limitTracker(boolean limitTracker) {
        this.limitTracker = limitTracker;
    }

    public int idleTCPMillis() {
        return idleTCPMillis;
    }

    public void idleTCPMillis(int idleTCPMillis) {
        this.idleTCPMillis = idleTCPMillis;
    }

    public int idleUDPMillis() {
        return idleUDPMillis;
    }

    public void idleUDPMillis(int idleUDPMillis) {
        this.idleUDPMillis = idleUDPMillis;
    }

    public int connectTimeoutMillis() {
        return connectTimeouMillis;
    }

    public void connectTimeoutMillis(int connectTimeouMillist) {
        this.connectTimeouMillis = connectTimeouMillist;
    }

    public void maxOpenConnection(int maxOpenConnection) {
        this.maxOpenConnection = maxOpenConnection;
    }

    public int maxOpenConnection() {
        return maxOpenConnection;
    }

    public int maxCreating() {
        return maxCreating;
    }

    public void maxCreating(int maxCreating) {
        this.maxCreating = maxCreating;
    }

    public boolean isForceTrackerTCP() {
        return forceTrackerTCP;
    }

    public void forceTrackerTCP(boolean forceTrackerTCP) {
        this.forceTrackerTCP = forceTrackerTCP;
    }

    public boolean isForceStorageUDP() {
        return forceStorageUDP;
    }

    public void forceStorageUDP(boolean forceStorageUDP) {
        this.forceStorageUDP = forceStorageUDP;
    }
}
