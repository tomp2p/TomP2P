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

import java.util.concurrent.ScheduledExecutorService;

/**
 * A bean that holds sharable configuration settings for the peer. The non-sharable configurations are stored
 * in {@link PeerBean}.
 * 
 * @author Thomas Bocek
 */
public class ConnectionBean {

    public enum Protocol {UDP, TCP, UDT}
    
    /**
     * The thread name is important to identify threads where blocking (wait) is possible.
     */
    public static final String THREAD_NAME = "NETTY-TOMP2P - ";
    
    //non-final to be able to adapt changes
    public static int DEFAULT_TCP_IDLE_MILLIS = 5 * 1000;
    public static int DEFAULT_TCP_IDLE_SLOW_MILLIS = 30 * 1000;
    public static int DEFAULT_UDP_IDLE_MILLIS = 5 * 1000;
    public static int DEFAULT_CONNECTION_TIMEOUT_TCP = 3 * 1000;
    public static int DEFAULT_HEARTBEAT_SECONDS = 2;
    
    public static int UDP_LIMIT = 1400;

    private final int p2pId;
    private final Dispatcher dispatcher;
    private final Sender sender;
    private final Connect connect;
    private final ChannelServer channelServer;
    private final BulkReservation reservation;
    private final ChannelClientConfiguration resourceConfiguration;
    private final ScheduledExecutorService timer;
    
    /**
	 * The connection bean with unmodifiable objects. Once it is set, it cannot be changed. If it is required
	 * to change, then the peer must be shut down and a new one created.
     * 
     * @param p2pId
	 *            The P2P ID
     * @param dispatcher
     *            The dispatcher object that receives all messages
     * @param sender
     *            The sender object that sends out messages
     * @param channelServer
     *            The channel server that listens on incoming connections
     * @param reservation
     *            The connection reservation that is responsible for resource management
     * @param resourceConfiguration
     *            The configuration that is responsible for the resource numbers
     * @param timer
     *            The timer for the discovery process
     */
    public ConnectionBean(final int p2pId, final Dispatcher dispatcher, final Connect connect, final Sender sender,
            final ChannelServer channelServer, final BulkReservation reservation,
            final ChannelClientConfiguration resourceConfiguration, final ScheduledExecutorService timer) {
        this.p2pId = p2pId;
        this.dispatcher = dispatcher;
        this.connect = connect;
        this.sender = sender;
        this.channelServer = channelServer;
        this.reservation = reservation;
        this.resourceConfiguration = resourceConfiguration;
        this.timer = timer;
    }

    /**
	 * @return The P2P ID
     */
    public int p2pId() {
        return p2pId;
    }

    /**
     * @return The dispatcher object that receives all messages
     */
    public Dispatcher dispatcher() {
        return dispatcher;
    }

    /**
     * @return The sender object that sends out messages
     */
    public Sender sender() {
        return sender;
    }
    
    public Connect connect() {
        return connect;
    }
    
    /**
     * @return The channel server that listens on incoming connections
     */
    public ChannelServer channelServer() {
        return channelServer;
    }

    /**
     * @return The connection reservation that is responsible for resource management
     */
    public BulkReservation reservation() {
        return reservation;
    }

    /**
     * @return The configuration that is responsible for the resource numbers
     */
    public ChannelClientConfiguration resourceConfiguration() {
        return resourceConfiguration;
    }

    /**
     * @return The timer used for the discovery
     */
    public ScheduledExecutorService timer() {
        return timer;
    }
}
