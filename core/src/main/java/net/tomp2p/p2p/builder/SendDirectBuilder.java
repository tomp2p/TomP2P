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

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.ProgressListener;
import net.tomp2p.message.Buffer;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.SendDirectBuilderI;
import net.tomp2p.utils.Utils;

public class SendDirectBuilder implements ConnectionConfiguration, SendDirectBuilderI{
    private static final FutureDirect FUTURE_REQUEST_SHUTDOWN = new FutureDirect(null)
            .setFailed0("Peer is shutting down");

    private final Peer peer;

    private final PeerAddress recipientAddress;

    private Buffer buffer;

    private FuturePeerConnection recipientConnection;

    private Object object;

    private FutureChannelCreator futureChannelCreator;

    private boolean streaming = false;

    private boolean forceUDP = false;

    private boolean signMessage = false;

    private int idleTCPSeconds = ConnectionBean.DEFAULT_TCP_IDLE_SECONDS;
    private int idleUDPSeconds = ConnectionBean.DEFAULT_UDP_IDLE_SECONDS;
    private int connectionTimeoutTCPMillis = ConnectionBean.DEFAULT_CONNECTION_TIMEOUT_TCP;

    private boolean forceTCP = false;

    private ProgressListener progressListener;

    public SendDirectBuilder(Peer peer, PeerAddress recipientAddress) {
        this.peer = peer;
        this.recipientAddress = recipientAddress;
        this.recipientConnection = null;
    }

    public SendDirectBuilder(Peer peer, FuturePeerConnection recipientConnection) {
        this.peer = peer;
        this.recipientAddress = null;
        this.recipientConnection = recipientConnection;
    }

    public PeerAddress getRecipient() {
        return recipientAddress;
    }

    public Buffer getBuffer() {
        return buffer;
    }

    public SendDirectBuilder setBuffer(Buffer buffer) {
        this.buffer = buffer;
        return this;
    }

    public FuturePeerConnection getConnection() {
        return recipientConnection;
    }

    public SendDirectBuilder setConnection(FuturePeerConnection connection) {
        this.recipientConnection = connection;
        return this;
    }

    public Object getObject() {
        return object;
    }

    public SendDirectBuilder setObject(Object object) {
        this.object = object;
        return this;
    }

    public FutureChannelCreator getFutureChannelCreator() {
        return futureChannelCreator;
    }

    public SendDirectBuilder setFutureChannelCreator(FutureChannelCreator futureChannelCreator) {
        this.futureChannelCreator = futureChannelCreator;
        return this;
    }

    public SendDirectBuilder streaming(boolean streaming) {
        this.streaming = streaming;
        return this;
    }

    public boolean streaming() {
        return streaming;
    }

    public SendDirectBuilder setStreaming() {
        this.streaming = true;
        return this;
    }

    public boolean isRaw() {
        return object == null;
    }

    public FutureDirect start() {
        if (peer.isShutdown()) {
            return FUTURE_REQUEST_SHUTDOWN;
        }

        final boolean keepAlive;
        final PeerAddress remotePeer;
        if (recipientAddress != null && recipientConnection == null) {
            keepAlive = false;
            remotePeer = recipientAddress;
        } else if (recipientAddress == null && recipientConnection != null) {
            keepAlive = true;
            remotePeer = recipientConnection.remotePeer();
        } else {
            throw new IllegalArgumentException("either remotePeer or connection has to be set");
        }
        
        if (futureChannelCreator == null) {
            futureChannelCreator = peer.getConnectionBean().reservation().create(isForceUDP()?1:0, isForceUDP()?0:1);
        }
        
        final RequestHandler<FutureResponse> request = peer.getDirectDataRPC().sendInternal(remotePeer, this);
        if (keepAlive) {
            recipientConnection.addListener(new BaseFutureAdapter<FuturePeerConnection>() {
                @Override
                public void operationComplete(final FuturePeerConnection future) throws Exception {
                    if (future.isSuccess()) {
                        FutureChannelCreator futureChannelCreator2 = recipientConnection.getObject().acquire(request.futureResponse());
                        futureChannelCreator2.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
                            @Override
                            public void operationComplete(FutureChannelCreator future) throws Exception {
                                if(future.isSuccess()) {
                                    request.futureResponse().getRequest().setKeepAlive(true);
                                    request.sendTCP(recipientConnection.getObject().channelCreator(), recipientConnection.getObject());
                                } else {
                                    request.futureResponse().setFailed("Could not acquire channel (2)", future);
                                }
                            }
                            
                        });
                        
                    } else {
                        request.futureResponse().setFailed("Could not acquire channel (1)", future);
                    }

                }
            });

        } else {
            futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
                @Override
                public void operationComplete(final FutureChannelCreator future) throws Exception {
                    if (future.isSuccess()) {
                        final FutureResponse futureResponse = request.sendTCP(future.getChannelCreator());
                        Utils.addReleaseListener(future.getChannelCreator(), futureResponse);
                    } else {
                        request.futureResponse().setFailed("could not create channel", future);
                    }
                }
            });
        }
       
        return new FutureDirect(request.futureResponse());
    }

    public boolean isForceUDP() {
        return forceUDP;
    }

    public SendDirectBuilder setForceUDP(final boolean forceUDP) {
        this.forceUDP = forceUDP;
        return this;
    }

    public SendDirectBuilder setForceUDP() {
        this.forceUDP = true;
        return this;
    }

    /**
     * @return Set to true if the message should be signed. For protecting an entry, this needs to be set to true.
     */
    public boolean isSignMessage() {
        return signMessage;
    }

    /**
     * @param signMessage
     *            Set to true if the message should be signed. For protecting an entry, this needs to be set to true.
     * @return This class
     */
    public SendDirectBuilder setSignMessage(final boolean signMessage) {
        this.signMessage = signMessage;
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see net.tomp2p.p2p.builder.ConnectionConfiguration#idleTCPSeconds()
     */
    @Override
    public int idleTCPSeconds() {
        return idleTCPSeconds;
    }

    /**
     * @param idleTCPSeconds
     *            The time that a connection can be idle before its considered not active for short-lived connections
     * @return This class
     */
    public SendDirectBuilder idleTCPSeconds(final int idleTCPSeconds) {
        this.idleTCPSeconds = idleTCPSeconds;
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see net.tomp2p.p2p.builder.ConnectionConfiguration#idleUDPSeconds()
     */
    @Override
    public int idleUDPSeconds() {
        return idleUDPSeconds;
    }

    /**
     * @param idleUDPSeconds
     *            The time that a connection can be idle before its considered not active for short-lived connections
     * @return This class
     */
    public SendDirectBuilder idleUDPSeconds(final int idleUDPSeconds) {
        this.idleUDPSeconds = idleUDPSeconds;
        return this;
    }

    /**
     * @param connectionTimeoutTCPMillis
     *            The time a TCP connection is allowed to be established
     * @return This class
     */
    public SendDirectBuilder connectionTimeoutTCPMillis(final int connectionTimeoutTCPMillis) {
        this.connectionTimeoutTCPMillis = connectionTimeoutTCPMillis;
        return this;
    }

    /*
     * (non-Javadoc)
     * 
     * @see net.tomp2p.p2p.builder.ConnectionConfiguration#connectionTimeoutTCPMillis()
     */
    @Override
    public int connectionTimeoutTCPMillis() {
        return connectionTimeoutTCPMillis;
    }

    /**
     * @return Set to true if the communication should be TCP, default is UDP for routing
     */
    public boolean isForceTCP() {
        return forceTCP;
    }

    /**
     * @param forceTCP
     *            Set to true if the communication should be TCP, default is UDP for routing
     * @return This class
     */
    public SendDirectBuilder setForceTCP(final boolean forceTCP) {
        this.forceTCP = forceTCP;
        return this;
    }

    /**
     * @return Set to true if the communication should be TCP, default is UDP for routing
     */
    public SendDirectBuilder setForceTCP() {
        this.forceTCP = true;
        return this;
    }

    public SendDirectBuilder progressListener(ProgressListener progressListener) {
        this.progressListener = progressListener;
        return this;
    }

    public ProgressListener progressListener() {
        return progressListener;
    }
}
