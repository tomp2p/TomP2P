/*
 * Copyright 2009 Thomas Bocek
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
package net.tomp2p.connection2;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.GenericFutureListener;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message2;
import net.tomp2p.message.MessageID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Is able to send UDP messages (as a request) and processes incoming replies. It is important that this class handles
 * close() because if we shutdown the connections, the we need to notify the futures. In case of error set the peer to
 * offline. A similar class is {@link RequestHandlerTCP}, which is used for TCP.
 * 
 * @author Thomas Bocek
 * @param <K>
 *            The type of future to handle
 */
public class RequestHandler<K extends FutureResponse> extends SimpleChannelInboundHandler<Message2> {
    private static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);

    // The future response which is currently be waited for
    private final K futureResponse;

    // The node this request handler is associated with
    private final PeerBean peerBean;
    private final ConnectionBean connectionBean;

    private final Message2 message;

    private final MessageID sendMessageID;

    // modifiable variables
    private final int idleTCPSeconds; // = ConnectionBean.DEFAULT_TCP_IDLE_SECONDS;
    private final int idleUDPSeconds; // = ConnectionBean.DEFAULT_UDP_IDLE_SECONDS;
    private final int connectionTimeoutTCPMillis; // = ConnectionBean.DEFAULT_CONNECTION_TIMEOUT_TCP;

    /**
     * Create a request handler that can send UDP messages.
     * 
     * @param futureResponse
     *            The future that will be called when we get an answer
     * @param peerBean
     *            The peer bean
     * @param connectionBean
     *            The connection bean
     * @param configuration
     *            the client side connection configuration
     */
    public RequestHandler(final K futureResponse, final PeerBean peerBean,
            final ConnectionBean connectionBean, final ConnectionConfiguration configuration) {
        this.peerBean = peerBean;
        this.connectionBean = connectionBean;
        this.futureResponse = futureResponse;
        this.message = futureResponse.getRequest();
        this.sendMessageID = new MessageID(message);
        this.idleTCPSeconds = configuration.idleTCPSeconds();
        this.idleUDPSeconds = configuration.idleUDPSeconds();
        this.connectionTimeoutTCPMillis = configuration.connectionTimeoutTCPMillis();
    }

    /**
     * @return The future response that will be called when we get an answer
     */
    public K futureResponse() {
        return futureResponse;
    }

    /**
     * @return The peer bean
     */
    public PeerBean peerBean() {
        return peerBean;
    }

    /**
     * @return The connection bean
     */
    public ConnectionBean connectionBean() {
        return connectionBean;
    }

    /**
     * @return The time that a TCP connection can be idle
     */
    public int idleTCPSeconds() {
        return idleTCPSeconds;
    }

    /**
     * @return The time that a UDP connection can be idle
     */
    public int idleUDPSeconds() {
        return idleUDPSeconds;
    }

    /**
     * @return The time a TCP connection is allowed to be established
     */
    public int connectionTimeoutTCPMillis() {
        return connectionTimeoutTCPMillis;
    }

    /**
     * Send a UDP message and expect a reply.
     * 
     * @param channelCreator
     *            The channel creator will create a UDP connection
     * @return The future that was added in the constructor
     */
    public K sendUDP(final ChannelCreator channelCreator) {
        connectionBean.sender().sendUDP(this, futureResponse, message, channelCreator, idleUDPSeconds, false);
        return futureResponse;
    }

    /**
     * Send a UDP message and don't expect a reply.
     * 
     * @param channelCreator
     *            The channel creator will create a UDP connection
     * @return The future that was added in the constructor
     */
    public K fireAndForgetUDP(final ChannelCreator channelCreator) {
        connectionBean.sender().sendUDP(null, futureResponse, message, channelCreator, 0, false);
        return futureResponse;
    }

    /**
     * Broadcast a UDP message (layer 2) and expect a reply.
     * 
     * @param channelCreator
     *            The channel creator will create a UDP connection
     * @return The future that was added in the constructor
     */
    public K sendBroadcastUDP(final ChannelCreator channelCreator) {
        connectionBean.sender().sendUDP(this, futureResponse, message, channelCreator, idleUDPSeconds, true);
        return futureResponse;
    }

    /**
     * Send a TCP message and expect a reply.
     * 
     * @param channelCreator
     *            The channel creator will create a TCP connection
     * @return The future that was added in the constructor
     */
    public K sendTCP(final ChannelCreator channelCreator) {
        connectionBean.sender().sendTCP(this, futureResponse, message, channelCreator, idleTCPSeconds,
                connectionTimeoutTCPMillis, null);
        return futureResponse;
    }
    
    /**
     * Send a TCP message and expect a reply.
     * 
     * @param channelCreator
     *            The channel creator will create a TCP connection
     * @return The future that was added in the constructor
     */
    public K sendTCP(final ChannelCreator channelCreator, final PeerConnection peerConnection) {
        connectionBean.sender().sendTCP(this, futureResponse, message, channelCreator, idleTCPSeconds,
                connectionTimeoutTCPMillis, peerConnection);
        return futureResponse;
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
        LOG.debug("Error originating from: {}, cause {}", futureResponse.getRequest(), cause);
        if (futureResponse.isCompleted()) {
            LOG.warn("Got exception, but ignored (future response completed): {}",
                    futureResponse.getFailedReason());
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("exception caugth, but handled properly: " + cause.toString());
            }
            if (cause instanceof PeerException) {
                PeerException pe = (PeerException) cause;
                if (pe.getAbortCause() != PeerException.AbortCause.USER_ABORT) {
                    boolean force = pe.getAbortCause() != PeerException.AbortCause.TIMEOUT;
                    // do not force if we ran into a timeout, the peer may be
                    // busy
                    boolean added = peerBean.peerMap().peerFailed(futureResponse.getRequest().getRecipient(),
                            force);
                    if (added) {
                        if (LOG.isWarnEnabled()) {
                            LOG.warn("removed from map, cause: " + pe.toString() + " msg: " + message);
                        }
                    } else if (LOG.isDebugEnabled()) {
                        LOG.debug(pe.toString() + " msg: " + message);
                    }
                } else if (LOG.isWarnEnabled()) {
                    LOG.warn("error in request", cause);
                }
            } else {
                peerBean.peerMap().peerFailed(futureResponse.getRequest().getRecipient(), true);
            }
        }
        
        LOG.debug("report failure", cause);
        if(futureResponse.setFailedLater(cause)) {
            reportFailed(ctx.close());
        } else {
        	ctx.close();
        }
        
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final Message2 responseMessage)
            throws Exception {
        MessageID recvMessageID = new MessageID(responseMessage);
        // Error handling
        if (responseMessage.getType() == Message2.Type.UNKNOWN_ID) {
            String msg = "Message was not delivered successfully, unknow id (peer may be offline): " + this.message;
            exceptionCaught(ctx, new PeerException(PeerException.AbortCause.PEER_ABORT, msg));
            return;
        } else if (responseMessage.getType() == Message2.Type.EXCEPTION) {
            String msg = "Message caused an exception on the other side, handle as peer_abort: "
                    + this.message;
            exceptionCaught(ctx, new PeerException(PeerException.AbortCause.PEER_ABORT, msg));
            return;
        } else if (!sendMessageID.equals(recvMessageID)) {
            String msg = "Message [" + responseMessage
                    + "] sent to the node is not the same as we expect. We sent [" + this.message + "]";
            exceptionCaught(ctx, new PeerException(PeerException.AbortCause.PEER_ABORT, msg));
            return;
        }

        

        // We got a good answer, let's mark the sender as alive
        if (responseMessage.isOk() || responseMessage.isNotOk()) {
            peerBean.peerMap().peerFound(responseMessage.getSender(), null);
        }
        
        // call this for streaming support
        futureResponse.progress(responseMessage);
        if (!responseMessage.isDone()) {
            LOG.debug("message is streaming {}", responseMessage);
            return;
        }
        
        // Now we now we have the right message
        LOG.debug("perfect: {}", responseMessage);

        if (!message.isKeepAlive()) {
            //set the success now, but trigger the notify when we closed the channel.
            if (futureResponse.setResponseLater(responseMessage)) {
                LOG.debug("close channel {}", responseMessage);
                reportMessage(ctx.close(), responseMessage);
            } else {
            	ctx.close();
            }
        } else {
            futureResponse.setResponse(responseMessage);
        }
    }

    /**
     * Report a successful response after the channel was closed.
     * 
     * @param close
     *            The close future
     * @param responseMessage
     *            The response message
     */
    private void reportMessage(final ChannelFuture close, final Message2 responseMessage) {
        close.addListener(new GenericFutureListener<ChannelFuture>() {
            @Override
            public void operationComplete(final ChannelFuture arg0) throws Exception {
                LOG.debug("report success {}", responseMessage);
                //trigger the notify when we closed the channel.
                futureResponse.setResponseNow();
            }
        });
    }

    /**
     * Report a failure after the channel was closed.
     * 
     * @param close
     *            The close future
     */
    private void reportFailed(final ChannelFuture close) {
        close.addListener(new GenericFutureListener<ChannelFuture>() {
            @Override
            public void operationComplete(final ChannelFuture arg0) throws Exception {
                futureResponse.setResponseNow();
            }
        });
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        futureResponse.setFailed("channel is inactive");
    }
}
