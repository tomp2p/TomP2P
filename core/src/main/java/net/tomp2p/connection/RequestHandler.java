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
package net.tomp2p.connection;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.MessageID;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.rpc.RPC;

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
public class RequestHandler<K extends FutureResponse> extends SimpleChannelInboundHandler<Message> {
    private static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);

    // The future response which is currently be waited for
    private final K futureResponse;

    // The node this request handler is associated with
    private final PeerBean peerBean;
    private final ConnectionBean connectionBean;

    private final Message message;

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
        this.message = futureResponse.request();
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
    
    public K sendTCP(final PeerConnection peerConnection) {
        connectionBean.sender().sendTCP(this, futureResponse, message, null, idleTCPSeconds,
                connectionTimeoutTCPMillis, peerConnection);
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
        LOG.debug("Error originating from: {}, cause {}", futureResponse.request(), cause);
        if (futureResponse.isCompleted()) {
            LOG.warn("Got exception, but ignored (future response completed): {}",
                    futureResponse.failedReason());
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("exception caugth, but handled properly: " + cause.toString());
            }
            if (cause instanceof PeerException) {
                PeerException pe = (PeerException) cause;
                if (pe.abortCause() != PeerException.AbortCause.USER_ABORT) {
                    // do not force if we ran into a timeout, the peer may be
                    // busy
                    synchronized (peerBean.peerStatusListeners()) {
                    	for (PeerStatusListener peerStatusListener : peerBean.peerStatusListeners()) {
							peerStatusListener.peerFailed(futureResponse.request().recipient(), pe);
						}
                    }
                    LOG.debug("removed from map, cause: {} msg: {}", pe.toString(), message);
                } else {
                    LOG.warn("error in request", cause);
                }
            } else {
            	synchronized (peerBean.peerStatusListeners()) {
            		for (PeerStatusListener peerStatusListener : peerBean.peerStatusListeners()) {
						peerStatusListener.peerFailed(futureResponse.request().recipient(), new PeerException(cause));
					}
            	}
            }
        }
        
        LOG.debug("report failure", cause);
        futureResponse.failedLater(cause);
        ctx.close();
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final Message responseMessage)
            throws Exception {
        MessageID recvMessageID = new MessageID(responseMessage);
        // Error handling
        if (responseMessage.type() == Message.Type.UNKNOWN_ID) {
            String msg = "Message was not delivered successfully, unknow id (peer may be offline or unknown RPC handler): " + this.message;
            exceptionCaught(ctx, new PeerException(PeerException.AbortCause.PEER_ABORT, msg));
            return;
        } else if (responseMessage.type() == Message.Type.EXCEPTION) {
            String msg = "Message caused an exception on the other side, handle as peer_abort: "
                    + this.message;
            exceptionCaught(ctx, new PeerException(PeerException.AbortCause.PEER_ABORT, msg));
            return;
        } else if (responseMessage.isRequest()) {
            ctx.fireChannelRead(responseMessage);
            return;
        } else if (!sendMessageID.equals(recvMessageID)) {
            String msg = "Message [" + responseMessage
                    + "] sent to the node is not the same as we expect. We sent [" + this.message + "]";
            exceptionCaught(ctx, new PeerException(PeerException.AbortCause.PEER_ABORT, msg));
            return;
        } else if (responseMessage.command() != RPC.Commands.RCON.getNr() && message.recipient().isRelayed() != responseMessage.sender().isRelayed()) {
        	String msg = "Message [" + responseMessage
                    + "] sent has a different relay flag than we sent [" + this.message + "]. Recipient ("+message.recipient().isRelayed()+") / Sender ("+responseMessage.sender().isRelayed()+")";
            exceptionCaught(ctx, new PeerException(PeerException.AbortCause.PEER_ABORT, msg));
            return;
        }

        // We got a good answer, let's mark the sender as alive
		if (responseMessage.isOk() || responseMessage.isNotOk()) {
			synchronized (peerBean.peerStatusListeners()) {
				for (PeerStatusListener peerStatusListener : peerBean.peerStatusListeners()) {
					peerStatusListener.peerFound(responseMessage.sender(), null, null);
				}
			}
		}
        
        // call this for streaming support
        futureResponse.progress(responseMessage);
        if (!responseMessage.isDone()) {
            LOG.debug("good message is streaming {}", responseMessage);
            return;
        }
        
        // support slow, unreachable devices which cannot respond instantly
        if(this.message.recipient().isRelayed() && this.message.recipient().isSlow() && responseMessage.type() == Message.Type.PARTIALLY_OK) {
        	LOG.debug("Received partially ok by the relay peer. Wait for answer of the unreachable peer.");
        	// wait for the (real) answer of the unreachable peer.
        	connectionBean.dispatcher().addPendingRequest(message.messageId(), futureResponse);
        	// close the channel to the relay peer
        	ctx.close();
        	return;
        }
        
        if (!message.isKeepAlive()) {
        	LOG.debug("good message, we can close {}, {}", responseMessage, ctx.channel());
            //set the success now, but trigger the notify when we closed the channel.
            futureResponse.responseLater(responseMessage); 
            //the channel creater adds a listener that sets futureResponse.setResponseNow, when the channel is closed
            ctx.close();
        } else {
        	LOG.debug("good message, leave open {}", responseMessage);
            futureResponse.response(responseMessage);
        }
    }
}
