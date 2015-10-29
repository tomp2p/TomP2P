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

 import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.MessageID;
import net.tomp2p.peers.PeerStatusListener;

/**
 * Is able to send TCP and UDP messages (as a request) and processes incoming responses. It is important that
 * this class handles
 * close() because if we shutdown the connections, then we need to notify the futures. In case of errors set
 * the peer to
 * offline.
 * 
 * @author Thomas Bocek
 * @param <K>
 *            The type of future to handle
 */
public class RequestHandler<K extends FutureResponse> extends SimpleChannelInboundHandler<Message> {
    private static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);

	// The future response which is currently being waited for
    private final K futureResponse;

	// The node with which this request handler is associated with
    private final PeerBean peerBean;
    private final ConnectionBean connectionBean;

    private final Message message;

    private final MessageID sendMessageID;

    // modifiable variables
    private final int idleTCPMillis; // = ConnectionBean.DEFAULT_TCP_IDLE_SECONDS;
    private final int idleUDPMillis; // = ConnectionBean.DEFAULT_UDP_IDLE_SECONDS;
    private final int connectionTimeoutTCPMillis; // = ConnectionBean.DEFAULT_CONNECTION_TIMEOUT_TCP;
    private final int slowResponseTimeoutSeconds; // = ConnectionBean.DEFAULT_SLOW_RESPONSE_TIMEOUT_SECONDS;
    /**
	 * Creates a request handler that can send TCP and UDP messages.
     * 
     * @param futureResponse
     *            The future that will be called when we get an answer
     * @param peerBean
     *            The peer bean
     * @param connectionBean
     *            The connection bean
     * @param configuration
	 *            the client-side connection configuration
     */
    public RequestHandler(final K futureResponse, final PeerBean peerBean,
            final ConnectionBean connectionBean, final ConnectionConfiguration configuration) {
        this.peerBean = peerBean;
        this.connectionBean = connectionBean;
        this.futureResponse = futureResponse;
        this.message = futureResponse.request();
        this.sendMessageID = new MessageID(message);
        this.idleTCPMillis = configuration.idleTCPMillis();
        this.idleUDPMillis = configuration.idleUDPMillis();
        this.connectionTimeoutTCPMillis = configuration.connectionTimeoutTCPMillis();
        this.slowResponseTimeoutSeconds = configuration.slowResponseTimeoutSeconds();
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
    public int idleTCPMillis() {
        return idleTCPMillis;
    }

    /**
     * @return The time that a UDP connection can be idle
     */
    public int idleUDPMillis() {
        return idleUDPMillis;
    }

    /**
     * @return The time a TCP connection is allowed to be established
     */
    public int connectionTimeoutTCPMillis() {
        return connectionTimeoutTCPMillis;
    }
    
    /**
     * @return The time when a slow response time outs
     */
    public int slowResponseTimeoutSeconds() {
    	return slowResponseTimeoutSeconds;
    }

    /**
     * Sends a UDP message and expects a reply.
     * 
     * @param channelCreator
     *            The channel creator will create a UDP connection
     * @return The future that was added in the constructor
     */
    public K sendUDP(final ChannelCreator channelCreator) {
        connectionBean.sender().sendUDP(this, futureResponse, message, channelCreator, idleUDPMillis, false);
        return futureResponse;
    }

    /**
	 * Broadcasts a UDP message (layer 2) and expects a response.
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
        connectionBean.sender().sendUDP(this, futureResponse, message, channelCreator, idleUDPMillis, true);
        return futureResponse;
    }
    
    /**
	 * Sends a UDP message and doesn't expect a response.
     * 
     * @param channelCreator
     *            The channel creator will create a UDP connection
     * @return The future that was added in the constructor
     */
    public K fireAndForgetBroadcastUDP(final ChannelCreator channelCreator) {
        connectionBean.sender().sendUDP(null, futureResponse, message, channelCreator, 0, true);
        return futureResponse;
    }

    /**
	 * Sends a TCP message and expects a response.
     * 
     * @param channelCreator
     *            The channel creator will create a TCP connection
     * @return The future that was added in the constructor
     */
    public K sendTCP(final ChannelCreator channelCreator) {
        connectionBean.sender().sendTCP(this, futureResponse, message, channelCreator, idleTCPMillis,
                connectionTimeoutTCPMillis, null);
        return futureResponse;
    }
    
	// TODO add JavaDoc
    public K sendTCP(final PeerConnection peerConnection) {
        connectionBean.sender().sendTCP(this, futureResponse, message, null, idleTCPMillis,
                connectionTimeoutTCPMillis, peerConnection);
        return futureResponse;
    }
    
    /**
	 * Sends a TCP message and expects a response.
     * 
     * @param channelCreator
     *            The channel creator will create a TCP connection
	 * @param peerConnection
     * @return The future that was added in the constructor
     */
    public K sendTCP(final ChannelCreator channelCreator, final PeerConnection peerConnection) {
        connectionBean.sender().sendTCP(this, futureResponse, message, channelCreator, idleTCPMillis,
                connectionTimeoutTCPMillis, peerConnection);
        return futureResponse;
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
		LOG.debug("Error originating from {}. Cause {}.", futureResponse.request(), cause);
        if (futureResponse.isCompleted()) {
			LOG.warn("Got exception, but ignored it. (FutureResponse completed.): {}.",
                    futureResponse.failedReason());
        } else {
            if (LOG.isDebugEnabled()) {
				LOG.debug("Exception caught, but handled properly: " + cause.toString());
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
					LOG.debug("Removed from map. Cause: {}. Message: {}.", pe.toString(), message);
                } else {
					LOG.warn("Error in request.", cause);
                }
            } else {
            	synchronized (peerBean.peerStatusListeners()) {
            		for (PeerStatusListener peerStatusListener : peerBean.peerStatusListeners()) {
						peerStatusListener.peerFailed(futureResponse.request().recipient(), new PeerException(cause));
					}
            	}
            }
        }
        
        futureResponse.failedLater(cause);
        ctx.close();
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final Message responseMessage)
            throws Exception {
        MessageID recvMessageID = new MessageID(responseMessage);
        // Error handling
        if (responseMessage.type() == Message.Type.UNKNOWN_ID) {
			String msg = "Message was not delivered successfully, unknown ID (peer may be offline or unknown RPC handler): "
					+ this.message;
            exceptionCaught(ctx, new PeerException(PeerException.AbortCause.PEER_ABORT, msg));
            responseMessage.release();
            return;
		} 
        if (responseMessage.type() == Message.Type.EXCEPTION) {
            String msg = "Message caused an exception on the other side, handle as peer_abort: "
                    + this.message;
            exceptionCaught(ctx, new PeerException(PeerException.AbortCause.PEER_ABORT, msg));
            responseMessage.release();
            return;
		} 
        if (responseMessage.isRequest()) {
            ctx.fireChannelRead(responseMessage);
            return;
		} 
        if (!sendMessageID.equals(recvMessageID)) {
			String msg = "Response message [" + responseMessage
                    + "] sent to the node is not the same as we expect. We sent [" + this.message + "]";
            exceptionCaught(ctx, new PeerException(PeerException.AbortCause.PEER_ABORT, msg));
            responseMessage.release();
            return;
		}
		// We need to exclude RCON Messages from the sanity check because we
		// use this RequestHandler for sending a Type.REQUEST_1,
		// RPC.Commands.RCON message on top of it. Therefore the response
		// type will never be the same Type as the one the user initially
		// used (e.g. DIRECT_DATA).
		/*if (responseMessage.command() != RPC.Commands.RCON.getNr()
				&& message.recipient().isRelayed() != responseMessage.sender().isRelayed()) {
			String msg = "Response message [" + responseMessage + "] sent has a different relay flag than we sent with request message ["
					+ this.message + "]. Recipient (" + message.recipient().isRelayed() + ") / Sender ("
					+ responseMessage.sender().isRelayed() + ")";
			LOG.warn(msg);
        }*/
        
        //NAT reflection, change it back, as this will be stored in our peer map that may be queried from other peers
		if(message.recipientReflected() != null) {
			responseMessage.sender(message.recipient().withIpv4Socket(message.recipient().ipv4Socket()));
		}

        // Stop time measurement of RTT
        futureResponse.stopRTTMeasurement();

        // We got a good answer, let's mark the sender as alive
        //if its an announce, the peer status will be handled in the RPC
		if (responseMessage.isOk() || responseMessage.isNotOk()) {
			LOG.debug("Try adding peer {} to map, {}", responseMessage.sender(), responseMessage);
			peerBean.notifyPeerFound(responseMessage.sender(), null, null, futureResponse.getRoundTripTime());
		}
        
        // call this for streaming support
        if (!responseMessage.isDone()) {
            LOG.debug("Good message is streaming. {}", responseMessage);
            return;
        }
        
        // support slow, unreachable devices which cannot respond instantly
        if(this.message.recipient().relaySize() > 0 && this.message.recipient().slow() && responseMessage.type() == Message.Type.PARTIALLY_OK) {
        	LOG.debug("Received partially ok by the relay peer. Wait for answer of the unreachable peer.");
        	// wait for the (real) answer of the unreachable peer.
        	connectionBean.dispatcher().addPendingRequest(message.messageId(), futureResponse, slowResponseTimeoutSeconds, connectionBean.timer());
        	// close the channel to the relay peer
        	ctx.close();
        	return;
        }
        
        if (!message.isKeepAlive()) {
			LOG.debug("Good message {}. Close channel {}.", responseMessage, ctx.channel());
            //set the success now, but trigger the notify when we closed the channel.
            futureResponse.responseLater(responseMessage); 
			// the channel creator adds a listener that sets futureResponse.setResponseNow, when the channel
			// is closed
            ctx.close();
        } else {
			LOG.debug("Good message {}. Leave channel {} open.", responseMessage, ctx.channel());
            futureResponse.response(responseMessage);
        }
    }
}
