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
import io.netty.channel.EventLoop;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.MessageID;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.rpc.RPC;
import net.tomp2p.utils.Pair;
import net.tomp2p.utils.Utils;

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
    private final int heartBeatSeconds; // = ConnectionBean.DEFAULT_SLOW_RESPONSE_TIMEOUT_SECONDS;
    
    private final PeerAddress recipient;
    private final PeerAddress sender;
    private final boolean isReflected;
    private final boolean isKeepAlive;
    
    private volatile PeerConnection peerConnection;
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
        this.heartBeatSeconds = configuration.heartBeatSeconds();
        // NAT reflection - rewrite recipient if we found a local address for
	// the recipient
        Pair<PeerAddress, Boolean> pair = handleReflection(message);
        this.recipient = pair.element0();
        this.sender = peerBean.serverPeerAddress();
        this.isReflected = pair.element1();
        this.isKeepAlive = message.isKeepAlive();
    }
    
    private Pair<PeerAddress, Boolean> handleReflection(final Message message) {
	PeerSocketAddress.PeerSocket4Address reflectedRecipient = Utils.natReflection(message.recipient(), peerBean().serverPeerAddress());
	if(reflectedRecipient != null) {
            LOG.debug("reflect recipient {}", message);
            return new Pair<PeerAddress, Boolean>(message.recipient().withIpv4Socket(reflectedRecipient), true);
	}
        return new Pair<PeerAddress, Boolean>(message.recipient(), false);
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
    public int heartBeatSeconds() {
    	return heartBeatSeconds;
    }

    /**
     * Sends a UDP message and expects a reply.
     * 
     * @param channelCreator
     *            The channel creator will create a UDP connection
     * @return The future that was added in the constructor
     */
    public K sendUDP(final ChannelCreator channelCreator) {
        final PeerConnection peerConnection = PeerConnection.newPeerConnectionUDP(channelCreator, recipient, idleTCPMillis);
        final SendBehavior.SendMethod sendMethod = connectionBean.connect().connectUDP(this, channelCreator, sender,
                peerConnection, isReflected, isReflected);
        this.peerConnection = peerConnection;
        switch(sendMethod) {
            case DIRECT:
                connectionBean.sender().sendMessage(futureResponse, message, peerConnection, false);
                break;
            case SELF:
                connectionBean.sender().sendSelf(futureResponse, message);
                break;
        }
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
        final PeerConnection peerConnection = PeerConnection.newPeerConnectionUDP(channelCreator, recipient, idleTCPMillis);
        final SendBehavior.SendMethod sendMethod = connectionBean.connect().connectUDP(this, channelCreator, sender,
                peerConnection, isReflected, isReflected);
        this.peerConnection = peerConnection;
        switch(sendMethod) {
            case DIRECT:
                connectionBean.sender().sendMessage(futureResponse, message, peerConnection, true);
                break;
            case SELF:
                connectionBean.sender().sendSelf(futureResponse, message);
                break;
        }
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
        //connectionBean.sender().sendUDP(null, futureResponse, message, channelCreator, 0, true);
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
        final PeerConnection peerConnection;
        if(isKeepAlive) {
            peerConnection = PeerConnection.newPeerConnectionTCP(channelCreator, recipient, idleTCPMillis);
        } else {
            peerConnection = PeerConnection.newPermanentPeerConnectionTCP(channelCreator, recipient, idleTCPMillis, heartBeatSeconds);
        }
        return sendTCP(peerConnection);
    }

    public K sendTCP(final PeerConnection peerConnection) {
        
        final SendBehavior.SendMethod sendMethod = connectionBean.connect().connectTCP(
                this, connectionTimeoutTCPMillis, sender, peerConnection, isReflected);
        this.peerConnection = peerConnection;
        switch(sendMethod) {
            case DIRECT:
                connectionBean.sender().sendMessage(futureResponse, message, peerConnection, false);
                break;
            case SELF:
                connectionBean.sender().sendSelf(futureResponse, message);
                break;
            case EXISTING_CONNECTION:
                connectionBean.sender().sendTCPPeerConnection(peerConnection, this);
                break;
            case RCON:
                int i = 0;
                for(PeerSocketAddress psa:peerConnection.remotePeer().relays()) {
                    Message rconMessage = createRconMessage(psa, message);
                    
                    i++;
                    if(i>3) break;
                }
                
                
                break;
        }
        return futureResponse;
    }
    
    /**
     * This method makes a copy of the original Message and prepares it for sending it to the relay.
     *
     * @param message
     * @return rconMessage
     */
    private static Message createRconMessage(PeerSocketAddress ps, final Message originalMessage) {
        Message rconMessage = new Message();
        rconMessage.sender(originalMessage.sender());
        rconMessage.version(originalMessage.version());
        // store the message id in the payload to get the cached message later
        rconMessage.intValue(originalMessage.messageId());
        // making the message ready to send
        PeerAddress recipient = originalMessage.recipient().withIPSocket(ps).withRelaySize(0);
        rconMessage.recipient(recipient);
        rconMessage.command(RPC.Commands.RCON.getNr());
        rconMessage.type(Message.Type.REQUEST_1);
        return rconMessage;
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
        
        //futureResponse.failedLater(cause);
        ctx.close();
        peerConnection.closeListener().failAfter(futureResponse, cause);
    }
    
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.READER_IDLE) {
                if(isKeepAlive) {
                    final Message ping = createMessage(recipient, RPC.Commands.PING.getNr(), Message.Type.REQUEST_5);
                    ctx.writeAndFlush(ping);
                } else {
                    exceptionCaught(ctx, new PeerException(PeerException.AbortCause.TIMEOUT, "timetout in request"));
                }
            }
         }
     }
     
     public Message createMessage(final PeerAddress recipient, final byte name, final Message.Type type) {
        return new Message().recipient(recipient).sender(peerBean().serverPeerAddress())
                .command(name).type(type).version(connectionBean().p2pId());
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
        
        if(responseMessage.command() == RPC.Commands.PING.getNr() 
                && responseMessage.type() == Message.Type.PARTIALLY_OK) {
            //ignore interal ping
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
        
        if (!message.isKeepAlive()) {
			LOG.debug("Good message {}. Close channel {}.", responseMessage, ctx.channel());
            //set the success now, but trigger the notify when we closed the channel.
            //futureResponse.responseLater(responseMessage); 
			// the channel creator adds a listener that sets futureResponse.setResponseNow, when the channel
			// is closed
            //final EventLoop loop = ctx.channel().eventLoop();
            //loop.
            /*ctx.close().addListener(new GenericFutureListener<Future<? super Void>>() {
                            @Override
                            public void operationComplete(
                                    Future<? super Void> future) throws Exception {
                                futureResponse.responseNow();
                            }
                        });*/
            ctx.close();
            peerConnection.closeListener().after(futureResponse, responseMessage);
            //futureResponse.response(responseMessage);
        } else {
			LOG.debug("Good message {}. Leave channel {} open.", responseMessage, ctx.channel());
            futureResponse.response(responseMessage);
        }
    }
}
