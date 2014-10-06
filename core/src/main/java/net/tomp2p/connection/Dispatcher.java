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

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramChannel;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.tomp2p.connection.PeerException.AbortCause;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;
import net.tomp2p.rpc.RPC.Commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to deliver incoming REQUEST messages to their specific handlers. You can register handlers using the
 * {@link registerIoHandler} function.
 * <p>
 * You probably want to add an instance of this class to the end of a pipeline to be able to receive messages. This
 * class is able to cover several channels but only one P2P network!
 * </p>
 * 
 * @author Thomas Bocek
 */
@Sharable
public class Dispatcher extends SimpleChannelInboundHandler<Message> {

    private static final Logger LOG = LoggerFactory.getLogger(Dispatcher.class);

    private final int p2pID;
    private final PeerBean peerBeanMaster;
    private final int heartBeatMillis;

    /** copy on write map. The key {@link Number320} can be devided into two parts: first {@link Number160} is the peerID that registers,
     * the second {@link Number160} the peerID for which the ioHandler is registered. For example a relay peer can register a handler
     * on behalf of another peer.
     */
    private volatile Map<Number320, Map<Integer, DispatchHandler>> ioHandlers = new HashMap<Number320, Map<Integer, DispatchHandler>>();
    
	/**
	 * Map that stores requests that are not answered yet. Normally, the {@link RequestHandler} handles
	 * responses, however, in case the asked peer has {@link PeerAddress#isSlow()} set to true, the answer
	 * might arrive later. The key of the map is the expected message id.
	 */
    private volatile Map<Integer, FutureResponse> pendingRequests = new HashMap<Integer, FutureResponse>();
    
    /**
     * Constructor.
     * 
     * @param p2pID
     *            the p2p ID the dispatcher is looking for in messages
     * @param peerBean
     *            .
     */
    public Dispatcher(final int p2pID, final PeerBean peerBeanMaster, final int heartBeatMillis) {
        this.p2pID = p2pID;
        this.peerBeanMaster = peerBeanMaster;
        this.heartBeatMillis = heartBeatMillis;
    }

    /**
     * Registers a handler with this dispatcher. Future received messages adhering to the given parameters will be
     * forwarded to that handler. Note that the dispatcher only handles REQUEST messages. This method is thread-safe,
     * and uses copy on write as its expected to run this only during initialization.
     * 
     * @param peerId
     *            Specifies the receiver the dispatcher filters for. This allows to use one dispatcher for several
     *            interfaces or even nodes.
     * @param onBehalfOf
     * 			  The ioHandler can be registered for the own use of in behalf of another peer (e.g. in case of relay node).
     * @param ioHandler
     *            the handler which should process the given type of messages
     * @param names
     *            The command of the {@link Message} the given handler processes. All messages having that command will
     *            be forwarded to the given handler.<br />
     *            <b>Note:</b> If you register multiple handlers with the same command, only the last registered handler
     *            will receive these messages!
     */
    public void registerIoHandler(final Number160 peerId, final Number160 onBehalfOf, final DispatchHandler ioHandler, final int... names) {
        Map<Number320, Map<Integer, DispatchHandler>> copy = new HashMap<Number320, Map<Integer, DispatchHandler>>(ioHandlers);
        Map<Integer, DispatchHandler> types = copy.get(new Number320(peerId, onBehalfOf));
        if (types == null) {
            types = new HashMap<Integer, DispatchHandler>();
            copy.put(new Number320(peerId, onBehalfOf), types);
        }
        for (Integer name : names) {
            types.put(name, ioHandler);
        }
        
        ioHandlers = Collections.unmodifiableMap(copy);
    }

    /**
     * If we shutdown, we remove the handlers. This means that a server may respond that the handler is unknown.
     * 
     * @param peerId
     *            The Id of the peer to remove the handlers .
     * @param onBehalfOf
     * 			  The ioHandler can be registered for the own use of in behalf of another peer (e.g. in case of relay node).
     */
    public void removeIoHandler(final Number160 peerId, final Number160 onBehalfOf) {
        Map<Number320, Map<Integer, DispatchHandler>> copy = new HashMap<Number320, Map<Integer, DispatchHandler>>(ioHandlers);
        copy.remove(new Number320(peerId, onBehalfOf));
        ioHandlers = Collections.unmodifiableMap(copy);
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final Message message) throws Exception {
        LOG.debug("received request {} from channel {}", message, ctx.channel());
        if (message.version() != p2pID) {
            LOG.error("Wrong version. We are looking for {} but we got {}, received: {}", p2pID,
                    message.version(), message);
            ctx.close();
            synchronized (peerBeanMaster.peerStatusListeners()) {
            	for (PeerStatusListener peerStatusListener : peerBeanMaster.peerStatusListeners()) {
                   peerStatusListener.peerFailed(message.sender(), new PeerException(AbortCause.PEER_ERROR, "wrong P2P version"));
            	}
            }
            return;
        }
        
        if (!message.isRequest()) {
            LOG.debug("handing message to the next handler {}", message);
            ctx.fireChannelRead(message);
            return;
        }
        
        if(message.sender().isSlow()) {
        	// This might be a late answer from a slow peer
        	if(pendingRequests.containsKey(message.messageId())) {
        		LOG.debug("Received late response from slow peer: {}", message);
        		FutureResponse futureResponse = pendingRequests.get(message.messageId());
        		futureResponse.response(message);
        	}
        }

        Responder responder = new DirectResponder(ctx, message);
        final DispatchHandler myHandler = associatedHandler(message);
        if (myHandler != null) {
            boolean isUdp = ctx.channel() instanceof DatagramChannel;
            boolean isRelay = message.sender().isRelayed();
            if(isRelay && !message.peerSocketAddresses().isEmpty()) {
            	PeerAddress sender = message.sender().changePeerSocketAddresses(message.peerSocketAddresses());
            	message.sender(sender);
            }
            LOG.debug("about to respond to {}", message);
            PeerConnection peerConnection = new PeerConnection(message.sender(), new DefaultChannelPromise(ctx.channel()).setSuccess(), heartBeatMillis);
            myHandler.forwardMessage(message, isUdp ? null : peerConnection, responder);
        } else {
        	//do better error handling, if a handler is not present at all, print a warning
        	if(ioHandlers.isEmpty()) {
        		LOG.debug("No handler found for {}. Probably we have shutdown this peer.", message);
        	} else {
        		final Collection<Integer> knownCommands = knownCommands();
        		if(!knownCommands.contains(Integer.valueOf(message.command()))) {
            		StringBuilder sb = new StringBuilder("known cmds");
            		for(Integer integer:knownCommands()) {
            			sb.append(", ").append(Commands.find(integer.intValue()));
            		}
            		LOG.warn("No handler found for {}. Did you register the RPC command {}? I have {}.", 
            				message, Commands.find(message.command()), sb);
        		} else {
            		LOG.debug("No handler found for {}. Probably we have partially shutdown this peer.", message);
            	}
        	}
        	
            Message responseMessage = DispatchHandler.createResponseMessage(message, Type.UNKNOWN_ID, peerBeanMaster.serverPeerAddress());
            response(ctx, responseMessage);
        }
    }
    
    private Collection<Integer> knownCommands() {
    	Set<Integer> retVal = new HashSet<Integer>();
    	for(final Map.Entry<Number320, Map<Integer, DispatchHandler>> entry:ioHandlers.entrySet()) {
    		retVal.addAll(entry.getValue().keySet());
    	}
    	return retVal;
    }
    
    public class DirectResponder implements Responder {
        final ChannelHandlerContext ctx;
        final Message requestMessage;
        DirectResponder(final ChannelHandlerContext ctx, final Message requestMessage) {
            this.ctx = ctx;
            this.requestMessage = requestMessage;
        }
        
        @Override
        public void response(Message responseMessage) {
        	
        	if(responseMessage == null || responseMessage.sender() == null) {
        		System.err.println("why");
        	}
        	if(responseMessage.sender().isRelayed()) {
        		responseMessage.peerSocketAddresses(responseMessage.sender().peerSocketAddresses());
    		}
        	
            Dispatcher.this.response(ctx, responseMessage);
        }
        
        @Override
        public void failed(Message.Type type, String reason) {
            Message responseMessage = DispatchHandler.createResponseMessage(requestMessage, type, peerBeanMaster.serverPeerAddress());
            Dispatcher.this.response(ctx, responseMessage);
        }
        
        @Override
		public void responseFireAndForget() {
            LOG.debug("The reply handler was a fire-and-forget handler, we don't send any message back! {}", requestMessage);    
           if (!(ctx.channel() instanceof DatagramChannel)) {
               LOG.warn("There is no TCP fire and forget, use UDP in that case {}", requestMessage);
               throw new RuntimeException("There is no TCP fire and forget, use UDP in that case.");
           } else {
               TimeoutFactory.removeTimeout(ctx);
           }
        }
    }

    /**
     * Respond within a session. Keep the connection open if we are asked to do so. Connection is only kept alive for
     * TCP data.
     * 
     * @param ctx
     *            The channel context
     * @param response
     *            The response to send
     */
    private void response(final ChannelHandlerContext ctx, final Message response) {
        if (ctx.channel() instanceof DatagramChannel) {
            // check if channel is still open. If its not, then do not send
            // anything because
            // this will cause an exception that will be logged.
            if (!ctx.channel().isOpen()) {
                LOG.debug("channel UDP is not open, do not reply {}", response);
                return;
            }
            LOG.debug("reply UDP message {}", response);
        } else {
            // check if channel is still open. If its not, then do not send
            // anything because
            // this will cause an exception that will be logged.
            if (!ctx.channel().isActive()) {
                LOG.debug("channel TCP is not open, do not reply {}", response);
                return;
            }
            LOG.debug("reply TCP message {} to {}", response, ctx.channel().remoteAddress());
        }
        ctx.channel().writeAndFlush(response);
    }

	/**
	 * Checks if we have a handler for the given message.
	 * 
	 * @param message
	 *            the message a handler should be found for
	 * @return the handler for the given message, null if none has been
	 *         registered for that message.
	 */
	public DispatchHandler associatedHandler(final Message message) {
		if (message == null || !(message.isRequest())) {
			return null;
		}

		PeerAddress recipient = message.recipient();

		// Search for handler, 0 is ping. If we send with peerid = ZERO, then we
		// take the first one we found
		if (recipient.peerId().isZero() && message.command() == RPC.Commands.PING.getNr()) {
			Number160 peerId = peerBeanMaster.serverPeerAddress().peerId();
			return searchHandler(peerId, peerId, RPC.Commands.PING.getNr());
			// else we search for the handler that we are responsible for
		} else {
			DispatchHandler handler = searchHandler(recipient.peerId(), recipient.peerId(), message.command());
			if (handler != null) {
				return handler;
			}

			// if we could not find a handler that we are responsible for, we
			// are most likely a relay. Since we have no id of the relay, we
			// just take the first one.
			Map<Number320, DispatchHandler> map = searchHandler(Integer.valueOf(message.command()));
			for (Map.Entry<Number320, DispatchHandler> entry : map.entrySet()) {
				if (entry.getKey().domainKey().equals(recipient.peerId())) {
					return entry.getValue();
				}
			}
			return null;
		}
	}

    /**
     * Looks for a registered handler according to the given parameters.
     * 
     * @param recipientID
     *            The recipient of the message
     * @param onBehalfOf
     * 			  The ioHandler can be registered for the own use of in behalf of another peer (e.g. in case of relay node).
     * @param command
     *            The type of the message to be filtered
     * @return the handler for the given message or null if none has been found
     */
    public DispatchHandler searchHandler(final Number160 recipientID, Number160 onBehalfOf, final int cmd) {
    	final Integer command = Integer.valueOf(cmd);
        Map<Integer, DispatchHandler> types = ioHandlers.get(new Number320(recipientID, onBehalfOf));
        
        if (types != null && types.containsKey(command)) {
            return types.get(command);
        } else {
            // not registered
            LOG.debug("Handler not found for type {} we are looking for the server with ID {}", command,
                    recipientID);
            return null;
        }
    }
    
    /**
     * May take longer.. used for testing
     * @param command
     * @return
     */
    public Map<Number320, DispatchHandler> searchHandler(final Integer command) {
    	Map<Number320, DispatchHandler> result = new HashMap<Number320, DispatchHandler>();
    	for(Map.Entry<Number320, Map<Integer, DispatchHandler>> entry:ioHandlers.entrySet()) {
    		for(Map.Entry<Integer, DispatchHandler> entry2:entry.getValue().entrySet()) {
    			DispatchHandler handlerh = entry.getValue().get(command);
    			if(handlerh!=null && entry2.getKey().equals(command)) {
    				result.put(entry.getKey(), handlerh);
    			}
    		}
    	}
    	return result;
    }

    /**
     * 
     * @param peerId
     * 			  The id of the peer the get the dispatcher map
     * @param onBehalfOf
     * 			  The ioHandler can be registered for the own use of in behalf of another peer (e.g. in case of relay node).
     * @return the map containing all dispatchers for each {@link Commands} type
     */
	public Map<Integer, DispatchHandler> searchHandlerMap(Number160 peerId, Number160 onBehalfOf) {
		Map<Integer, DispatchHandler> ioHandlerMap = ioHandlers.get(new Number320(peerId, onBehalfOf));
		return ioHandlerMap;
	}
	
	/**
	 * Add a new pending request. If slow peers answer, this map will be checked for an entry
	 * 
	 * @param messageId the message id
	 * @param futureResponse the future to respond as soon as a (satisfying) response from the slow peer
	 *            arrived.
	 */
	public void addPendingRequest(int messageId, FutureResponse futureResponse) {
		// TODO add timeout for the pending requests
		pendingRequests.put(messageId, futureResponse);
	}
}
