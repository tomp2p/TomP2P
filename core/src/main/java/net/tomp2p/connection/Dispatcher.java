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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.peers.PeerStatusListener.FailReason;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;

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
    private final PeerBean peerBean;
    private final int heartBeatMillis;

    //copy on write map
    private volatile Map<Number160, Map<Integer, DispatchHandler>> ioHandlers = new HashMap<Number160, Map<Integer, DispatchHandler>>();

    /**
     * Constructor.
     * 
     * @param p2pID
     *            the p2p ID the dispatcher is looking for in messages
     * @param peerBean
     *            .
     */
    public Dispatcher(final int p2pID, final PeerBean peerBean, final int heartBeatMillis) {
        this.p2pID = p2pID;
        this.peerBean = peerBean;
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
     * @param ioHandler
     *            the handler which should process the given type of messages
     * @param names
     *            The command of the {@link Message} the given handler processes. All messages having that command will
     *            be forwarded to the given handler.<br />
     *            <b>Note:</b> If you register multiple handlers with the same command, only the last registered handler
     *            will receive these messages!
     */
    public void registerIoHandler(final Number160 peerId, final DispatchHandler ioHandler, final int... names) {
        Map<Number160, Map<Integer, DispatchHandler>> copy = new HashMap<Number160, Map<Integer, DispatchHandler>>(ioHandlers);
        Map<Integer, DispatchHandler> types = copy.get(peerId);
        if (types == null) {
            types = new HashMap<Integer, DispatchHandler>();
            copy.put(peerId, types);
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
     */
    public void removeIoHandler(final Number160 peerId) {
        Map<Number160, Map<Integer, DispatchHandler>> copy = new HashMap<Number160, Map<Integer, DispatchHandler>>(ioHandlers);
        copy.remove(peerId);
        ioHandlers = Collections.unmodifiableMap(copy);
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final Message message) throws Exception {
        LOG.debug("received request {} from channel {}", message, ctx.channel());
        if (message.version() != p2pID) {
            LOG.error("Wrong version. We are looking for {} but we got {}, received: {}", p2pID,
                    message.version(), message);
            ctx.close();
            synchronized (peerBean.peerStatusListeners()) {
            	for (PeerStatusListener peerStatusListener : peerBean.peerStatusListeners()) {
                    peerStatusListener.peerFailed(message.sender(), FailReason.Exception);
                }
            }
            return;
        }
        if (!message.isRequest()) {
            LOG.debug("handing message to the next handler {}", message);
            ctx.fireChannelRead(message);
            return;
        }
        //Message responseMessage = null;
        Responder responder = new DirectResponder(ctx, message);
        final DispatchHandler myHandler = associatedHandler(message);
        if (myHandler != null) {
            boolean isUdp = ctx.channel() instanceof DatagramChannel;
            boolean isRelay = message.sender().isRelayed();
            if(isRelay) {
            	PeerAddress sender = message.sender().changePeerSocketAddresses(message.peerSocketAddresses());
            	message.sender(sender);
            }
            LOG.debug("about to respond to {}", message);
            PeerConnection peerConnection = new PeerConnection(message.sender(), new DefaultChannelPromise(ctx.channel()).setSuccess(), heartBeatMillis);
            myHandler.forwardMessage(message, isUdp ? null : peerConnection, responder);
        } else {
            LOG.debug("No handler found for {}. Probably we have shutdown this peer.", message);
            Message responseMessage = DispatchHandler.createResponseMessage(message, Type.UNKNOWN_ID, peerBean.serverPeerAddress());
            response(ctx, responseMessage);
        }
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
            Message responseMessage = DispatchHandler.createResponseMessage(requestMessage, type, peerBean.serverPeerAddress());
            Dispatcher.this.response(ctx, responseMessage);
        }
        
        @Override
		public void responseFireAndForget() {
            LOG.debug("The reply handler was a fire-and-forget handler, "
                    + "we don't send any message back! {}", requestMessage);    
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
     * @return the handler for the given message, null if none has been registered for that message.
     */
    public DispatchHandler associatedHandler(final Message message) {
        if (message == null || !(message.isRequest())) {
            return null;
        }
        PeerAddress recipient = message.recipient();
        // Search for handler, 0 is ping
        if (recipient.peerId().isZero() && message.command() == RPC.Commands.PING.getNr()) {
            return searchHandler(peerBean.serverPeerAddress().peerId(), RPC.Commands.PING.getNr());
        } else {
            return searchHandler(recipient.peerId(), message.command());
        }
    }

    /**
     * Looks for a registered handler according to the given parameters.
     * 
     * @param recipientID
     *            The recipient of the message
     * @param command
     *            The type of the message to be filtered
     * @return the handler for the given message or null if none has been found
     */
    public DispatchHandler searchHandler(final Number160 recipientID, final int cmd) {
    	final Integer command = Integer.valueOf(cmd);
        Map<Integer, DispatchHandler> types = ioHandlers.get(recipientID);
        
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
    public Map<Number160, DispatchHandler> searchHandler(final Integer command) {
    	Map<Number160, DispatchHandler> result = new HashMap<Number160, DispatchHandler>();
    	for(Map.Entry<Number160, Map<Integer, DispatchHandler>> entry:ioHandlers.entrySet()) {
    		for(Map.Entry<Integer, DispatchHandler> entry2:entry.getValue().entrySet()) {
    			DispatchHandler handlerh = entry.getValue().get(command);
    			if(handlerh!=null && entry2.getKey().equals(command)) {
    				result.put(entry.getKey(), handlerh);
    			}
    		}
    	}
    	return result;
    }

	public Map<Integer, DispatchHandler> searchHandlerMap(Number160 peerId) {
		Map<Integer, DispatchHandler> ioHandlerMap = ioHandlers.get(peerId);
		return ioHandlerMap;
	}
}
