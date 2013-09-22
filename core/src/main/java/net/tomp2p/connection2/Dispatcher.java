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
package net.tomp2p.connection2;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.socket.DatagramChannel;
import net.tomp2p.message.Message2;
import net.tomp2p.message.Message2.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatusListener;
import net.tomp2p.rpc.DispatchHandler;

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
public class Dispatcher extends SimpleChannelInboundHandler<Message2> {

    private static final Logger LOG = LoggerFactory.getLogger(Dispatcher.class);

    private final int p2pID;
    private final PeerBean peerBean;

    private volatile Map<Number160, Map<Integer, DispatchHandler>> ioHandlers = new HashMap<Number160, Map<Integer, DispatchHandler>>();

    /**
     * Constructor.
     * 
     * @param p2pID
     *            the p2p ID the dispatcher is looking for in messages
     * @param peerBean
     *            .
     */
    public Dispatcher(final int p2pID, final PeerBean peerBean) {
        this.p2pID = p2pID;
        this.peerBean = peerBean;
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
        Map<Number160, Map<Integer, DispatchHandler>> copy = ioHandlers == null ? new HashMap<Number160, Map<Integer, DispatchHandler>>()
                : new HashMap<Number160, Map<Integer, DispatchHandler>>(ioHandlers);
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
        if (ioHandlers == null) {
            return;
        }
        Map<Number160, Map<Integer, DispatchHandler>> copy = ioHandlers == null ? new HashMap<Number160, Map<Integer, DispatchHandler>>()
                : new HashMap<Number160, Map<Integer, DispatchHandler>>(ioHandlers);
        copy.remove(peerId);
        ioHandlers = Collections.unmodifiableMap(copy);
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final Message2 message) throws Exception {

        LOG.debug("received request {}", message);
        if (message.getVersion() != p2pID) {
            LOG.error("Wrong version. We are looking for {} but we got {}, received: {}", p2pID,
                    message.getVersion(), message);
            ctx.close();
            for (PeerStatusListener peerStatusListener : peerBean.peerStatusListeners()) {
                peerStatusListener.peerFailed(message.getSender(), true);
            }
            return;
        }
        Message2 responseMessage = null;
        final DispatchHandler myHandler = getAssociatedHandler(message);
        if (myHandler != null) {
            LOG.debug("about to respond to {}", message);
            responseMessage = myHandler.forwardMessage(message);
            if (responseMessage == null) {
                LOG.warn("Repsonse message was null, probaly a custom handler failed {}", message);
                message.setRecipient(message.getSender()).setSender(peerBean.serverPeerAddress())
                        .setType(Type.EXCEPTION);
                response(ctx, message);
            } else if (responseMessage == message) {
            	 LOG.debug("The reply handler was a fire-and-forget handler, "
                         + "we don't send any message back! {}", message);    
                if (!(ctx.channel() instanceof DatagramChannel)) {
                    LOG.warn("There is no TCP fire and forget, use UDP in that case {}", message);
                	throw new RuntimeException("There is no TCP fire and forget, use UDP in that case.");
                }
            } else {
                response(ctx, responseMessage);
            }
        } else {
            LOG.warn("No handler found for {}. Probably we have shutdown this peer.", message);
            message.setRecipient(message.getSender()).setSender(peerBean.serverPeerAddress())
                    .setType(Type.UNKNOWN_ID);
            response(ctx, message);
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
    private void response(final ChannelHandlerContext ctx, final Message2 response) {
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
    private DispatchHandler getAssociatedHandler(final Message2 message) {
        if (message == null || !(message.isRequest())) {
            return null;
        }
        PeerAddress recipient = message.getRecipient();
        // Search for handler, 0 is ping
        if (recipient.getPeerId().isZero() && message.getCommand() == 0) {
            return searchHandler(peerBean.serverPeerAddress().getPeerId(), 0);
        } else {
            return searchHandler(recipient.getPeerId(), Integer.valueOf(message.getCommand()));
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
    private DispatchHandler searchHandler(final Number160 recipientID, final Integer command) {
        if (ioHandlers == null) {
            return null;
        }
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
}
