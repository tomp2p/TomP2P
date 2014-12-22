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
package net.tomp2p.rpc;

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.PeerException;
import net.tomp2p.connection.Responder;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatusListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The dispatcher handlers that can be added to the Dispatcher.
 * 
 * @author Thomas Bocek
 * 
 */
public abstract class DispatchHandler {
    private static final Logger LOG = LoggerFactory.getLogger(DispatchHandler.class);

    private final PeerBean peerBean;

    private final ConnectionBean connectionBean;

    private boolean sign = false;

    /**
     * Creates a handler with a peer bean and a connection bean.
     * 
     * @param peerBean
     *            The peer bean
     * @param connectionBean
     *            The connection bean
     * @param names
     *            The command names
     */
    protected DispatchHandler(final PeerBean peerBean, final ConnectionBean connectionBean) {
        this.peerBean = peerBean;
        this.connectionBean = connectionBean;
    }
    
    /**
     * Registers all names on the dispatcher on behalf of the own peer
     * @param names
     */
    public void register(final int... names) {
    	Number160 onBehalfOf = peerBean.serverPeerAddress().peerId();
    	register(onBehalfOf, names);
    }
    
    /**
     * Registers all names on the dispatcher on behalf of the given peer
      * @param onBehalfOf
     * 			  The ioHandler can be registered for the own use of in behalf of another peer (e.g. in case of a relay node).
    * @param names
     */
    public void register(Number160 onBehalfOf, final int... names) {
        connectionBean.dispatcher().registerIoHandler(peerBean.serverPeerAddress().peerId(), onBehalfOf, this, names);
    }

    /**
     * @param sign
     *            Set to true if the message is signed
     */
    public void sign(final boolean sign) {
        this.sign = sign;
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
     * Creates a request message and fills it with peer bean and connection bean parameters.
     * 
     * @param recipient
     *            The recipient of this message
     * @param name
     *            The commend type
     * @param type
     *            The request type
     * @return The created request message
     */
    public Message createMessage(final PeerAddress recipient, final byte name, final Type type) {
        return new Message().recipient(recipient).sender(peerBean().serverPeerAddress())
                .command(name).type(type).version(connectionBean().p2pId());
    }

    /**
     * Creates a response message and fills it with peer bean and connection bean parameters.
     * 
     * @param requestMessage
     *            The request message
     * @param replyType
     *            The type of the reply
     * @return The response message
     */
    public Message createResponseMessage(final Message requestMessage, final Type replyType) {
        return createResponseMessage(requestMessage, replyType, peerBean().serverPeerAddress());
    }
    
    public static Message createResponseMessage(final Message requestMessage, final Type replyType, final PeerAddress peerAddress) {
        Message replyMessage = new Message();
        // this will have the ports > 40'000 that we need to know for sendig the reply
        replyMessage.senderSocket(requestMessage.senderSocket());
        replyMessage.recipientSocket(requestMessage.recipientSocket());
        replyMessage.recipient(requestMessage.sender());
        replyMessage.sender(peerAddress);
        replyMessage.command(requestMessage.command());
        replyMessage.type(replyType);
        replyMessage.version(requestMessage.version());
        replyMessage.messageId(requestMessage.messageId());
        replyMessage.udp(requestMessage.isUdp());
        return replyMessage;
    }

    /**
     * Forwards the request to a handler.
     * 
     * @param requestMessage
     *            The request message
     * @param peerConnection The peer connection that can be used for communication
     * @param responder 
     * @return The response message
     */
    public void forwardMessage(final Message requestMessage, PeerConnection peerConnection, Responder responder) {
        // here we need a referral, since we got contacted and we don't know
        // if we can contact the peer with its address. The peer may be
        // behind a NAT
        synchronized (peerBean.peerStatusListeners()) {
			for (PeerStatusListener peerStatusListener : peerBean.peerStatusListeners()) {
				peerStatusListener.peerFound(requestMessage.sender(), requestMessage.sender(), peerConnection);
			}
		}
        
        try {
            handleResponse(requestMessage, peerConnection, sign, responder);
        } catch (Throwable e) {
        	synchronized (peerBean.peerStatusListeners()) {
        		for (PeerStatusListener peerStatusListener : peerBean.peerStatusListeners()) {
					peerStatusListener.peerFailed(requestMessage.sender(), new PeerException(e));
				}
        	}
        	LOG.error("Exception in custom handler", e);
            responder.failed(Type.EXCEPTION , e.toString());
        }
    }

    /**
     * If the message is OK, that has been previously checked by the user using checkMessage, a response to the message is
     * generated here.
     * 
     * @param message
     *            Request message
     * @param peerConnection 
     * @param sign
     *            Flag indicating whether the message is signed
     * @param responder 
     * @throws Exception
     *             Any exception
     */
    public abstract void handleResponse(Message message, PeerConnection peerConnection, boolean sign, Responder responder) throws Exception;

}
