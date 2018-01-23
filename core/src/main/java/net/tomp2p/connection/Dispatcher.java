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

import java.io.IOException;
import java.nio.channels.DatagramChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.sctp4nat.core.SctpChannelFacade;
import net.tomp2p.connection.PeerException.AbortCause;
import net.tomp2p.futures.FutureDone;
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

import org.jdeferred.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to deliver incoming REQUEST messages to their specific handlers.
 * <p>
 * You probably want to add an instance of this class to the end of a pipeline to be able to receive messages. This
 * class is able to cover several channels but only one P2P network!
 * </p>
 * 
 * @author Thomas Bocek
 */
public class Dispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(Dispatcher.class);

    private final int p2pID;
    private final PeerBean peerBeanMaster;

    //use locks instead copy on write as testcases became really slow
    final private ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
    final private Lock readLock = reentrantReadWriteLock.readLock();
    final private Lock writeLock = reentrantReadWriteLock.writeLock();
    final private SortedMap<Number320, Map<Integer, DispatchHandler>> ioHandlers = new TreeMap<Number320, Map<Integer, DispatchHandler>>();
    
	/**
	 * Map that stores requests that are not answered yet. Normally, the {@link RequestHandler} handles
	 * responses.
	 */
    //final private Map<Integer, FutureResponse> pendingRequests = new ConcurrentHashMap<Integer, FutureResponse>();

    final private ChannelServerConfiguration csc;
    /**
     * Creates a dispatcher.
     *
     * @param p2pID
     *            The P2P ID the dispatcher is looking for incoming messages
     * @param peerBeanMaster
     *            .
     */
    public Dispatcher(final int p2pID, final PeerBean peerBeanMaster, ChannelServerConfiguration csc) {
        this.p2pID = p2pID;
        this.peerBeanMaster = peerBeanMaster;
        this.csc = csc;
    }
    
    public PeerBean peerBean() {
    	return peerBeanMaster;
    }

    /**
     * Registers a handler with this dispatcher. Future received messages adhering to the given parameters will be
     * forwarded to that handler. Note that the dispatcher only handles REQUEST messages. This method is thread-safe,
     * and uses copy on write as it is expected to run this only during initialization.
     * 
     * @param peerId
     *            Specifies the receiver the dispatcher filters for. This allows to use one dispatcher for several
     *            interfaces or even nodes.
     * @param onBehalfOf
     * 			  The ioHandler can be registered for the own use in behalf of another peer. (E.g., in case of a relay node.)
     * @param ioHandler
     *            The handler which should process the given type of messages
     * @param names
     *            The command of the {@link Message} the given handler processes. All messages having that command will
     *            be forwarded to the given handler.<br />
     *            <b>Note:</b> If you register multiple handlers with the same command, only the last registered handler
     *            will receive these messages!
     */
    public void registerIoHandler(final Number160 peerId, final Number160 onBehalfOf, final DispatchHandler ioHandler, final int... names) {
    	writeLock.lock();
    	try {
    		Map<Integer, DispatchHandler> types = ioHandlers.get(new Number320(peerId, onBehalfOf));
    		if (types == null) {
    			types = new HashMap<Integer, DispatchHandler>();
    			ioHandlers.put(new Number320(peerId, onBehalfOf), types);
    		}
    		for (Integer name : names) {
    			types.put(name, ioHandler);
    		}
    	} finally {
    		writeLock.unlock();
    	}
        
    }

    /**
     * If we shutdown, we remove the handlers. This means that a server may respond that the handler is unknown.
     * 
     * @param peerId
     *            The Id of the peer to remove the handlers .
     * @param onBehalfOf
     * 			  The ioHandler can be registered for the own use in behalf of another peer (e.g. in case of relay node).
     */
    public void removeIoHandler(final Number160 peerId, final Number160 onBehalfOf) {
        writeLock.lock();
    	try {
    		ioHandlers.remove(new Number320(peerId, onBehalfOf));
    	}  finally {
    		writeLock.unlock();
    	}
    }
    
    public void removeIoHandler(final Number160 peerId) {
    	writeLock.lock();
    	try {
    		Number320 min = new Number320(peerId, Number160.ZERO); 
    		Number320 max = new Number320(peerId, Number160.MAX_VALUE);
    		ioHandlers.subMap(min, max).clear();
    	}  finally {
    		writeLock.unlock();
    	}
    }

    public void exceptionCaught(DatagramChannel ctx, Throwable cause) {
		try {
			ctx.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

    public void dispatch(Responder responder, final Message message, Promise<SctpChannelFacade, Exception, Void> p, ChannelSender sender) throws IOException {
        LOG.debug("Received request message {}", message);
        if (message.version() != p2pID) {
            LOG.error("Wrong version. We are looking for {}, but we got {}. Received: {}.", p2pID,
                    message.version(), message);
            synchronized (peerBeanMaster.peerStatusListeners()) {
            	for (PeerStatusListener peerStatusListener : peerBeanMaster.peerStatusListeners()) {
                   peerStatusListener.peerFailed(message.sender(), new PeerException(AbortCause.PEER_ERROR, "Wrong P2P version."));
            	}
            }
            return;
        }
        
        if(message.isAck()) {
        	peerBeanMaster.notifyPeerFound(message.sender(), null, null);
        	return;
        } else if((message.isOk() || message.isNotOk()) && message.isVerified()) {
        	//peer is online, mark as such
        	peerBeanMaster.notifyPeerFound(message.sender(), null, null);
        }
        
        final DispatchHandler myHandler = associatedHandler(message);
        if (myHandler != null) {
            LOG.debug("About to respond to request message {}.", message);
            myHandler.forwardMessage(responder, message, p, sender);
        } else {
        	if (LOG.isWarnEnabled()) {
        		printWarnMessage(message);
        	}
        	responder.response(DispatchHandler.createResponseMessage(message, Type.UNKNOWN_ID, peerBeanMaster.serverPeerAddress()));
        }
    }
    
    /**
     * This is rather a slow operation, but it is only called when log level set to warning
     */
    private void printWarnMessage(Message message) {
    	final Collection<Integer> knownCommands = new HashSet<Integer>();
    	
    	readLock.lock();
    	try {
    		for(final Map.Entry<Number320, Map<Integer, DispatchHandler>> entry:ioHandlers.entrySet()) {
    			knownCommands.addAll(entry.getValue().keySet());
    		}
    	} finally {
    		readLock.unlock();
    	}
    	
    	if(!knownCommands.contains(Integer.valueOf(message.command()))) {
    		StringBuilder sb = new StringBuilder("known cmds");
    		for(Integer integer:knownCommands) {
    			sb.append(", ").append(Commands.find(integer.intValue()));
    		}
    		LOG.warn("No handler found for {}. Did you register the RPC command {}? I have {}.", 
        		message, Commands.find(message.command()), sb);
    	} else if(knownCommands.isEmpty()) {
    		LOG.debug("No handler found for {}. Probably we have shutdown this peer.", message);
    	}
    	else {
    		LOG.debug("No handler found for {}. Probably we have partially shutdown this peer.", message);
    	}
    }

	/**
	 * Returns the registered handler for the provided message, if any.
	 * 
	 * @param message
	 *            The message a handler should be found for
	 * @return The handler for the provided message or null, if none has been registered for that message.
	 */
	public DispatchHandler associatedHandler(final Message message) {
		if (message == null || !message.isRequest()) {
			return null;
		}

		PeerAddress recipient = message.recipient();

		// Search for handler, 0 is ping. If we send with peerid = ZERO, then we
		// take the first one we found
		if (recipient.peerId().isZero() && 
				(message.command() == RPC.Commands.PING.getNr())) {
			Number160 peerId = peerBeanMaster.serverPeerAddress().peerId();
			return searchHandler(peerId, peerId, message.command());
			// else we search for the handler that we are responsible for
		} else {
			//search first for main peer and port sharing peers
			DispatchHandler handler = searchHandler(recipient.peerId(), recipient.peerId(), message.command());
			if (handler != null) {
				return handler;
			}
			
			//on behalf on peers
			handler = searchHandler(peerBean().serverPeerAddress().peerId(), recipient.peerId(), message.command());
			if (handler != null) {
				return handler;
			}
			LOG.warn("No handler found for {} on behalf of {}, command {}. Message is {}", peerBean().serverPeerAddress().peerId(), recipient.peerId(), RPC.Commands.find(message.command()), message);
			return null;
		}
	}
	
	//not relaying
	public boolean isPrimaryTarget(final Number160 recipientID) {
		readLock.lock();
		final Map<Integer, DispatchHandler> ioHandlers;
		try {
			ioHandlers = search(recipientID, recipientID);
		} finally {
			readLock.unlock();
		}
		if(ioHandlers == null) {
			return false;
		}
		return true;
	}

    /**
     * Looks for a registered handler according to the given parameters.
     * 
     * @param recipientID
     *            The ID of the recipient of the message.
     * @param onBehalfOf
     * 			  The ID of the onBehalfOf peer.
     * @param cmd
     *            The command of the message to be filtered for
     * @return The handler for the provided parameters or null, if none has been found.
     */
    public DispatchHandler searchHandler(final Number160 recipientID, final Number160 onBehalfOf, final int cmd) {
		final Integer command = Integer.valueOf(cmd);
		readLock.lock();
		try {
			final Map<Integer, DispatchHandler> types = search(recipientID, onBehalfOf);
			if (types != null && types.containsKey(command)) {
				return types.get(command);
			} else {
				// not registered
				LOG.debug(
						"Handler not found for type {} we are looking for the server with ID {} on behalf of {}",
						RPC.Commands.values()[command], recipientID, onBehalfOf);
				return null;
			}
		} finally {
			readLock.unlock();
		}
    }
    
    /**
     * May take longer.. used for testing
     * @param command
     * @return
     */
    public Map<Number320, DispatchHandler> searchHandler(final Integer command) {
		readLock.lock();
		try {
			Map<Number320, DispatchHandler> result = new HashMap<Number320, DispatchHandler>();
			for (Map.Entry<Number320, Map<Integer, DispatchHandler>> entry : ioHandlers.entrySet()) {
				for (Map.Entry<Integer, DispatchHandler> entry2 : entry.getValue().entrySet()) {
					DispatchHandler handler = entry.getValue().get(command);
					if (handler != null && entry2.getKey().equals(command)) {
						result.put(entry.getKey(), handler);
					}
				}
			}
			return result;
		} finally {
			readLock.unlock();
		}
    }
    
	@SuppressWarnings("unchecked")
	public <T> T searchHandler(Class<T> clazz, Number160 peerID, Number160 peerId2) {
		readLock.lock();
		try {
			final Map<Integer, DispatchHandler> ioHandlers = search(peerID, peerId2);
			if(ioHandlers == null) {
				return null;
			}
			for (DispatchHandler handler : ioHandlers.values()) {
				if (clazz.isInstance(handler)) {
					return (T) handler;
				}
			}
			return null;
		} finally {
			readLock.unlock();
		}
	}
	
	public Map<Integer, DispatchHandler> searchHandler(Number160 peerId, Number160 onBehalfOf) {
		readLock.lock();
		try {
			return new HashMap<Integer, DispatchHandler>(ioHandlers.get(new Number320(peerId, onBehalfOf)));
		} finally {
			readLock.unlock();
		}
	}

    /**
     * 
     * @param peerId
     * 			  The id of the peer the get the dispatcher map
     * @param onBehalfOf
     * 			  The ioHandler can be registered for the own use of in behalf of another peer (e.g. in case of relay node).
     * @return the map containing all dispatchers for each {@link Commands} type
     */
	private Map<Integer, DispatchHandler> search(Number160 peerId, Number160 onBehalfOf) {
		return ioHandlers.get(new Number320(peerId, onBehalfOf));
	}

	public boolean responsibleFor(Number160 peerId) {
		readLock.lock();
    	try {
    		return ioHandlers.containsKey(new Number320(peerId, peerId));
    	} finally {
    		readLock.unlock();
    	}
	}
}
