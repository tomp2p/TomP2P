package net.tomp2p.relay;

import java.util.HashSet;
import java.util.Set;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.peers.PeerAddress;

/**
 * Every firewalled peer has one or multiple relay servers. This class represents one of these relays (at the
 * client side).
 * 
 * @author Nico Rutishauser
 *
 */
public abstract class BaseRelayConnection {

	private final PeerAddress relayAddress;
	protected final Set<RelayListener> listeners;

	public BaseRelayConnection(PeerAddress relayAddress) {
		this.relayAddress = relayAddress;
		this.listeners = new HashSet<RelayListener>();
	}
	
	public PeerAddress relayAddress() {
		return relayAddress;
	}
	
	public abstract RelayType relayType();
	
	public abstract FutureResponse sendToRelay(Message message);
	
	public abstract FutureDone<Void> shutdown();
	
	/**
	 * Adds a close listener for an open peer connection, so that if the
	 * connection to the relay peer drops, a new relay is found and a new relay
	 * connection is established
	 * 
	 * @param peerConnection
	 *            the peer connection on which to add a close listener
	 * @param bootstrapBuilder
	 *            bootstrap builder, used to find neighbors of this peer
	 */
	public void addCloseListener(RelayListener listener) {
		listeners.add(listener);
	}
}
