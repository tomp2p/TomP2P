package net.tomp2p.relay;

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

	public BaseRelayConnection(PeerAddress relayAddress) {
		this.relayAddress = relayAddress;
	}
	
	public PeerAddress relayAddress() {
		return relayAddress;
	}
	
	public abstract RelayType relayType();
	
	public abstract FutureResponse sendToRelay(Message message);
	
	public abstract FutureDone<Void> shutdown();
}
