package net.tomp2p.relay;

import java.util.Collection;
import java.util.Collections;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;

/**
 * Holds multiple relay types with their configuration
 * 
 * @author Nico Rutishauser
 *
 */
public abstract class RelayClientConfig {

	private final RelayType type;

	// configurable
	private int peerMapUpdateInterval;
	private Collection<PeerAddress> manualRelays;
	private int failedRelayWaitTime;
	private int maxFail;

	protected RelayClientConfig(RelayType type, int peerMapUpdateInterval, int failedRelayWaitTime, int maxFail) {
		this.type = type;
		this.peerMapUpdateInterval = peerMapUpdateInterval;
		this.failedRelayWaitTime = failedRelayWaitTime;
		this.maxFail = maxFail;

		this.manualRelays = Collections.emptyList();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(type.toString());
		sb.append("[Interval:").append(peerMapUpdateInterval).append("s").append("]");
		return sb.toString();
	}

	/**
	 * @return the relay type
	 */
	public RelayType type() {
		return type;
	}

	/**
	 * Get the peer map update interval
	 */
	public int peerMapUpdateInterval() {
		return peerMapUpdateInterval;
	}

	/**
	 * Defines the time interval of sending the peer map of the unreachable peer
	 * to its relays. The routing requests are not relayed to the unreachable
	 * peer but handled by the relay peers. Therefore, the relay peers should
	 * always have an up-to-date peer map of the relayed peer
	 * 
	 * @param peerMapUpdateInterval the interval of updating the own peer map at the relay
	 * @return this instance
	 */
	public RelayClientConfig peerMapUpdateInterval(int peerMapUpdateInterval) {
		this.peerMapUpdateInterval = peerMapUpdateInterval;
		return this;
	}

	/**
	 * Add a relay to the relays list
	 */
	public void addManualRelay(PeerAddress manualRelay) {
		synchronized (manualRelays) {
			manualRelays.add(manualRelay);
		}
	}

	/**
	 * Set the relay list where the peer should connect to
	 * 
	 * @param manualRelays publicly reachable relay nodes
	 * @return this instance
	 */
	public RelayClientConfig manualRelays(Collection<PeerAddress> manualRelays) {
		if (manualRelays == null) {
			this.manualRelays = Collections.emptySet();
		} else {
			this.manualRelays = manualRelays;
		}
		return this;
	}

	/**
	 * @return the currently configured list of relay peers
	 */
	public Collection<PeerAddress> manualRelays() {
		return manualRelays;
	}

	/**
	 * Defines how many seconds to wait at least until asking a relay that
	 * denied a relay request or a relay that failed to act as a relay again
	 * 
	 * @param failedRelayWaitTime
	 *            wait time in seconds
	 * @return this instance
	 */
	public RelayClientConfig failedRelayWaitTime(int failedRelayWaitTime) {
		if (failedRelayWaitTime < 0) {
			throw new IllegalArgumentException("Negative wait time is not allowed");
		}
		this.failedRelayWaitTime = failedRelayWaitTime;
		return this;
	}

	/**
	 * @return How many seconds to wait at least until asking a relay that
	 *         denied a relay request or a relay that failed to act as a relay
	 *         again
	 */
	public int failedRelayWaitTime() {
		return failedRelayWaitTime;
	}

	/**
	 * Defines how many times a setup with a relay can fail before it's ignored
	 * 
	 * @param maxFail the allowed number of fails
	 * @return this instance
	 */
	public RelayClientConfig maxFail(int maxFail) {
		if (maxFail < 0) {
			throw new IllegalArgumentException("Negative maximum fail count is not allowed");
		}
		this.maxFail = maxFail;
		return this;
	}

	/**
	 * @return the maximum number of allowed fails
	 */
	public int maxFail() {
		return maxFail;
	}
	
	/**
	 * Creates a client object
	 */
	public abstract BaseRelayClient createClient(PeerConnection connection, Peer peer);
	
	/**
	 * Gives the opportunity to add more data to the setup message which is sent from the unreachable peer to
	 * the relay peer.
	 * 
	 * @param message the message that is sent to the relay peer.
	 */
	public abstract void prepareSetupMessage(Message message);

	/**
	 * Gives the opportunity to add more data to the map update message.
	 * 
	 * @param message the message which is regularly sent to the relay peer to update the routing table.
	 */
	public abstract void prepareMapUpdateMessage(Message message);
}
