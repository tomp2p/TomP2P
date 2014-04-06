package net.tomp2p.nat;

import java.net.InetAddress;

import net.tomp2p.connection.Ports;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.peers.PeerAddress;

public class RelayConf {

	private BootstrapBuilder bootstrapBuilder;
	private PeerAddress bootstrapAddress;
	private InetAddress bootstrapInetAddress;

	private int ports = Ports.DEFAULT_PORT;
	private int peerMapUpdateInterval = 5;
	private int relaySearchInterval = 60;
	private int failedRelayWaitTime = 60;
	private int minRelays = 2;

	/**
	 * Defines how many seconds to wait at least until asking a relay that
	 * denied a relay request or a relay that failed to act as a relay again
	 * 
	 * @param failedRelayWaitTime
	 *            wait time in seconds
	 * @return this instance
	 */
	public RelayConf failedRelayWaitTime(int failedRelayWaitTime) {
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
	 * If a relay peer failed but no new possible relay peers could be found,
	 * the peer will actively search for new relay peers. With this method, the
	 * interval in which the peer searches for new relays can be defined.
	 * 
	 * @param relaySearchInterval
	 *            search interval in seconds
	 * @return this instance
	 */
	public RelayConf relaySearchInterval(int relaySearchInterval) {
		this.relaySearchInterval = relaySearchInterval;
		return this;
	}

	/**
	 * @return the interval in seconds
	 */
	public int relaySearchInterval() {
		return relaySearchInterval;
	}

	/**
	 * Defines how many relays have to be set up. If less than minRelays relay
	 * peers could be set up, it is considered a fail.
	 * 
	 * @param minRelays
	 *            minimum amount of relays
	 * @return this instance
	 */
	public RelayConf minRelays(int minRelays) {
		this.minRelays = minRelays;
		return this;
	}

	/**
	 * @return How many relays have to be set up. If less than minRelays relay
	 *         peers could be set up, it is considered a fail.
	 */
	public int minRelays() {
		return minRelays;
	}

	/**
	 * @return Gets the maximum number of peers. A peer can currently have up to
	 *         5 relay peers (specified in {@link PeerAddress#MAX_RELAYS}). Any
	 *         number higher than 5 will result in 5 relay peers.
	 */
	public int maxRelays() {
		return PeerAddress.MAX_RELAYS;
	}

	/**
	 * Defines the time interval of sending the peer map of the unreachable peer
	 * to its relays. The routing requests are not relayed to the unreachable
	 * peer but handled by the relay peers. Therefore, the relay peers should
	 * always have an up-to-date peer map of the relayed peer
	 * 
	 * @param peerMapUpdateInterval
	 *            interval of updates in seconds
	 * @return this instance
	 */
	public RelayConf peerMapUpdateInterval(int peerMapUpdateInterval) {
		this.peerMapUpdateInterval = peerMapUpdateInterval;
		return this;
	}

	/**
	 * @return the peer map update interval in seconds
	 */
	public int peerMapUpdateInterval() {
		return peerMapUpdateInterval;
	}

	/**
	 * Sets the bootstrap address. For more specific bootstrap configuration use
	 * {@link RelayConf#bootstrapBuilder(BootstrapBuilder)}
	 * 
	 * @param bootrapAddress
	 *            PeerAddress of any peer in the network.
	 * @return this instance
	 */
	public RelayConf bootstrapAddress(PeerAddress bootrapAddress) {
		this.bootstrapAddress = bootrapAddress;
		return this;
	}

	/**
	 * @return bootstrap address. For more specific bootstrap configuration use
	 *         {@link RelayConf#bootstrapBuilder(BootstrapBuilder)}
	 */
	public PeerAddress bootstrapAddress() {
		return bootstrapAddress;
	}

	/**
	 * Set a bootstrap address for setting up the relay peers. If ports are not
	 * set using {@link RelayConf#ports(int)} a default port is used.
	 * 
	 * @param bootstrapInetAddress
	 *            The bootstrap address
	 * @return this instance
	 */
	public RelayConf bootstrapInetAddress(InetAddress bootstrapInetAddress) {
		this.bootstrapInetAddress = bootstrapInetAddress;
		return this;
	}

	/**
	 * @return Get a bootstrap address for setting up the relay peers. If ports
	 *         are not set using {@link RelayConf#ports(int)} a default port
	 *         is used.
	 */
	public InetAddress bootstrapInetAddress() {
		return bootstrapInetAddress;
	}

	/**
	 * Sets the ports of the bootstrap peer. For more specific bootstrap
	 * configuration use {@link RelayConf#bootstrapBuilder(BootstrapBuilder)}
	 * 
	 * @param port
	 *            The port of the bootstrap peer
	 * @return this instance
	 */
	public RelayConf ports(int ports) {
		this.ports = ports;
		return this;
	}

	/**
	 * @return Gets the ports of the bootstrap peer. For more specific bootstrap
	 *         configuration use
	 *         {@link RelayConf#bootstrapBuilder(BootstrapBuilder)}
	 */
	public int ports() {
		return ports;
	}

	/**
	 * Specify a bootstrap builder that will be used to bootstrap during the
	 * process of setting up relay peers and after that.
	 * 
	 * @param bootstrapBuilder
	 *            The bootstrap builder
	 * @return this instance
	 */
	public RelayConf bootstrapBuilder(BootstrapBuilder bootstrapBuilder) {
		this.bootstrapBuilder = bootstrapBuilder;
		return this;
	}

	/**
	 * @return Get a bootstrap builder that will be used to bootstrap during the
	 *         process of setting up relay peers and after that.
	 */
	public BootstrapBuilder bootstrapBuilder() {
		return bootstrapBuilder;
	}
}
