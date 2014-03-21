package net.tomp2p.relay;

import java.net.InetAddress;

import net.tomp2p.connection.Ports;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.p2p.builder.Builder;
import net.tomp2p.peers.PeerAddress;

public class RelayBuilder implements Builder {

    final private static RelayFuture FUTURE_RELAY_NO_BOOTSTRAP_ADDRESS = new RelayFuture(0).setFailed("No bootrap address has been set");

    private final Peer peer;
    private final RelayRPC relayRPC;

    private BootstrapBuilder bootstrapBuilder;
    private PeerAddress bootstrapAddress;
    private InetAddress bootstrapInetAddress;

    private int port = Ports.DEFAULT_PORT;
    private int peerMapUpdateInterval = 5;
    private int relaySearchInterval = 60;
    private int failedRelayWaitTime = 60;
    private int minRelays = 2;
    private int maxRelays = PeerAddress.MAX_RELAYS;

    public RelayBuilder(Peer peer) {
        this.peer = peer;
        this.relayRPC = RelayRPC.setup(peer);
    }

    /**
     * Defines how many seconds to wait at least until asking a relay that
     * denied a relay request or a relay that failed to act as a relay again
     * 
     * @param failedRelayWaitTime
     *            wait time in seconds
     * @return this instance
     */
    public RelayBuilder failedRelayWaitTime(int failedRelayWaitTime) {
        this.failedRelayWaitTime = failedRelayWaitTime;
        return this;
    }

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
    public RelayBuilder relaySearchInterval(int relaySearchInterval) {
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
    public RelayBuilder minRelays(int minRelays) {
        this.minRelays = minRelays;
        return this;
    }

    /**
     * Sets the maximum number of peers. A peer can currently have up to 5 relay
     * peers (specified in {@link PeerAddress#MAX_RELAYS}). Any number higher
     * than 5 will result in 5 relay peers.
     * 
     * @param maxRelays
     *            maximum number of relay peers (maximum specified in
     *            {@link PeerAddress#MAX_RELAYS}). Currently up to 5 relay peers
     *            are allowed
     * @return this instance
     */
    public RelayBuilder maxRelays(int maxRelays) {
        this.maxRelays = maxRelays;
        return this;
    }

    /**
     * @return maximum number of relays
     */
    public int maxRelays() {
        return maxRelays;
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
    public RelayBuilder peerMapUpdateInterval(int peerMapUpdateInterval) {
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
     * {@link RelayBuilder#bootstrapBuilder(BootstrapBuilder)}
     * 
     * @param bootrapAddress
     *            PeerAddress of any peer in the network.
     * @return this instance
     */
    public RelayBuilder bootstrapAddress(PeerAddress bootrapAddress) {
        this.bootstrapAddress = bootrapAddress;
        return this;
    }

    /**
     * Set a bootstrap address for setting up the relay peers. If ports are not
     * set using {@link RelayBuilder#ports(int)} a default port is used.
     * 
     * @param bootstrapInetAddress
     *            The bootstrap address
     * @return this instance
     */
    public RelayBuilder bootstrapInetAddress(InetAddress bootstrapInetAddress) {
        this.bootstrapInetAddress = bootstrapInetAddress;
        return this;
    }

    /**
     * Sets the ports of the bootstrap peer. For more specific bootstrap
     * configuration use {@link RelayBuilder#bootstrapBuilder(BootstrapBuilder)}
     * 
     * @param port
     *            The port of the bootstrap peer
     * @return this instance
     */
    public RelayBuilder ports(int port) {
        this.port = port;
        return this;
    }

    /**
     * Specify a bootstrap builder that will be used to bootstrap during the
     * process of setting up relay peers and after that.
     * 
     * @param bootstrapBuilder
     *            The bootstrap builder
     * @return this instance
     */
    public RelayBuilder bootstrapBuilder(BootstrapBuilder bootstrapBuilder) {
        this.bootstrapBuilder = bootstrapBuilder;
        return this;
    }

    /**
     * Start setting up the relay peers
     * 
     * @return A RelayFuture
     */
    public RelayFuture start() {

        BootstrapBuilder bootstrapBuilder = null;

        if (bootstrapAddress != null) {
            bootstrapBuilder = peer.bootstrap().setPeerAddress(bootstrapAddress);
        } else if (bootstrapInetAddress != null) {
            bootstrapBuilder = peer.bootstrap().setInetAddress(bootstrapInetAddress).setPorts(port);
        } else if (this.bootstrapBuilder != null) {
            bootstrapBuilder = this.bootstrapBuilder;
        } else {
            return FUTURE_RELAY_NO_BOOTSTRAP_ADDRESS;
        }

        RelayManager relayManager = new RelayManager(peer, relayRPC, bootstrapBuilder, maxRelays, minRelays, peerMapUpdateInterval, relaySearchInterval, failedRelayWaitTime);

        return relayManager.setupRelays();
    }

}
