package net.tomp2p.relay;

import java.net.InetAddress;

import net.tomp2p.connection.Ports;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.p2p.builder.Builder;
import net.tomp2p.peers.PeerAddress;

public class RelayBuilder implements Builder {
    
	final private static RelayFuture FUTURE_RELAY_NO_BOOTSTRAP_ADDRESS= new RelayFuture().setFailed("No bootrap address has been set");
    
    final private Peer peer;
    
    private PeerAddress bootstrapAddress;
    private InetAddress bootstrapInetAddress;
    private int port = Ports.DEFAULT_PORT;
    private BootstrapBuilder bootstrapBuilder;
    private int maxRelays = PeerAddress.MAX_RELAYS;
    private RelayManager relayManager;
    private RelayRPC relayRPC;
    
    public RelayBuilder(RelayPeer relayPeer) {
        this.peer = relayPeer.peer();
        this.relayRPC = relayPeer.relayRPC();
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
        
        if(bootstrapAddress != null) {
            bootstrapBuilder = peer.bootstrap().setPeerAddress(bootstrapAddress);
        } else if(bootstrapInetAddress != null) {
            bootstrapBuilder = peer.bootstrap().setInetAddress(bootstrapInetAddress).setPorts(port);
        } else if(this.bootstrapBuilder != null) {
            bootstrapBuilder = this.bootstrapBuilder;
        } else {
            return FUTURE_RELAY_NO_BOOTSTRAP_ADDRESS;
        }
        
        //TODO: can we create the releaymananger in the constructor? can we reuse it?
        relayManager = new RelayManager(peer, bootstrapBuilder, maxRelays, relayRPC);
        return relayManager.setupRelays();
    }

}
