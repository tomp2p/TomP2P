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
    
    
    public RelayBuilder(Peer peer) {
        this.peer = peer;
    }
    
    public RelayBuilder bootstrapAddress(PeerAddress bootrapAddress) {
        this.bootstrapAddress = bootrapAddress;
        return this;
    }
    
    public RelayBuilder bootstrapInetAddress(InetAddress bootstrapInetAddress) {
        this.bootstrapInetAddress = bootstrapInetAddress;
        return this;
    }
    
    public RelayBuilder maxRelays(int maxRelays) {
        this.maxRelays = maxRelays;
        return this;
    }
    
    public RelayBuilder ports(int port) {
        this.port = port;
        return this;
    }
    
    public RelayBuilder bootstrapBuilder(BootstrapBuilder bootstrapBuilder) {
        this.bootstrapBuilder = bootstrapBuilder;
        return this;
    }
    
    public RelayManager relayManager() {
    	return relayManager;
    }
    
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
        relayManager = new RelayManager(peer, bootstrapBuilder, maxRelays);
        return relayManager.setupRelays();
    }

}
