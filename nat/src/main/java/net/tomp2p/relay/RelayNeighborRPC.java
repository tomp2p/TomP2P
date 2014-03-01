package net.tomp2p.relay;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.NeighborRPC;
import net.tomp2p.rpc.RPC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelayNeighborRPC extends NeighborRPC {
    
    private final static Logger logger = LoggerFactory.getLogger(RelayNeighborRPC.class);
    
    private PeerAddress unreachablePeer;
    private List<Map<Number160, PeerStatatistic>> peerMap = null;

    public RelayNeighborRPC(Peer peer, PeerAddress unreachablePeer) {
        super(peer.getPeerBean(), peer.getConnectionBean(), false);
        peer.getConnectionBean().dispatcher().registerIoHandler(unreachablePeer.getPeerId(), this, RPC.Commands.NEIGHBOR.getNr());
        this.unreachablePeer = unreachablePeer;
    }
    
    @Override
    public Message createResponseMessage(Message requestMessage, Type replyType) {
    	//we need to create a recipient that has the IP/port of the relay peer, but the ID of the unreachable peer
        return DispatchHandler.createResponseMessage(requestMessage, 
        		replyType, peerBean().serverPeerAddress().changePeerId(unreachablePeer.getPeerId()));
    }
    
    @Override
    protected SortedSet<PeerAddress> getNeighbors(Number160 id, int atLeast) {
        logger.trace("Answering routing request on behalf of unreachable peer {}, neighbors of {}", unreachablePeer, id);
        if(peerMap == null) {
            return null;
        } else {
            return PeerMap.closePeers(unreachablePeer.getPeerId(), id, NeighborRPC.NEIGHBOR_SIZE, peerMap);
        }
    }

	public Collection<PeerAddress> getAll() {
		Collection<PeerStatatistic> result1 = new ArrayList<PeerStatatistic>();
		for(Map<Number160, PeerStatatistic> map:peerMap) {
			result1.addAll(map.values());
		}
		Collection<PeerAddress> result2 = new ArrayList<PeerAddress>();
	    for(PeerStatatistic peerStatatistic:result1) {
	    	result2.add(peerStatatistic.getPeerAddress());
	    }
	    return result2;
    }

	public void setMap(List<Map<Number160, PeerStatatistic>> peerMap) {
	    this.peerMap = peerMap;
    }
}
