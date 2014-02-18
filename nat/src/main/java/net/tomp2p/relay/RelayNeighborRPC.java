package net.tomp2p.relay;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.rpc.NeighborRPC;
import net.tomp2p.rpc.ObjectDataReply;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelayNeighborRPC extends NeighborRPC {
    
    private final static Logger logger = LoggerFactory.getLogger(RelayRouting.class);
    
    private Peer peer;
    private PeerAddress unreachablePeer;
    private List<Map<Number160, PeerStatatistic>> peerMap = null;

    public RelayNeighborRPC(Peer peer, PeerAddress unreachablePeer) {
        super(peer.getPeerBean(), peer.getConnectionBean());
        peer.getConnectionBean().dispatcher().registerIoHandler(unreachablePeer.getPeerId(), this, NeighborRPC.NEIGHBORS_COMMAND);
        this.peer = peer;
        this.unreachablePeer = unreachablePeer;
        setObjectReply();
    }
    
    @Override
    protected SortedSet<PeerAddress> getNeighbors(Number160 id, int atLeast) {
        if(peerMap == null) {
            return null;
        } else {
            return PeerMap.closePeers(unreachablePeer.getPeerId(), id, NeighborRPC.NEIGHBOR_SIZE, peerMap);
        }
    }
    
    private void setObjectReply() {
        peer.setObjectDataReply(new ObjectDataReply() {
            @SuppressWarnings("unchecked")
            public Object reply(PeerAddress sender, Object request) throws Exception {
                if(request instanceof List<?>) {
                    peerMap = (List<Map<Number160, PeerStatatistic>>) request;
                    logger.trace("Peer map of unreachable peer {} was updated", sender);
                }
                return true;
            }
        });
    }

}
