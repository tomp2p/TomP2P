package relay;

import java.util.HashMap;
import java.util.Map;

import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;

public class RelayTable {
	
	private Map<PeerAddress, FuturePeerConnection> peerConnections;
	private Peer peer;
	
	public RelayTable(Peer peer) {
		peerConnections = new HashMap<PeerAddress, FuturePeerConnection>();
		this.peer = peer;
	}
	
	public void forwardMessage(PeerAddress pa, Message msg) {
		peer.sendDirect(peerConnections.get(pa)).setObject(msg).start();
	}
	
	public void addUnreachablePeer(PeerAddress pa, FuturePeerConnection fpc) {
		peerConnections.put(pa, fpc);
		//new RelayForwarder(peer.getConnectionBean(), peer.getPeerBean(), fpc, pa);
	}

	
}
