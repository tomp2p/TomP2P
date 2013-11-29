package relay;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DirectDataRPC;

public class PermanentConnectionRPC extends DirectDataRPC {
	
	private FuturePeerConnection futurePeerConnection = null;
	private final Peer peer;
	private final PeerAddress unreachablePeer;
	

	public PermanentConnectionRPC(Peer peer, PeerAddress unreachablePeer) {
		super(peer.getPeerBean(), peer.getConnectionBean());
		this.peer = peer;
		this.unreachablePeer = unreachablePeer;
	}
	
    @Override
    public Message handleResponse(Message message, PeerConnection peerConnection, boolean sign)
            throws Exception {
    	System.out.println("direct message content: " + message.getBuffer(0).object().toString());
    	if(message.getSender().equals(unreachablePeer) && futurePeerConnection == null) {
    		futurePeerConnection = new FuturePeerConnection(message.getSender());
            futurePeerConnection.setDone(peerConnection);
            new RelayForwarder(futurePeerConnection, peer);
    	}
        return super.handleResponse(message, peerConnection, sign);
    }

}
