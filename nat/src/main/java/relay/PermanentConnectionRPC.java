package relay;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
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
    public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign, Responder responder) throws Exception {

    	if(message.getSender().equals(unreachablePeer) && futurePeerConnection == null) {
    		futurePeerConnection = new FuturePeerConnection(message.getSender());
            futurePeerConnection.setDone(peerConnection);
            new RelayForwarder(futurePeerConnection, peer);
    	}
        super.handleResponse(message, peerConnection, sign, responder);
    }

}
