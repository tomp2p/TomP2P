package relay;

import net.tomp2p.connection2.ConnectionBean;
import net.tomp2p.connection2.PeerBean;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.builder.SendDirectBuilder;
import net.tomp2p.rpc.DispatchHandler;

public class RelayForwarder extends DispatchHandler {
	
	private FuturePeerConnection fps;
	private final Peer peer;
	
	public RelayForwarder(PeerBean peerBean, ConnectionBean connectionBean, FuturePeerConnection fps, Peer peer) {
		super(peerBean, connectionBean, 0,1,2,3,4,5,6,7,8,9,10,11,12);	
		this.fps = fps;
		this.peer = peer;
	}

	@Override
	public Message handleResponse(Message message, boolean sign) throws Exception {
		peer.sendDirect(fps).setObject(message).start();
		return message;
	}

}
