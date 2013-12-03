package relay;

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.Dispatcher.Responder;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.message.Message;
import net.tomp2p.rpc.DispatchHandler;

public class RelayResponseRPC extends DispatchHandler {
	
    public static final byte RELAY_RESPONSE_RPC = 78;

	public RelayResponseRPC(PeerBean peerBean, ConnectionBean connectionBean) {
		super(peerBean, connectionBean, RELAY_RESPONSE_RPC);
	}

	@Override
	public void handleResponse(Message message, PeerConnection peerConnection, boolean sign, Responder responder) throws Exception {
		connectionBean().dispatcher().getAssociatedHandler(message).handleResponse(message, peerConnection, sign, responder);
	}

}
