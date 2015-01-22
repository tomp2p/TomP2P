package net.tomp2p.relay;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.rpc.DispatchHandler;

public abstract class RelayServerConfig {

	protected final Peer peer;

	public RelayServerConfig(Peer peer) {
		this.peer = peer;
	}
	
	/**
	 * Helper method to create a response message
	 */
	protected Message createResponse(Message requestMessage, Type replyType) {
		return DispatchHandler.createResponseMessage(requestMessage, replyType, peer.peerAddress());
	}
	
	/**
	 * Creates a new relay server for the unreachable peer that send the message.
	 * Note that the reply message must be sent by the implementation
	 * 
	 * @return the server or <code>null</code> if something went wrong.
	 */
	public abstract BaseRelayServer createServer(Message message, final PeerConnection peerConnection, Responder responder);
}
