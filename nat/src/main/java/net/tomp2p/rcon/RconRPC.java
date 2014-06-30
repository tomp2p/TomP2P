package net.tomp2p.rcon;

import java.lang.reflect.UndeclaredThrowableException;

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.message.Message;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;

public class RconRPC extends DispatchHandler {

	/**
	 * Constructor that registers this RPC with the message handler.
	 * 
	 * @param peerBean
	 *            The peer bean that contains data that is unique for each peer
	 * @param connectionBean
	 *            The connection bean that is unique per connection (multiple
	 *            peers can share a single connection)
	 */
	public RconRPC(PeerBean peerBean, ConnectionBean connectionBean) {
		super(peerBean, connectionBean);
		register(RPC.Commands.RCON.getNr());
	}

	/**
	 * REQEST_1 = relay rcon forwarding
	 * REQUEST_2 = open socket and transmit PeerConnection
	 * REQUEST_3 = open socket and connect via PeerConnection
	 */
	@Override
	public void handleResponse(Message message, PeerConnection peerConnection, boolean sign, Responder responder)
			throws Exception {
		if (message.type() == Message.Type.REQUEST_1 && message.command() == RPC.Commands.RCON.getNr()) {
			//TODO JWA the message reached the relay node
			
		} else if (message.type() == Message.Type.REQUEST_2 && message.command() == RPC.Commands.RCON.getNr()) {
			//TODO JWA the message reached the unreachable peer
			
		} else if (message.type() == Message.Type.REQUEST_3 && message.command() == RPC.Commands.RCON.getNr()) {
			//TODO JWA the message reached the requesting peer
			
		} else {
			throw new IllegalArgumentException("Message content is wrong");
		}
	}
	
	private void handleForwarding(Message message, Responder responder, PeerConnection peerConnection) {
		
	}
	
	private void handleRconSetup(Message message, Responder responder, PeerConnection peerConnection) {
		
	}
	
	private void sendMessage(Message message, Responder responder, PeerConnection peerConnection) {
		
	}
	
}
