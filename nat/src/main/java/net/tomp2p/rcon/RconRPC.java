package net.tomp2p.rcon;

import java.lang.reflect.UndeclaredThrowableException;

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;

public class RconRPC extends DispatchHandler {

	private final Peer peer;
	private final ConnectionConfiguration config;
	
	public RconRPC(Peer peer) {
		super(peer.peerBean(), peer.connectionBean());
		register(RPC.Commands.RCON.getNr());
		this.peer = peer;
		this.config = new DefaultConnectionConfiguration();
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
