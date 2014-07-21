package net.tomp2p.rcon;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;

public class RconRPC extends DispatchHandler {

	private final Peer peer;
	private final ConnectionConfiguration config;
	private static final Logger LOG = LoggerFactory.getLogger(RconRPC.class);
	
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
		LOG.debug("received RPC message {}", message);
		if (message.type() == Message.Type.REQUEST_1 && message.command() == RPC.Commands.RCON.getNr()) {
			//TODO JWA the message reached the relay node
			
			
			message.type(Message.Type.REQUEST_2);
			
			responder.response(createResponseMessage(message, Type.OK));
			
		} else if (message.type() == Message.Type.REQUEST_2 && message.command() == RPC.Commands.RCON.getNr()) {
			//TODO JWA the message reached the unreachable peer
			
		} else if (message.type() == Message.Type.REQUEST_3 && message.command() == RPC.Commands.RCON.getNr()) {
			//TODO JWA the message reached the requesting peer
			
		} else {
			throw new IllegalArgumentException("Message content is wrong");
		}
	}
}
