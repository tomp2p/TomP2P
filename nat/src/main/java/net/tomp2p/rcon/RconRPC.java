package net.tomp2p.rcon;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.Dispatcher;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.relay.RelayForwarderRPC;
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
			handleRconSetup(message, responder);
			
		} else if (message.type() == Message.Type.REQUEST_2 && message.command() == RPC.Commands.RCON.getNr()) {
			//TODO JWA the message reached the unreachable peer
			
		} else if (message.type() == Message.Type.REQUEST_3 && message.command() == RPC.Commands.RCON.getNr()) {
			//TODO JWA the message reached the requesting peer
			
		} else {
			throw new IllegalArgumentException("Message content is wrong");
		}
	}

	private void handleRconSetup(Message message, Responder responder) {
		//TODO JWA the message reached the relay node
		Message forwardMessage = new Message();
		forwardMessage.type(Message.Type.REQUEST_2);
		forwardMessage.command(RPC.Commands.RCON.getNr());
		
		NeighborSet ns= new NeighborSet(1);
		ns.add(message.sender());
		forwardMessage.neighborsSet(ns);
		
//		peer.connectionBean().dispatcher().searchHandler(RPC.Commands.RELAY.ordinal());
//		DispatchHandler handler = peer.connectionBean().dispatcher().searchHandler(message.recipient().peerId(), RPC.Commands.RELAY.getNr());
		Dispatcher dispatcher = peer.connectionBean().dispatcher();
		Map<Integer, DispatchHandler> ioHandlers = dispatcher.searchHandlerMap(message.recipient().peerId());
		for (Map.Entry<Integer, DispatchHandler> element : ioHandlers.entrySet()) {
			if (element.getValue().getClass().equals(RelayForwarderRPC.class)) {
				System.out.println(element.getValue().toString());
			}
		}
		
		
//		RelayForwarderRPC relayForwarderRPC = (RelayForwarderRPC) handler;
//		PeerConnection peerConnection2 = relayForwarderRPC.peerConnection();
//		
//		//TODO jwa use random token
//		forwardMessage.longValue(345243);
//		
//		FutureResponse futureResponse = new FutureResponse(forwardMessage);
//		RelayUtils.sendSingle(peerConnection2, futureResponse, peer.peerBean(), peer.connectionBean(), config);
		
		responder.response(createResponseMessage(message, Type.OK));
	}
}
