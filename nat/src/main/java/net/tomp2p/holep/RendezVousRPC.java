package net.tomp2p.holep;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Ports;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RendezVousRPC extends DispatchHandler {
	private static final Logger LOG = LoggerFactory.getLogger(RendezVousRPC.class);

	private final Peer peer;
	private final ConnectionConfiguration config;
	
	private Ports initPeerPorts;
	private Ports receivingPeerPorts;

	// TODO jwa list this in PeerNAT so that it is available to the rendezvous
	// server
	public RendezVousRPC(final Peer peer) {
		super(peer.peerBean(), peer.connectionBean());
		register(RPC.Commands.RZVS.getNr());
		this.peer = peer;
		this.config = new DefaultConnectionConfiguration();
	}

	@Override
	public void handleResponse(Message message, PeerConnection peerConnection, boolean sign, Responder responder) throws Exception {
		// This means, that a new Holepunch has been initiated.
		if (message.type() == Message.Type.REQUEST_1) {
			handleHolePSetup(message, peerConnection, responder);
		} 
		
		// This means that peer1 has answered
		else if (message.type() == Message.Type.REQUEST_2) {
			
		} 
		// This means that peer2 has answered
		else if (message.type() == Message.Type.REQUEST_3) {

		} else {
			throw new IllegalArgumentException("Message Content is wrong!");
		}
	}

	private void handleHolePSetup(Message message, PeerConnection peerConnection, Responder responder) {
		Message holePMessage = new Message();
		
		holePMessage.messageId(message.messageId());
		holePMessage.version(message.version());
		holePMessage.sender(message.sender());
		
		FutureResponse fr = new FutureResponse(holePMessage);
		
		System.out.println("SUCCESS! Here's the Message {}");
	}
}