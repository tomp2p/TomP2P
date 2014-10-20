package net.tomp2p.holep;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;

public class HolePunchRPC extends DispatchHandler {

	private static final Logger LOG = LoggerFactory.getLogger(RendezVousRPC.class);

	private final Peer peer;
	private final ConnectionConfiguration config;

	public HolePunchRPC(Peer peer) {
		super(peer.peerBean(), peer.connectionBean());
		register(RPC.Commands.HOLEP.getNr());
		this.peer = peer;
		this.config = new DefaultConnectionConfiguration();
	}

	@Override
	public void handleResponse(Message message, PeerConnection peerConnection, boolean sign, Responder responder) throws Exception {

	}

}