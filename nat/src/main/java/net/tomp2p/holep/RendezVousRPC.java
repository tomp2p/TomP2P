package net.tomp2p.holep;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Ports;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
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
		this.config = new ConnectionConfiguration() {
			
			@Override
			public boolean isForceUDP() {
				return true;
			}
			
			@Override
			public boolean isForceTCP() {
				return false;
			}
			
			@Override
			public int idleUDPSeconds() {
				return 60;
			}
			
			@Override
			public int idleTCPSeconds() {
				return -1;
			}
			
			@Override
			public int connectionTimeoutTCPMillis() {
				return -1;
			}
		};
	}

	@Override
	public void handleResponse(Message message, PeerConnection peerConnection, boolean sign, Responder responder) throws Exception {
		// This means, that a new Holepunch has been initiated.
		if (message.type() == Message.Type.REQUEST_1) {
			punchHoles(message, peerConnection, sign, responder);
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

	private void punchHoles(Message message, PeerConnection peerConnection, boolean sign, Responder responder) {
		Message initHolePunchMessage = createInitHolePunchMessage();
		
		FutureResponse futureResponse = new FutureResponse(new Message());
		FutureResponse fr = RelayUtils.sendSingle(peerConnection, futureResponse, peer.peerBean(), peer.connectionBean(), config);
		fr.addListener(new BaseFutureAdapter<FutureResponse>() {

			@Override
			public void operationComplete(FutureResponse future) throws Exception {
				
			}
		});
	}

	private Message createInitHolePunchMessage() {
		// TODO Auto-generated method stub
		return null;
	}

}