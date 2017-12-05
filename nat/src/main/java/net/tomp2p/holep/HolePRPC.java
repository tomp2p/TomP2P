package net.tomp2p.holep;

import org.jdeferred.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sctp4nat.core.SctpChannelFacade;
import net.tomp2p.connection.ChannelSender;
import net.tomp2p.connection.Responder;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.AbstractHolePRPC;
import net.tomp2p.p2p.Peer;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;

public class HolePRPC extends AbstractHolePRPC {

	private static final Logger LOG = LoggerFactory.getLogger(HolePRPC.class);
	private final Peer peer;
	
	public HolePRPC(final Peer peer) {
		super(peer.peerBean(), peer.connectionBean());
		register(RPC.Commands.HOLEP.getNr());
		this.peer = peer;
	}

	@Override
	public void handleResponse(Responder responder, Message message, boolean sign,
			Promise<SctpChannelFacade, Exception, Void> p, ChannelSender sender) throws Exception {

		// This means, that a new Holepunch has been initiated.
		if (message.type() == Message.Type.REQUEST_1) {
			LOG.debug("New HolePunch process initiated from peer " + message.sender().peerId() + " to peer " + message.recipient().peerId()
					+ " on ports: " + message.intList().toString());
			forwardHolePunchMessage(message, responder);
		}
		// This means that the initiating peer has sent a holep setup message to
		// this peer
		else if (message.type() == Message.Type.REQUEST_2) {
			LOG.debug("HolePunch initiated on peer: " + message.recipient().peerId());
			handleHolePunch(message, responder);
		} else {
			throw new IllegalArgumentException("Message Content is wrong!");
		}
	}

	private void handleHolePunch(Message message, Responder responder) {
		// TODO Auto-generated method stub
		
	}

	private void forwardHolePunchMessage(Message message, Responder responder) {
		// TODO jwa implement this please 
	}
	
	
}