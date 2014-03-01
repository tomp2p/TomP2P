package net.tomp2p.relay;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The RelayForwarder is responsible for forwarding all messages that are
 * received on a relay peer, but are intended for an unreachable peer that is
 * connected to the relay peer.
 * 
 * @author Raphael Voellmy
 * 
 */
public class RelayForwarderRPC extends DispatchHandler {

	private final static Logger LOG = LoggerFactory.getLogger(RelayForwarderRPC.class);

	// connection to unreachable peer
	private final PeerConnection peerConnection;
	
	private final RelayRPC relayRPC;

	/**
	 * 
	 * @param peerConnection
	 *            A peer connection to an unreachable peer that is permanently
	 *            open
	 * @param peer
	 *            The relay peer
	 */
	public RelayForwarderRPC(PeerConnection peerConnection, Peer peer, RelayRPC relayRPC) {
		super(peer.getPeerBean(), peer.getConnectionBean());
		PeerAddress unreachablePeer = peerConnection.remotePeer();
		this.peerConnection = peerConnection;
		this.relayRPC = relayRPC;
		// TODO: use enum iteration
		peer.getConnectionBean().dispatcher()
		        .registerIoHandler(unreachablePeer.getPeerId(), this, 0, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14);
		LOG.debug("created forwarder from peer {} to peer {}", peer.getPeerAddress(), peerConnection.remotePeer());
	}

	@Override
	public void handleResponse(final Message message, PeerConnection peerConnectionUnused, final boolean sign,
	        final Responder responder) throws Exception {
		// PeerConnection will not be used from the arguments, as this
		// connection is from the sender to the relay, not from the unreachable
		// peer
		LOG.debug("Received message {} to forward to unreachable peer {}", message, peerConnection.remotePeer());

		//the sender should have the ip/port from the releay peer, the peerId from the unreachabel peer
		final PeerAddress sender = peerBean().serverPeerAddress().changePeerId(peerConnection.remotePeer().getPeerId());

		//since we know if the unreachabel peer is online, we can answer directly
		if (message.getCommand() == RPC.Commands.PING.getNr()) {
			Message response = createResponseMessage(message, peerConnection.isOpen() ? Type.OK : Type.EXCEPTION,
			        sender);
			responder.response(response);
			return;
		}

		// Send message via direct message through the open connection to the
		// unreachable peer
		message.restoreContentReferences();
		final Buffer buf = RelayUtils.encodeMessage(message);
		
		FutureResponse fr = relayRPC.forwardMessage(peerConnection, buf);
		
		fr.addListener(new BaseFutureAdapter<FutureResponse>() {
			public void operationComplete(FutureResponse future) throws Exception {
				if (future.isSuccess()) {
					Buffer buffer = future.getResponse().getBuffer(0);
					Message responseFromUnreachablePeer = RelayUtils.decodeMessage(buffer,
					        message.recipientSocket(), message.senderSocket());
					responseFromUnreachablePeer.restoreContentReferences();
					responseFromUnreachablePeer.setSender(sender);
					responseFromUnreachablePeer.setRecipient(message.getSender());
					LOG.debug("response from unreachable peer: {}", responseFromUnreachablePeer);
					responder.response(responseFromUnreachablePeer);
				} else {
					responder.failed(Type.USER1, "Relaying message failed: " + future.getFailedReason());
				}
			}
		});
	}
}
