package net.tomp2p.relay;

import java.net.InetSocketAddress;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DispatchHandler;

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
public class RelayForwarder extends DispatchHandler {

	private final static Logger logger = LoggerFactory.getLogger(RelayForwarder.class);

	private final Peer peer;

	// connection to unreachable peer
	private final FuturePeerConnection futurePeerConnection;

	/**
     * 
     * @param fps
     *            A peer connection to an unreachable peer that is permanently
     *            open
     * @param peer
     *            The relay peer
     */
	private RelayForwarder(PeerConnection peerConnection, Peer peer) {
		super(peer.getPeerBean(), peer.getConnectionBean());
		PeerAddress unreachablePeer = peerConnection.remotePeer();
		
		//TODO: use enum iteration
		peer.getConnectionBean().dispatcher().registerIoHandler(unreachablePeer.getPeerId(), this, 0, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14);
		
		this.futurePeerConnection = new FuturePeerConnection(peerConnection.remotePeer());
		futurePeerConnection.setDone(peerConnection);
		
		this.peer = peer;
		new RelayNeighborRPC(peer, unreachablePeer);
		//TODO: RelayPing
		logger.debug("created forwarder from peer " + peer.getPeerAddress() + " to peer " + peerConnection.remotePeer());
	}
	
	public static RelayForwarder setup(PeerConnection peerConnection, Peer peer) {
	    return new RelayForwarder(peerConnection, peer);
	}

	@Override
	public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign, final Responder responder) throws Exception {
		logger.debug("Received message {} to forward to unreachable peer {}", message, futurePeerConnection.getObject().remotePeer());

		// Send message via direct message through the open connection to the unreachable peer
		message.restoreContentReferences();
		final Buffer buf = RelayUtils.encodeMessage(message);

		FutureDirect fd = peer.sendDirect(futurePeerConnection).setBuffer(buf).start();
		
		fd.addListener(new BaseFutureAdapter<FutureDirect>() {
			public void operationComplete(FutureDirect future) throws Exception {
				if (future.isSuccess()) {
					// send response
					Message response = RelayUtils.decodeMessage(future.getBuffer(), new InetSocketAddress(0), message.senderSocket()); // sender
					response.restoreContentReferences();
					logger.debug("response from unreachable peer: " + response);
					responder.response(response);
				} else {
					responder.failed(Type.USER1, "Relaying message failed: " + future.getFailedReason());
				}
			}
		});
	}
}
