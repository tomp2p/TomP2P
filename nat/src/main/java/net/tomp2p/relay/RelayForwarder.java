package net.tomp2p.relay;

import java.net.InetSocketAddress;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.Responder;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DirectDataRPC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelayForwarder extends DirectDataRPC {

	private final static Logger logger = LoggerFactory.getLogger(RelayForwarder.class);
	private final FuturePeerConnection futurePeerConnection; // connection to
																// unreachable
																// peer
	private final Peer peer;

	public RelayForwarder(FuturePeerConnection fps, Peer peer) {
		super(peer.getPeerBean(), peer.getConnectionBean());
		peer.getConnectionBean().dispatcher().registerIoHandler(fps.getObject().remotePeer().getPeerId(), this, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
		logger.debug("created forwarder from peer " + peer.getPeerAddress() + " to peer " + fps.getObject().remotePeer());
		this.futurePeerConnection = fps;
		this.peer = peer;
	}

	@Override
	public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign, final Responder responder) throws Exception {
		logger.debug("Received message {} to forward to unreachable peer {}", message, futurePeerConnection.getObject().remotePeer());
		
		// Send message via direct message through the open connection to the unreachable peer
		final boolean udp = message.isUdp();
		
		Buffer buf = RelayUtils.encodeMessage(message);
		FutureDirect fd = peer.sendDirect(futurePeerConnection).setBuffer(buf).start();
		
		fd.addListener(new BaseFutureListener<FutureDirect>() {
			@Override
			public void operationComplete(FutureDirect future) throws Exception {
				if(future.isSuccess()) {
					//send response
					Message response = RelayUtils.decodeMessage(future.getBuffer(), new InetSocketAddress(0), message.senderSocket()); //sender socket irrelevant
					response.udp(udp);
					logger.debug("response from unreachable peer: " + response);
					responder.response(response);
				} else {
					responder.failed(Type.EXCEPTION, "Relaying message failed: " + future.getFailedReason());
				}
			}
			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				t.printStackTrace();
			}
		});
	}
}
