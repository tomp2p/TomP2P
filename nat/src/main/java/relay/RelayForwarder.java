package relay;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.connection.RequestHandler;
import net.tomp2p.connection.Dispatcher.Responder;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.TomP2POutbound;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.DirectDataRPC;

public class RelayForwarder extends DirectDataRPC {

	private final FuturePeerConnection futurePeerConnection; // connection to unreachable peer
	private final Peer peer;
	private ConnectionConfiguration config;


	public RelayForwarder(FuturePeerConnection fps, Peer peer) {
		super(peer.getPeerBean(), peer.getConnectionBean());
		peer.getConnectionBean().dispatcher().registerIoHandler(fps.getObject().remotePeer().getPeerId(), this, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
		System.out.println("created forwarder from peer " + peer.getPeerAddress() + " to peer " + fps.getObject().remotePeer());
		this.futurePeerConnection = fps;
		this.peer = peer;
		this.config = new DefaultConnectionConfiguration();
	}

	@Override
    public void handleResponse(final Message message, PeerConnection peerConnection, final boolean sign, final Responder responder) throws Exception {
		
		FutureResponse futureResponse = new FutureResponse(message);
		final RequestHandler<FutureResponse> requestHandler = new RequestHandler<FutureResponse>(futureResponse, peerBean(), connectionBean(), config);
		FutureResponse fr = requestHandler.sendTCP(futurePeerConnection.peerConnection());
		
		fr.addListener(new BaseFutureListener<FutureResponse>() {
			@Override
			public void operationComplete(FutureResponse future) throws Exception {
				responder.response(future.getResponse());
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				//TODO
			}
		});
	}
}
