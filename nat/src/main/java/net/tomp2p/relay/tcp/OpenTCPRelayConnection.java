package net.tomp2p.relay.tcp;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.relay.BaseRelayConnection;
import net.tomp2p.relay.RelayUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenTCPRelayConnection extends BaseRelayConnection {

	private final static Logger LOG = LoggerFactory.getLogger(OpenTCPRelayConnection.class);

	private final PeerConnection connection;
	private final Peer peer;
	private final ConnectionConfiguration config;

	public OpenTCPRelayConnection(PeerConnection connection, Peer peer, ConnectionConfiguration config) {
		super(connection.remotePeer());
		this.connection = connection;
		this.peer = peer;
		this.config = config;
		
		initCloseListener();
	}
	
	private void initCloseListener() {
		connection.closeFuture().addListener(new BaseFutureAdapter<FutureDone<Void>>() {
			public void operationComplete(FutureDone<Void> future) throws Exception {
				if (!peer.isShutdown()) {
					// peer connection not open anymore -> remove and open a new  relay connection
					LOG.debug("Relay connection {} failed.", relayAddress());
					notifyCloseListeners();
				}
			}
		});
	}

	@Override
	public FutureResponse sendToRelay(Message message) {
		message.keepAlive(true);
		return RelayUtils.send(connection, peer.peerBean(), peer.connectionBean(), config, message);
	}

	@Override
	public FutureDone<Void> shutdown() {
		return connection.closeFuture().done();
	}

	@Override
	public void onMapUpdateFailed() {
		// ignore because we already have a close listener on the TCP connection
	}

	@Override
	public void onMapUpdateSuccess() {
		// success is nice, but we only care about failures
	}
}
