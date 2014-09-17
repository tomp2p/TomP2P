package net.tomp2p.relay.tcp;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.BaseRelayConnection;
import net.tomp2p.relay.RelayListener;
import net.tomp2p.relay.RelayType;
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
					PeerAddress failedRelay = connection.remotePeer();
					LOG.debug("Relay connection {} failed.", failedRelay);
					
					for (RelayListener relayListener : listeners) {
						relayListener.relayFailed(failedRelay);
					}
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
		return connection.closeFuture();
	}

	/**
	 * Adds a close listener for an open peer connection, so that if the
	 * connection to the relay peer drops, a new relay is found and a new relay
	 * connection is established
	 * 
	 * @param peerConnection
	 *            the peer connection on which to add a close listener
	 * @param bootstrapBuilder
	 *            bootstrap builder, used to find neighbors of this peer
	 */
	@Override
	public void addCloseListener(RelayListener listener) {
		listeners.add(listener);
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
