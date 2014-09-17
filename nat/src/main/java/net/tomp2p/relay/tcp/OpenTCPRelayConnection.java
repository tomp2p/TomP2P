package net.tomp2p.relay.tcp;

import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.relay.BaseRelayConnection;
import net.tomp2p.relay.RelayType;
import net.tomp2p.relay.RelayUtils;

public class OpenTCPRelayConnection extends BaseRelayConnection {

	private final PeerConnection connection;
	private final PeerBean peerBean;
	private final ConnectionBean connectionBean;
	private final ConnectionConfiguration config;

	public OpenTCPRelayConnection(PeerConnection connection, PeerBean peerBean, ConnectionBean connectionBean, ConnectionConfiguration config) {
		super(connection.remotePeer());
		this.connection = connection;
		this.peerBean = peerBean;
		this.connectionBean = connectionBean;
		this.config = config;
	}
	
	@Override
	public FutureResponse sendToRelay(Message message) {
		message.keepAlive(true);
		return RelayUtils.send(connection, peerBean, connectionBean, config, message);
	}

	@Override
	public FutureDone<Void> shutdown() {
		return connection.closeFuture();
	}

	@Override
	public RelayType relayType() {
		return RelayType.OPENTCP;
	}


}
