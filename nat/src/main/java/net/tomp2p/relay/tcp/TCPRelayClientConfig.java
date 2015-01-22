package net.tomp2p.relay.tcp;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.relay.BaseRelayClient;
import net.tomp2p.relay.RelayClientConfig;
import net.tomp2p.relay.RelayType;

public class TCPRelayClientConfig extends RelayClientConfig {

	/**
	 * Creates a TCP relay configuration
	 */
	public TCPRelayClientConfig() {
		super(RelayType.OPENTCP, 15, 60, 2);
	}

	@Override
	public void prepareSetupMessage(Message message) {
		// no need to add anything
	}

	@Override
	public void prepareMapUpdateMessage(Message message) {
		// nothing to append
	}

	@Override
	public BaseRelayClient createClient(PeerConnection connection, Peer peer) {
		return new TCPRelayClient(connection, peer);
	}

}
