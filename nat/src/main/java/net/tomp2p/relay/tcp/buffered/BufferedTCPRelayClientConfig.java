package net.tomp2p.relay.tcp.buffered;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.relay.BaseRelayClient;
import net.tomp2p.relay.RelayClientConfig;
import net.tomp2p.relay.RelayType;

public class BufferedTCPRelayClientConfig extends RelayClientConfig {

	public BufferedTCPRelayClientConfig() {
		super(RelayType.BUFFERED_OPENTCP, 15, 60, 2);
	}

	@Override
	public BaseRelayClient createClient(PeerConnection connection, Peer peer) {
		return new BufferedTCPRelayClient(connection, peer);
	}

	@Override
	public void prepareSetupMessage(Message message) {
		// nothing to attach
	}

	@Override
	public void prepareMapUpdateMessage(Message message) {
		// nothing to attach
	}

}
