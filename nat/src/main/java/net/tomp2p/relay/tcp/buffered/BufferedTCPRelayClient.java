package net.tomp2p.relay.tcp.buffered;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.relay.RelayRPC;
import net.tomp2p.relay.buffer.BufferedRelayClient;
import net.tomp2p.relay.tcp.TCPRelayClient;

/**
 * Basically the same implementation as {@link TCPRelayClient}, but exending from the
 * {@link BufferedRelayClient} such that the {@link RelayRPC} can distinguish between buffered and unbuffered
 * connections.
 * 
 * @author Nico Rutishauser
 *
 */
public class BufferedTCPRelayClient extends BufferedRelayClient {

	private final TCPRelayClient tcpRelayClient;

	public BufferedTCPRelayClient(PeerConnection connection, Peer peer) {
		super(connection.remotePeer(), peer);
		tcpRelayClient = new TCPRelayClient(connection, peer);
	}

	@Override
	public FutureDone<Void> sendBufferRequest() {
		// nothing to do because the buffer is sent automatically
		return new FutureDone<Void>().done();
	}

	@Override
	public FutureResponse sendToRelay(Message message) {
		return tcpRelayClient.sendToRelay(message);
	}

	@Override
	public FutureDone<Void> shutdown() {
		return tcpRelayClient.shutdown();
	}

	@Override
	public void onMapUpdateSuccess() {
		// nothing to do
	}

	@Override
	public void onMapUpdateFailed() {
		// nothing to do
	}

}
