package net.tomp2p.relay.tcp.buffered;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.relay.RelayType;
import net.tomp2p.relay.buffer.BufferedRelayServer;
import net.tomp2p.relay.buffer.MessageBufferConfiguration;
import net.tomp2p.relay.tcp.TCPRelayServer;
import net.tomp2p.rpc.RPC.Commands;

/**
 * The buffered TCP server acts similar to the normal {@link TCPRelayServer}, but instead of sending messages
 * immediately to the unreachable peer, messages are buffered for a certain time.
 * 
 * @author Nico Rutishauser
 *
 */
public class BufferedTCPRelayServer extends BufferedRelayServer {

	private final PeerConnection connection;
	private final TCPRelayServer tcpRelayServer;

	protected BufferedTCPRelayServer(PeerConnection connection, Peer peer, MessageBufferConfiguration bufferConfig) {
		super(peer, connection.remotePeer(), RelayType.BUFFERED_OPENTCP, bufferConfig);
		this.connection = connection;
		this.tcpRelayServer = new TCPRelayServer(connection, peer);

		// add a listener when the connection is closed
		connection.closeFuture().addListener(new BaseFutureAdapter<FutureDone<Void>>() {
			@Override
			public void operationComplete(FutureDone<Void> future) throws Exception {
				notifyOfflineListeners();
			}
		});
	}

	@Override
	public void onBufferFull() {
		Buffer messages = collectBufferedMessages();
		Message message = createMessage(connection.remotePeer(), Commands.RELAY.getNr(), Type.REQUEST_4);
		message.buffer(messages);

		tcpRelayServer.forwardToUnreachable(message);
	}

	@Override
	protected void onBufferCollected() {
		// ignore
	}

	@Override
	protected boolean isAlive() {
		return connection.isOpen();
	}

}
