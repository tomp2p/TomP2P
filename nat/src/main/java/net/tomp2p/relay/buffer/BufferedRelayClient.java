package net.tomp2p.relay.buffer;

import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.BaseRelayClient;

/**
 * Extends the basic relay connection capabilities by functions to handle the buffer at the relay peer.
 * This class is held at the unreachable peer (client side).
 * 
 * @author Nico Rutishauser
 *
 */
public abstract class BufferedRelayClient extends BaseRelayClient {

	private final BufferedMessageHandler bufferedMessageHandler;
	protected final Peer peer;

	public BufferedRelayClient(PeerAddress relayAddress, Peer peer) {
		super(relayAddress);
		this.peer = peer;
		this.bufferedMessageHandler = new BufferedMessageHandler(peer);
	}
	
	public void onReceiveMessageBuffer(Message responseMessage, FutureDone<Void> futureDone) {
		bufferedMessageHandler.handleBufferResponse(responseMessage, futureDone);
	}
	
	/**
	 * Send a request to the relay peer to obtain the buffer.
	 */
	public abstract FutureDone<Void> sendBufferRequest();
}
