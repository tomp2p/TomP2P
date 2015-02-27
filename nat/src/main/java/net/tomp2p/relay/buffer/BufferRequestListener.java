package net.tomp2p.relay.buffer;

import net.tomp2p.futures.FutureDone;

public interface BufferRequestListener {

	/**
	 * Call this if the buffer should be obtained from a relay peer.
	 * 
	 * @param relayPeerId the relay's peer id
	 */
	FutureDone<Void> sendBufferRequest(String relayPeerId);
}
