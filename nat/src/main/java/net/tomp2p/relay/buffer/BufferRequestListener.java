package net.tomp2p.relay.buffer;

public interface BufferRequestListener {

	/**
	 * Call this if the buffer should be obtained from a relay peer.
	 * 
	 * @param relayPeerId the relay's peer id
	 */
	void sendBufferRequest(String relayPeerId);
}
