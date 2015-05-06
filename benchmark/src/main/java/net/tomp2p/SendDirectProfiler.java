package net.tomp2p;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.message.Buffer;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.builder.SendDirectBuilder;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.RawDataReply;
import net.tomp2p.storage.AlternativeCompositeByteBuf;

public abstract class SendDirectProfiler extends Profiler {

	private static final int BUFFER_SIZE_BYTES = 1000;
	protected final boolean isForceUdp;
	protected Peer sender;
	protected Peer receiver;
	protected ChannelCreator cc;
	protected SendDirectBuilder sendDirectBuilder;
	
	public SendDirectProfiler(boolean isForceUdp) {
		this.isForceUdp = isForceUdp;
	}
	
	@Override
	protected void shutdown() throws Exception {
		if (sender != null) {
			sender.shutdown().awaitUninterruptibly();
		}
		if (receiver != null) {
			receiver.shutdown().awaitUninterruptibly();
		}
		if (cc != null) {
			cc.shutdown().awaitUninterruptibly();
		}
	}
	
	protected static Buffer createSampleBuffer() {
		AlternativeCompositeByteBuf acbb = AlternativeCompositeByteBuf.compBuffer();
		for (int i = 0; i < BUFFER_SIZE_BYTES; i++) {
			acbb.writeByte(i % 256);
		}
		return new Buffer(acbb);
	}
	
	protected class SampleRawDataReply implements RawDataReply {

		@Override
		public Buffer reply(PeerAddress sender, Buffer requestBuffer, boolean complete) throws Exception {
			// server returns just OK if same buffer is returned
			return requestBuffer;
		}
	}
}
