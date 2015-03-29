package net.tomp2p;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Buffer;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.builder.SendDirectBuilder;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.RawDataReply;
import net.tomp2p.storage.AlternativeCompositeByteBuf;

public class SendDirectLocalProfiler extends Profiler {

	private static final int NETWORK_SIZE = 2;
	private static final int BUFFER_SIZE_BYTES = 64000;
	private final boolean isForceUdp;
	private Peer sender;
	private Peer receiver;
	private ChannelCreator cc;
	private SendDirectBuilder sendDirectBuilder;
	
	public SendDirectLocalProfiler(boolean isForceUdp) {
		this.isForceUdp = isForceUdp;
	}
	
	@Override
	protected void setup() throws Exception {

		Network = BenchmarkUtil.createNodes(NETWORK_SIZE, Rnd, 9099, false, false);
		sender = Network[0];
		receiver = Network[1];
		receiver.rawDataReply(new SampleRawDataReply());
		FutureChannelCreator fcc = sender.connectionBean().reservation().create(isForceUdp ? 1 : 0, isForceUdp ? 0 : 1);
		fcc.awaitUninterruptibly();
		
		sendDirectBuilder = new SendDirectBuilder(sender, (PeerAddress) null)
			.streaming()
			.idleUDPSeconds(0)
			.idleTCPSeconds(0)
			.buffer(createSampleBuffer())
			.forceUDP(isForceUdp);
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
	
	@Override
	protected void execute() throws Exception {
		
		FutureResponse fr = sender.directDataRPC().send(receiver.peerAddress(), sendDirectBuilder, cc);
		fr.awaitUninterruptibly();
		
		// make buffer reusable
		sendDirectBuilder.buffer().reset();
		sendDirectBuilder.buffer().buffer().readerIndex(0);
	}
	
	private static Buffer createSampleBuffer() {
		AlternativeCompositeByteBuf acbb = AlternativeCompositeByteBuf.compBuffer();
		for (int i = 0; i < BUFFER_SIZE_BYTES; i++) {
			acbb.writeByte(i % 256);
		}
		return new Buffer(acbb);
	}
	
	private class SampleRawDataReply implements RawDataReply {

		@Override
		public Buffer reply(PeerAddress sender, Buffer requestBuffer, boolean complete) throws Exception {
			return requestBuffer;
		}
	}
}
