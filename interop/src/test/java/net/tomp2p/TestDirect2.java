package net.tomp2p;

import java.io.IOException;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Buffer;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.builder.SendDirectBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.RawDataReply;
import net.tomp2p.storage.AlternativeCompositeByteBuf;

import org.junit.Assert;
import org.junit.Test;

public class TestDirect2 {

	private static final byte[] TestRawBytes = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
	
	@Test
	public void testDirect1() throws InterruptedException, IOException {

		Peer sender = null;
		Peer recv1 = null;
		ChannelCreator cc = null;
		try {
			
			sender = new PeerBuilder(new Number160("0x50")).p2pId(55).ports(2424)
					.maintenanceTask(TestUtil.CreateInfiniteIntervalMaintenanceTask())
					.channelServerConfiguration(TestUtil.createInfiniteTimeoutChannelServerConfiguration(2424, 2424))
					.start();
			recv1 = new PeerBuilder(new Number160("0x20")).p2pId(55).ports(8088)
					.maintenanceTask(TestUtil.CreateInfiniteIntervalMaintenanceTask())
					.channelServerConfiguration(TestUtil.createInfiniteTimeoutChannelServerConfiguration(8088, 8088))
					.start();
			recv1.rawDataReply(new RawDataReply() {
				
				@Override
				public Buffer reply(PeerAddress sender, Buffer requestBuffer, boolean complete) throws Exception {
					return createTestBuffer();
				}
			});
			
			FutureChannelCreator fcc = sender.connectionBean().reservation().create(0, 2);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
            
            SendDirectBuilder sendDirectBuilder = new SendDirectBuilder(sender, (PeerAddress) null);
            sendDirectBuilder.streaming();
            sendDirectBuilder.idleTCPSeconds(Integer.MAX_VALUE);
            Buffer buffer = createTestBuffer();
            sendDirectBuilder.buffer(buffer);
            
            FutureResponse fr1 = sender.directDataRPC().send(recv1.peerAddress(), sendDirectBuilder, cc);
            fr1.awaitUninterruptibly();
            Assert.assertEquals(true, fr1.isSuccess());
            
            Buffer ret = fr1.responseMessage().buffer(0);
            Assert.assertEquals(buffer, ret);
		}
		finally {
			if (sender != null) {
				sender.shutdown().await();
			}
			if (recv1 != null) {
				recv1.shutdown().await();
			}
			if (cc != null) {
				cc.shutdown().await();
			}
		}
	}
	
	private static Buffer createTestBuffer() {
		AlternativeCompositeByteBuf acbb = AlternativeCompositeByteBuf.compBuffer();
		acbb.writeBytes(TestRawBytes);
		return new Buffer(acbb);
	}
}
