package net.tomp2p.rpc;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;

import org.junit.Assert;
import org.junit.Test;

public class PingTest {

	/**
	 * Copy of {@link TestPing.testPingUDP()}.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testPingUDP() throws Exception {
		
		Peer sender = null;
        Peer recv1 = null;
        ChannelCreator cc = null;
        try {
            sender = new PeerBuilder(new Number160("0x9876")).p2pId(55).ports(2424).start();
            recv1 = new PeerBuilder(new Number160("0x1234")).p2pId(55).ports(8088).start();
            
            PingRPC handshake = new PingRPC(sender.peerBean(), sender.connectionBean());

            FutureChannelCreator fcc = recv1.connectionBean().reservation().create(1, 0);
            fcc.awaitUninterruptibly();
            cc = fcc.channelCreator();
            
            FutureResponse fr = handshake.pingUDP(recv1.peerAddress(), cc,
                    new DefaultConnectionConfiguration());
            fr.awaitUninterruptibly();
            
            Assert.assertEquals(true, fr.isSuccess());
        } finally {
            if (cc != null) {
                cc.shutdown().await();
            }
            if (sender != null) {
                sender.shutdown().await();
            }
            if (recv1 != null) {
                recv1.shutdown().await();
            }
		}
	}
}
