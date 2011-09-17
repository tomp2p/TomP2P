
package net.tomp2p.rpc;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.utils.Utils;

import org.junit.Assert;
import org.junit.Test;

public class TestPeerExchange
{
	@Test
	public void testPex() throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x9876"));
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x1234"));
			recv1.listen(8088, 8088);
			Number160 locationKey = new Number160("0x5555");
			Number160 domainKey = new Number160("0x7777");
			ChannelCreator cc=recv1.getConnectionHandler().getConnectionReservation().reserve(1);
			sender.getPeerBean().getTrackerStorage().addActive(locationKey, domainKey,sender.getPeerAddress(), null, 0, 0);
			FutureResponse fr = sender.getPeerExchangeRPC().peerExchange(recv1.getPeerAddress(), locationKey, domainKey, false, cc);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			Utils.sleep(200);
			Assert.assertEquals(1, recv1.getPeerBean().getTrackerStorage().sizeSecondary(locationKey, domainKey));
		}
		finally
		{
			if (sender != null)
				sender.shutdown();
			if (recv1 != null)
				recv1.shutdown();
		}
	}
}
