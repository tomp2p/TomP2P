
package net.tomp2p.rpc;

import net.tomp2p.Utils2;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
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
			sender.getPeerBean().getTrackerStorage().put(locationKey, domainKey,sender.getPeerAddress(), null, null);
			FutureResponse fr = sender.getPeerExchangeRPC().peerExchange(recv1.getPeerAddress(), locationKey, domainKey);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			Utils.sleep(200);
			Assert.assertEquals(1, recv1.getPeerBean().getTrackerStorage().size(locationKey, domainKey));
		}
		finally
		{
			if (sender != null)
				sender.shutdown();
			if (recv1 != null)
				recv1.shutdown();
		}
	}
	
	@Test
	public void testPex2() throws Exception
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
			
			PeerAddress pa1= Utils2.createAddress(new Number160("0x1111"));
			PeerAddress pa2= Utils2.createAddress(new Number160("0x1112"));
			
			
			sender.getPeerBean().getTrackerStorage().put(locationKey, domainKey, sender.getPeerAddress(), null, null);
			sender.getPeerBean().getTrackerStorage().put(locationKey, domainKey, pa1, null, null);
			sender.getPeerBean().getTrackerStorage().put(locationKey, domainKey, pa2,  null, null);
			FutureResponse fr = sender.getPeerExchangeRPC().peerExchange(recv1.getPeerAddress(), locationKey, domainKey);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			Utils.sleep(200);
			Assert.assertEquals(1, recv1.getPeerBean().getTrackerStorage().size(locationKey, domainKey));
			Assert.assertEquals(2, recv1.getPeerBean().getTrackerStorage().size(locationKey, domainKey.xor(Number160.MAX_VALUE)));
		}
		finally
		{
			if (sender != null)
				sender.shutdown();
			if (recv1 != null)
				recv1.shutdown();
		}
	}
	
	@Test
	public void testPexLoop() throws Exception
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
			sender.getPeerBean().getTrackerStorage().put(locationKey, domainKey, sender.getPeerAddress(), null, null);
			for(int i=0;i<30;i++)
			{
				FutureResponse fr = sender.getPeerExchangeRPC().peerExchange(recv1.getPeerAddress(), locationKey, domainKey);
				fr.awaitUninterruptibly();
				Assert.assertEquals(true, fr.isSuccess());
				Utils.sleep(100);
				Assert.assertEquals(1, recv1.getPeerBean().getTrackerStorage().size(locationKey, domainKey));
			}
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
