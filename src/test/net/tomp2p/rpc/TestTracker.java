package net.tomp2p.rpc;
import java.util.Random;

import junit.framework.Assert;

import net.tomp2p.Utils2;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.TrackerStorage;
import net.tomp2p.utils.Utils;

import org.junit.Test;


public class TestTracker
{
	final static Random rnd = new Random(0);

	@Test
	public void testTrackerPut() throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x50"));
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.listen(8088, 8088);
			Number160 loc = new Number160(rnd);
			Number160 dom = new Number160(rnd);
			FutureResponse fr = sender.getTrackerRPC().addToTracker(recv1.getPeerAddress(), loc,
					dom, new Data(new String("data")), false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			fr = sender.getTrackerRPC().getFromTracker(recv1.getPeerAddress(), loc, dom, false,
					false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			Assert.assertEquals(sender.getPeerAddress(), fr.getResponse().getPeerDataMap().keySet()
					.iterator().next());
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
	public void testTrackerMap() throws Exception
	{
		TrackerStorage ts = new TrackerStorage();
		Number160 loc = new Number160(rnd);
		Number160 dom = new Number160(rnd);
		PeerAddress pa1 = Utils2.createAddress("0x1");
		PeerAddress pa2 = Utils2.createAddress("0x2");
		Data data = new Data(new String("data"));
		data.setTTLSeconds(1);
		ts.put(loc, dom, pa1, null, data);
		ts.put(loc, dom, pa2, null, data);
		Utils.sleep(500);
		Assert.assertEquals(2, ts.get(loc, dom).size());
		Utils.sleep(500);
		Assert.assertEquals(0, ts.get(loc, dom).size());
	}
}
