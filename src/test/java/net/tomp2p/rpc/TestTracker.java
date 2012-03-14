package net.tomp2p.rpc;
import java.util.Random;

import junit.framework.Assert;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

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
			// make a good guess based on the config and the maxium tracker that can be found
			SimpleBloomFilter<Number160> bloomFilter=new SimpleBloomFilter<Number160>(4096, 1000);
			final FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = sender.getTrackerRPC().addToTracker(recv1.getPeerAddress(), loc,
					dom, null, false, false, bloomFilter, cc, false, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			bloomFilter=new SimpleBloomFilter<Number160>(4096, 1000);
			fr = sender.getTrackerRPC().getFromTracker(recv1.getPeerAddress(), loc, dom, false,
					false, bloomFilter, cc);
			fr.awaitUninterruptibly();
			System.err.println(fr.getFailedReason());
			Assert.assertEquals(true, fr.isSuccess());
			PeerAddress peerAddress=fr.getResponse().getTrackerData().iterator().next().getPeerAddress();
			Assert.assertEquals(sender.getPeerAddress(), peerAddress);
			sender.getConnectionBean().getConnectionReservation().release(cc);
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
	public void testTrackerPutNoBloomFilter() throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x50"));
			sender.getConnectionConfiguration().setIdleTCPMillis(Integer.MAX_VALUE);
			sender.getConnectionConfiguration().setIdleUDPMillis(Integer.MAX_VALUE);
			sender.listen(2424, 2424);
			
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.getConnectionConfiguration().setIdleTCPMillis(Integer.MAX_VALUE);
			recv1.getConnectionConfiguration().setIdleUDPMillis(Integer.MAX_VALUE);
			recv1.listen(8088, 8088);
			Number160 loc = new Number160(rnd);
			Number160 dom = new Number160(rnd);
			// make a good guess based on the config and the maxium tracker that can be found
			final FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = sender.getTrackerRPC().addToTracker(recv1.getPeerAddress(), loc,
					dom, null, false, false, null, cc, false, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			fr = sender.getTrackerRPC().getFromTracker(recv1.getPeerAddress(), loc, dom, false,
					false, null, cc);
			fr.awaitUninterruptibly();
			System.err.println(fr.getFailedReason());
			Assert.assertEquals(true, fr.isSuccess());
			PeerAddress peerAddress=fr.getResponse().getTrackerData().iterator().next().getPeerAddress();
			Assert.assertEquals(sender.getPeerAddress(), peerAddress);
			sender.getConnectionBean().getConnectionReservation().release(cc);
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
	public void testTrackerPutAttachment() throws Exception
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
			// make a good guess based on the config and the maxium tracker that can be found
			final FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(2);
			fcc.awaitUninterruptibly();
			ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = sender.getTrackerRPC().addToTracker(recv1.getPeerAddress(), loc,
					dom, new String("data").getBytes(), false, false, null, cc, false, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			fr = sender.getTrackerRPC().getFromTracker(recv1.getPeerAddress(), loc, dom, false,
					false, null, cc);
			fr.awaitUninterruptibly();
			System.err.println("ERR:"+fr.getFailedReason());
			Assert.assertEquals(true, fr.isSuccess());
			PeerAddress peerAddress=fr.getResponse().getTrackerData().iterator().next().getPeerAddress();
			Assert.assertEquals(sender.getPeerAddress(), peerAddress);
			String tmp=new String(fr.getResponse().getTrackerData().iterator().next().getAttachement());
			Assert.assertEquals(tmp,"data");
			sender.getConnectionBean().getConnectionReservation().release(cc);
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
	public void testTrackerBloomFilter() throws Exception
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
			// make a good guess based on the config and the maxium tracker that can be found
			SimpleBloomFilter<Number160> bloomFilter=new SimpleBloomFilter<Number160>(4096, 1000);
			final FutureChannelCreator fcc=sender.getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = sender.getTrackerRPC().addToTracker(recv1.getPeerAddress(), loc,
					dom, new String("data").getBytes(), false, false, bloomFilter, cc, false, false);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			bloomFilter.add(sender.getPeerID());
			fr = sender.getTrackerRPC().getFromTracker(recv1.getPeerAddress(), loc, dom, false,
					false, bloomFilter, cc);
			fr.awaitUninterruptibly();
			System.err.println(fr.getFailedReason());
			Assert.assertEquals(true, fr.isSuccess());
			Assert.assertEquals(0, fr.getResponse().getTrackerData().size());
			sender.getConnectionBean().getConnectionReservation().release(cc);
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
