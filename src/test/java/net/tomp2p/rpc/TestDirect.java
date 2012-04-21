package net.tomp2p.rpc;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureData;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Timings;

import org.junit.Assert;
import org.junit.Test;

public class TestDirect
{
	@Test
	public void testDirectMessage() throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).buildAndListen();
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).buildAndListen();
			recv1.setObjectDataReply(new ObjectDataReply() {
				@Override
				public Object reply(PeerAddress sender, Object request) throws Exception {
					return "yes";
				}
			});
			FutureData fd=sender.send(recv1.getPeerAddress(), "test");
			fd.awaitUninterruptibly();
			System.err.println(fd.getFailedReason());
			Assert.assertEquals(true, fd.isSuccess());
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
	public void testDirectMessage1() throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).buildAndListen();
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).buildAndListen();
			recv1.setObjectDataReply(new ObjectDataReply() {
				@Override
				public Object reply(PeerAddress sender, Object request) throws Exception {
					return "yes";
				}
			});
			FutureData fd1=sender.send(recv1.getPeerAddress(), "test");
			FutureData fd2=sender.send(recv1.getPeerAddress(), "test");
			fd1.awaitUninterruptibly();
			fd2.awaitUninterruptibly();
			System.err.println(fd1.getFailedReason());
			Assert.assertEquals(true, fd1.isSuccess());
			Assert.assertEquals(true, fd2.isSuccess());
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
	public void testDirectReconnect() throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).buildAndListen();
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).buildAndListen();
			recv1.setObjectDataReply(new ObjectDataReply() {
				@Override
				public Object reply(PeerAddress sender, Object request) throws Exception {
					return "yes";
				}
			});
			PeerConnection peerConnection = sender.createPeerConnection(recv1.getPeerAddress(), 10*1000);
			Assert.assertEquals(0, sender.getPeerBean().getStatistics().getTCPChannelCreationCount());
			Assert.assertEquals(0, sender.getPeerBean().getStatistics().getUDPChannelCreationCount());
			FutureData fd1=sender.send(peerConnection, "test");
			Assert.assertEquals(1, sender.getPeerBean().getStatistics().getTCPChannelCreationCount());
			Assert.assertEquals(0, sender.getPeerBean().getStatistics().getUDPChannelCreationCount());
			fd1.awaitUninterruptibly();
			Timings.sleep(2000);
			System.err.println("send second with the same connection");
			FutureData fd2=sender.send(peerConnection, "test");
			fd2.awaitUninterruptibly();
			Assert.assertEquals(1, sender.getPeerBean().getStatistics().getTCPChannelCreationCount());
			Assert.assertEquals(0, sender.getPeerBean().getStatistics().getUDPChannelCreationCount());
			Assert.assertEquals(true, fd1.isSuccess());
			Assert.assertEquals(true, fd2.isSuccess());
			peerConnection.close();
			System.err.println("done");
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
	public void testDirect2() throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).setEnableMaintenance(false).buildAndListen();
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).setEnableMaintenance(false).buildAndListen();
			recv1.setObjectDataReply(new ObjectDataReply() {
				@Override
				public Object reply(PeerAddress sender, Object request) throws Exception {
					return "yes";
				}
			});
			PeerConnection peerConnection = sender.createPeerConnection(recv1.getPeerAddress(), 5*1000);
			FutureData fd1=sender.send(peerConnection, "test");
			fd1.awaitUninterruptibly();

			Assert.assertEquals(1.0d, sender.getPeerBean().getStatistics().getTCPChannelCount(),0.0d);
			Assert.assertEquals(1.0d, sender.getPeerBean().getStatistics().getTCPChannelCreationCount(),0.0d);
			System.out.println("#TCP=" +  sender.getPeerBean().getStatistics().getTCPChannelCount()+"/"+sender.getPeerBean().getStatistics().getTCPChannelCreationCount());
			Timings.sleep(7000);
			Assert.assertEquals(0.0d, sender.getPeerBean().getStatistics().getTCPChannelCount(),0.0d);
			Assert.assertEquals(1.0d, sender.getPeerBean().getStatistics().getTCPChannelCreationCount(),0.0d);
			System.out.println("#TCP=" +  sender.getPeerBean().getStatistics().getTCPChannelCount()+"/"+sender.getPeerBean().getStatistics().getTCPChannelCreationCount());
			FutureData fd2=sender.send(peerConnection, "test");
			fd2.awaitUninterruptibly();
			peerConnection.close();
			System.out.println("done");
			Thread.sleep(4000);
			System.out.println("#TCP=" +  sender.getPeerBean().getStatistics().getTCPChannelCount()+"/"+sender.getPeerBean().getStatistics().getTCPChannelCreationCount());
			Assert.assertEquals(1.0d, sender.getPeerBean().getStatistics().getTCPChannelCount(),0.0d);
			Assert.assertEquals(2.0d, sender.getPeerBean().getStatistics().getTCPChannelCreationCount(),0.0d);
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
	public void testDirectReconnectParallel() throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x50")).setP2PId(55).setPorts(2424).buildAndListen();
			recv1 = new PeerMaker(new Number160("0x20")).setP2PId(55).setPorts(8088).buildAndListen();
			recv1.setObjectDataReply(new ObjectDataReply() {
				@Override
				public Object reply(PeerAddress sender, Object request) throws Exception {
					return "yes";
				}
			});
			PeerConnection peerConnection = sender.createPeerConnection(recv1.getPeerAddress(), 10*1000);
			int len=20;
			FutureData fd[]=new FutureData[len];
			for(int i=0;i<len;i++)
			{
				fd[i]=sender.send(peerConnection, "test");
			}
			Assert.assertEquals(1, sender.getPeerBean().getStatistics().getTCPChannelCreationCount());
			Assert.assertEquals(0, sender.getPeerBean().getStatistics().getUDPChannelCreationCount());
			for(int i=0;i<len;i++)
			{
				fd[i].awaitUninterruptibly();
				Assert.assertEquals(true, fd[i].isSuccess());
			}
			peerConnection.close();
			System.err.println("done");
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
	public void testOrder() throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new PeerMaker(new Number160("0x9876")).setP2PId(55).setPorts(2424).buildAndListen();
			recv1 = new PeerMaker(new Number160("0x1234")).setP2PId(55).setPorts(8088).buildAndListen();
			recv1.setObjectDataReply(new ObjectDataReply()
			{
				@Override
				public Object reply(PeerAddress sender, Object request) throws Exception
				{
					Integer i = (Integer) request;
					System.err.println("got "+i);
					return i + 1;
				}
			});
			for (int i = 0; i < 500; i++)
			{
				FutureData futureData = sender.send(recv1.getPeerAddress(), (Object) Integer
						.valueOf(i));
				futureData.addListener(new BaseFutureAdapter<FutureData>()
				{
					@Override
					public void operationComplete(FutureData future) throws Exception
					{
						System.err.println(future.getObject());
					}
				});
			}
			System.err.println("done");
			Timings.sleep(2000);
			
		}
		finally
		{
			sender.shutdown();
			recv1.shutdown();
		}
	}
}
