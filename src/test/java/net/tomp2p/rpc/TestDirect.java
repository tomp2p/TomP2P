package net.tomp2p.rpc;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureData;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Utils;

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
			sender = new Peer(55, new Number160("0x50"));
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.listen(8088, 8088);
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
			sender = new Peer(55, new Number160("0x50"));
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.listen(8088, 8088);
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
		ChannelCreator.resetStat();
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			
			sender = new Peer(55, new Number160("0x50"));
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.listen(8088, 8088);
			recv1.setObjectDataReply(new ObjectDataReply() {
				@Override
				public Object reply(PeerAddress sender, Object request) throws Exception {
					return "yes";
				}
			});
			PeerConnection peerConnection = sender.createPeerConnection(recv1.getPeerAddress(), 10*1000);
			Assert.assertEquals(0, ChannelCreator.getStatConnectionsCreatedTCP());
			Assert.assertEquals(0, ChannelCreator.getStatConnectionsCreatedUDP());
			FutureData fd1=sender.send(peerConnection, "test");
			Assert.assertEquals(1, ChannelCreator.getStatConnectionsCreatedTCP());
			Assert.assertEquals(0, ChannelCreator.getStatConnectionsCreatedUDP());
			fd1.awaitUninterruptibly();
			Utils.sleep(2000);
			System.err.println("send second with the same connection");
			FutureData fd2=sender.send(peerConnection, "test");
			fd2.awaitUninterruptibly();
			Assert.assertEquals(1, ChannelCreator.getStatConnectionsCreatedTCP());
			Assert.assertEquals(0, ChannelCreator.getStatConnectionsCreatedUDP());
			Assert.assertEquals(true, fd1.isSuccess());
			Assert.assertEquals(true, fd2.isSuccess());
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
	public void testDirectReconnectParallel() throws Exception
	{
		ChannelCreator.resetStat();
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x50"));
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.listen(8088, 8088);
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
			Assert.assertEquals(1, ChannelCreator.getStatConnectionsCreatedTCP());
			Assert.assertEquals(0, ChannelCreator.getStatConnectionsCreatedUDP());
			for(int i=0;i<len;i++)
			{
				fd[i].awaitUninterruptibly();
				Assert.assertEquals(true, fd[i].isSuccess());
			}
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
		final StringBuilder sb=new StringBuilder();
		new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				String s1="";
				for(int i=0;i<1000000;i++)
				{
					s1+="test"+i;
				}
				sb.append(s1);
			}
		}).start();
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x9876"));
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x1234"));
			recv1.listen(8088, 8088);
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
			Utils.sleep(2000);
			
		}
		finally
		{
			sender.shutdown();
			recv1.shutdown();
		}
	}
}
