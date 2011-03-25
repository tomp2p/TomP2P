package net.tomp2p.rpc;
import java.util.ArrayList;
import java.util.List;

import net.tomp2p.connection.Bindings;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.rpc.HandshakeRPC;
import net.tomp2p.utils.Utils;

import org.junit.Assert;
import org.junit.Test;

public class TestPing
{
	static Bindings bindings = new Bindings();
	static
	{
		bindings.addInterface("lo");
	}

	@Test
	public void testPingTCP() throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x9876"));
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x1234"));
			recv1.listen(8088, 8088);
			FutureResponse fr = sender.getHandshakeRPC().pingTCP(recv1.getPeerAddress());
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
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
	public void testPingTCP2() throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x9876"));
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x1234"));
			recv1.listen(8088, 8088);
			FutureResponse fr = sender.getHandshakeRPC().pingTCP(recv1.getPeerAddress());
			fr.awaitUninterruptibly();
			FutureResponse fr2 = recv1.getHandshakeRPC().pingTCP(sender.getPeerAddress());
			fr2.awaitUninterruptibly();
			Assert.assertEquals(true, fr2.isSuccess());
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
	public void testPingTCPDeadLock2() throws Exception
	{
		Peer sender1 = null;
		Peer recv11 = null;
		try
		{
			final Peer sender = new Peer(55, new Number160("0x9876"));
			sender1 = sender;
			sender.listen(2424, 2424);
			final Peer recv1 = new Peer(55, new Number160("0x1234"));
			recv11 = recv1;
			recv1.listen(8088, 8088);
			FutureResponse fr = sender.getHandshakeRPC().pingTCP(recv1.getPeerAddress());
			fr.awaitUninterruptibly();
			fr.addListener(new BaseFutureAdapter<FutureResponse>()
			{
				@Override
				public void operationComplete(FutureResponse future) throws Exception
				{
					FutureResponse fr2 = sender.getHandshakeRPC().pingTCP(recv1.getPeerAddress());
					try
					{
						fr2.await();
					}
					catch (IllegalStateException ise)
					{
						Assert.fail();
					}
				}
			});
			Utils.sleep(1000);
			Assert.assertEquals(true, fr.isSuccess());
		}
		finally
		{
			if (sender1 != null)
				sender1.shutdown();
			if (recv11 != null)
				recv11.shutdown();
		}
	}

	@Test
	public void testPingUDP() throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x9876"));
			sender.listen(2424, 2424);
			HandshakeRPC handshake = new HandshakeRPC(sender.getPeerBean(), sender
					.getConnectionBean());
			recv1 = new Peer(55, new Number160("0x1234"));
			recv1.listen(8088, 8088);
			new HandshakeRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			FutureResponse fr = handshake.pingUDP(recv1.getPeerAddress());
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
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
	public void testPingTimeoutTCP() throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x9876"));
			sender.listen(2424, 2424);
			HandshakeRPC handshake = new HandshakeRPC(sender.getPeerBean(), sender
					.getConnectionBean(), false, true, false);
			recv1 = new Peer(55, new Number160("0x1234"));
			recv1.listen(8088, 8088);
			new HandshakeRPC(recv1.getPeerBean(), recv1.getConnectionBean(), false, true, false);
			FutureResponse fr = handshake.pingTCP(recv1.getPeerBean().getServerPeerAddress());
			fr.awaitUninterruptibly();
			Assert.assertEquals(false, fr.isSuccess());
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
	public void testPingTimeoutTCP2() throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x9876"));
			sender.listen(2424, 2424);
			HandshakeRPC handshake = new HandshakeRPC(sender.getPeerBean(), sender
					.getConnectionBean(), false, true, true);
			recv1 = new Peer(55, new Number160("0x1234"));
			recv1.listen(8088, 8088);
			new HandshakeRPC(recv1.getPeerBean(), recv1.getConnectionBean(), false, true, true);
			FutureResponse fr = handshake.pingTCP(recv1.getPeerBean().getServerPeerAddress());
			fr.awaitUninterruptibly();
			Assert.assertEquals(false, fr.isSuccess());
			Assert.assertEquals(fr.getFailedReason().indexOf("complete=true/Timeout"),0);
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
	public void testPingTimeoutUDP() throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x9876"));
			sender.listen(2424, 2424);
			HandshakeRPC handshake = new HandshakeRPC(sender.getPeerBean(), sender
					.getConnectionBean(), false, true, false);
			recv1 = new Peer(55, new Number160("0x1234"));
			recv1.listen(8088, 8088);
			new HandshakeRPC(recv1.getPeerBean(), recv1.getConnectionBean(), false, true, false);
			FutureResponse fr = handshake.pingUDP(recv1.getPeerBean().getServerPeerAddress());
			fr.awaitUninterruptibly();
			Assert.assertEquals(false, fr.isSuccess());
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
	public void testPingTCPPool() throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x9876"));
			sender.listen(2424, 2424);
			Utils.sleep(100);
			// sender.setBlocking(false);
			recv1 = new Peer(55, new Number160("0x1234"));
			recv1.listen(8088, 8088);
			List<FutureResponse> list = new ArrayList<FutureResponse>(10000);
			for (int i = 0; i < 100; i++)
			{
				FutureResponse fr = sender.getHandshakeRPC().pingTCP(recv1.getPeerAddress());
				list.add(fr);
			}
			for (FutureResponse fr2 : list)
			{
				fr2.awaitUninterruptibly();
				Assert.assertTrue(fr2.isSuccess());
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

	@Test
	public void testPingTCPPool2() throws Exception
	{
		Peer p[] = new Peer[50];
		try
		{
			for (int i = 0; i < p.length; i++)
			{
				p[i] = new Peer(55, Number160.createHash(i));
				p[i].listen(2424 + i, 2424 + i);
			}
			List<FutureResponse> list = new ArrayList<FutureResponse>();
			for (int i = 0; i < p.length; i++)
			{
				FutureResponse fr = p[0].getHandshakeRPC().pingTCP(p[i].getPeerAddress());
				list.add(fr);
			}
			for (FutureResponse fr2 : list)
			{
				fr2.awaitUninterruptibly();
				boolean success = fr2.isSuccess();
				if (!success)
				{
					System.err.println("FAIL.");
					Assert.fail();
				}
			}
			System.err.println("DONE.");
		}
		finally
		{
			for (int i = 0; i < p.length; i++)
			{
				p[i].shutdown();
			}
		}
	}

	@Test
	public void testPingTime() throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x9876"));
			sender.listen(2424, 2424);
			Utils.sleep(100);
			// sender.setBlocking(false);
			recv1 = new Peer(55, new Number160("0x1234"));
			recv1.listen(8088, 8088);
			Utils.sleep(100);
			// recv1.setBlocking(false);
			long start = System.currentTimeMillis();
			List<FutureResponse> list = new ArrayList<FutureResponse>(10000);
			for (int i = 0; i < 10000; i++)
			{
				FutureResponse fr = sender.getHandshakeRPC().pingTCP(recv1.getPeerAddress());
				list.add(fr);
			}
			for (FutureResponse fr2 : list)
			{
				fr2.awaitUninterruptibly();
				if (fr2.isFailed())
					System.err.println(fr2.getFailedReason());
				if (!fr2.isSuccess())
					System.err.println("fail " + fr2.getFailedReason());
				Assert.assertEquals(true, fr2.isSuccess());
			}
			list.clear();
			System.out.println("TCP time: " + (System.currentTimeMillis() - start));
			for (FutureResponse fr2 : list)
			{
				fr2.awaitUninterruptibly();
				Assert.assertEquals(true, fr2.isSuccess());
			}
			//
			start = System.currentTimeMillis();
			list = new ArrayList<FutureResponse>(10000);
			for (int i = 0; i < 10000; i++)
			{
				FutureResponse fr = sender.getHandshakeRPC().pingUDP(recv1.getPeerAddress());
				list.add(fr);
			}
			for (FutureResponse fr2 : list)
			{
				fr2.awaitUninterruptibly();
				Assert.assertEquals(true, fr2.isSuccess());
			}
			System.out.println("UDP time: " + (System.currentTimeMillis() - start));
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
