package net.tomp2p.rpc;
import java.util.ArrayList;
import java.util.List;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelCreator;
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
			ChannelCreator cc=recv1.getConnectionBean().getReservation().reserve(1);
			FutureResponse fr = sender.getHandshakeRPC().pingTCP(recv1.getPeerAddress(), cc);
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
			ChannelCreator cc1=sender.getConnectionBean().getReservation().reserve(1);
			FutureResponse fr = sender.getHandshakeRPC().pingTCP(recv1.getPeerAddress(), cc1);
			fr.awaitUninterruptibly();
			ChannelCreator cc2=recv1.getConnectionBean().getReservation().reserve(1);
			FutureResponse fr2 = recv1.getHandshakeRPC().pingTCP(sender.getPeerAddress(), cc2);
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
			ChannelCreator cc1=sender.getConnectionBean().getReservation().reserve(1);
			FutureResponse fr = sender.getHandshakeRPC().pingTCP(recv1.getPeerAddress(), cc1);
			fr.awaitUninterruptibly();
			final ChannelCreator cc2=sender.getConnectionBean().getReservation().reserve(1);
			fr.addListener(new BaseFutureAdapter<FutureResponse>()
			{
				@Override
				public void operationComplete(FutureResponse future) throws Exception
				{
					FutureResponse fr2 = sender.getHandshakeRPC().pingTCP(recv1.getPeerAddress(), cc2);
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
			final ChannelCreator cc=sender.getConnectionBean().getReservation().reserve(1);
			FutureResponse fr = handshake.pingUDP(recv1.getPeerAddress(), cc);
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
			final ChannelCreator cc=sender.getConnectionBean().getReservation().reserve(1);
			FutureResponse fr = handshake.pingTCP(recv1.getPeerBean().getServerPeerAddress(), cc);
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
			final ChannelCreator cc=sender.getConnectionBean().getReservation().reserve(1);
			FutureResponse fr = handshake.pingTCP(recv1.getPeerBean().getServerPeerAddress(), cc);
			fr.awaitUninterruptibly();
			Assert.assertEquals(false, fr.isSuccess());
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
			final ChannelCreator cc=sender.getConnectionBean().getReservation().reserve(1);
			FutureResponse fr = handshake.pingUDP(recv1.getPeerBean().getServerPeerAddress(), cc);
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
			recv1 = new Peer(55, new Number160("0x1234"));
			recv1.listen(8088, 8088);
			List<FutureResponse> list = new ArrayList<FutureResponse>(50);
			final ChannelCreator cc=sender.getConnectionBean().getReservation().reserve(50);
			for (int i = 0; i < 50; i++)
			{
				FutureResponse fr = sender.getHandshakeRPC().pingTCP(recv1.getPeerAddress(), cc);
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
				final ChannelCreator cc=p[0].getConnectionBean().getReservation().reserve(1);
				FutureResponse fr = p[0].getHandshakeRPC().pingTCP(p[i].getPeerAddress(), cc);
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
			//Utils.sleep(100);
			// sender.setBlocking(false);
			recv1 = new Peer(55, new Number160("0x1234"));
			recv1.listen(8088, 8088);
			//Utils.sleep(100);
			// recv1.setBlocking(false);
			long start = System.currentTimeMillis();
			List<FutureResponse> list = new ArrayList<FutureResponse>(100);
			for (int i = 0; i < 20; i++)
			{
				final ChannelCreator cc=sender.getConnectionBean().getReservation().reserve(50);
				for (int j = 0; j < 50; j++)
				{
					FutureResponse fr = sender.getHandshakeRPC().pingTCP(recv1.getPeerAddress(), cc);
					list.add(fr);
				}
				for (FutureResponse fr2 : list)
				{
					fr2.awaitUninterruptibly();
					if (!fr2.isSuccess())
						System.err.println("fail " + fr2.getFailedReason());
					Assert.assertEquals(true, fr2.isSuccess());
				}
				list.clear();
				cc.release();
			}
			System.out.println("TCP time: " + (System.currentTimeMillis() - start));
			for (FutureResponse fr2 : list)
			{
				fr2.awaitUninterruptibly();
				Assert.assertEquals(true, fr2.isSuccess());
			}
			//
			start = System.currentTimeMillis();
			list = new ArrayList<FutureResponse>(50);
			for (int i = 0; i < 20; i++)
			{
				final ChannelCreator cc=sender.getConnectionBean().getReservation().reserve(50);
				for (int j = 0; j < 50; j++)
				{
					FutureResponse fr = sender.getHandshakeRPC().pingUDP(recv1.getPeerAddress(), cc);
					list.add(fr);
				}
				for (FutureResponse fr2 : list)
				{
					fr2.awaitUninterruptibly();
					Assert.assertEquals(true, fr2.isSuccess());
				}
				list.clear();
				cc.release();
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
