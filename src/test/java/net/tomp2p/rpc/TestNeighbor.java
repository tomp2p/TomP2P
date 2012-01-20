package net.tomp2p.rpc;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message.Command;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

import org.junit.Assert;
import org.junit.Test;

public class TestNeighbor
{
	public static int PORT_TCP = 5001;
	public static int PORT_UDP = 5002;

	@Test
	public void testNeigbhor() throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x50"));
			sender.listen(2424, 2424);
			PeerAddress[] pa = createDummyAddress(100);
			for (int i = 0; i < pa.length; i++)
				sender.getPeerBean().getPeerMap().peerFound(pa[i], null);
			new NeighborRPC(sender.getPeerBean(), sender.getConnectionBean());
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.listen(8088, 8088);
			NeighborRPC neighbors2 = new NeighborRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			FutureChannelCreator fcc = recv1.getConnectionBean().getReservation().reserve(1);
			fcc.awaitUninterruptibly();
			ChannelCreator cc = fcc.getChannelCreator();
			
			FutureResponse fr = neighbors2.closeNeighbors(sender.getPeerAddress(), new Number160(
					"0x30"), null, null, Command.NEIGHBORS_STORAGE, true, false, cc);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			Collection<PeerAddress> pas = fr.getResponse().getNeighbors();
			Assert.assertEquals(NeighborRPC.NEIGHBOR_SIZE, pas.size());
			Assert.assertEquals(new Number160("0x30"), pas.iterator().next().getID());
			Assert.assertEquals(PORT_TCP, pas.iterator().next().portTCP());
			Assert.assertEquals(PORT_UDP, pas.iterator().next().portUDP());
			recv1.getConnectionBean().getReservation().release(cc);
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
	public void testNeigbhor2() throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x50"));
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.listen(8088, 8088);
			new NeighborRPC(sender.getPeerBean(), sender.getConnectionBean());
			NeighborRPC neighbors2 = new NeighborRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			FutureChannelCreator fcc = recv1.getConnectionBean().getReservation().reserve(1);
			fcc.awaitUninterruptibly();
			ChannelCreator cc = fcc.getChannelCreator();
			FutureResponse fr = neighbors2.closeNeighbors(sender.getPeerAddress(), new Number160(
					"0x30"), null, null, Command.NEIGHBORS_STORAGE, true, false, cc);
			fr.awaitUninterruptibly();
			Assert.assertEquals(true, fr.isSuccess());
			Collection<PeerAddress> pas = fr.getResponse().getNeighbors();
			System.err.println(pas.iterator().next());
			// I see only myself
			Assert.assertEquals(1, pas.size());
			Assert.assertEquals(new Number160("0x20"), pas.iterator().next().getID());
			recv1.getConnectionBean().getReservation().release(cc);
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
	public void testNeigbhorFail() throws Exception
	{
		Peer sender = null;
		Peer recv1 = null;
		try
		{
			sender = new Peer(55, new Number160("0x50"));
			sender.listen(2424, 2424);
			recv1 = new Peer(55, new Number160("0x20"));
			recv1.listen(8088, 8088);
			new NeighborRPC(sender.getPeerBean(), sender.getConnectionBean());
			NeighborRPC neighbors2 = new NeighborRPC(recv1.getPeerBean(), recv1.getConnectionBean());
			FutureChannelCreator fcc = recv1.getConnectionBean().getReservation().reserve(1);
			fcc.awaitUninterruptibly();
			ChannelCreator cc = fcc.getChannelCreator();
			try
			{

				neighbors2.closeNeighbors(sender.getPeerAddress(),
						new Number160("0x30"), null,
						null, Command.PUT, true, false, cc);
				Assert.fail("");
			}
			catch (IllegalArgumentException i)
			{
				recv1.getConnectionBean().getReservation().release(cc);
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

	private PeerAddress[] createDummyAddress(int size) throws UnknownHostException
	{
		PeerAddress[] pa = new PeerAddress[size];
		for (int i = 0; i < size; i++)
			pa[i] = createAddress(i + 1);
		return pa;
	}

	private PeerAddress createAddress(int iid) throws UnknownHostException
	{
		Number160 id = new Number160(iid);
		InetAddress address = InetAddress.getByName("127.0.0.1");
		int portTCP = PORT_TCP;
		int portUDP = PORT_UDP;
		return new PeerAddress(id, address, portTCP, portUDP);
	}
}
