package net.tomp2p.peers;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import net.tomp2p.p2p.Statistics;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMapKadImpl;
import net.tomp2p.utils.Utils;

import org.junit.Assert;
import org.junit.Test;

public class TestKadRouting
{
	@Test
	public void testAdd()
	{
		Number160 id = new Number160("0x1");
		PeerMapKadImpl kadRouting = new PeerMapKadImpl(id, 2, 100, 60 * 1000, 3, new int[] {});
		Number160 id1 = new Number160("0x2");
		Number160 id2 = new Number160("0x3");
		Number160 id3 = new Number160("0x4");
		Number160 id4 = new Number160("0x5");
		Number160 id5 = new Number160("0x6");
		Number160 id6 = new Number160("0x7");
		Number160 id11 = id1.xor(id);
		Number160 id22 = id2.xor(id);
		Number160 id33 = id3.xor(id);
		Assert.assertEquals(2, id11.bitLength());
		Assert.assertEquals(2, id22.bitLength());
		Assert.assertEquals(3, id33.bitLength());
		// 
		PeerAddress pa1 = new PeerAddress(id1);
		PeerAddress pa2 = new PeerAddress(id2);
		PeerAddress pa3 = new PeerAddress(id3);
		PeerAddress pa4 = new PeerAddress(id4);
		PeerAddress pa5 = new PeerAddress(id5);
		PeerAddress pa6 = new PeerAddress(id6);
		kadRouting.peerFound(pa1, null);
		kadRouting.peerFound(pa2, null);
		kadRouting.peerFound(pa3, null);
		kadRouting.peerFound(pa4, null);
		kadRouting.peerFound(pa5, null);
		kadRouting.peerFound(pa6, null);
		SortedSet<PeerAddress> pa = kadRouting.closePeers(id, 2);
		Assert.assertEquals(2, pa.size());
		Iterator<PeerAddress> iterator = pa.iterator();
		Assert.assertEquals("0x3", iterator.next().getID().toString());
		Assert.assertEquals("0x2", iterator.next().getID().toString());
		pa = kadRouting.closePeers(id3, 3);
		Assert.assertEquals(4, pa.size());
		iterator = pa.iterator();
		Assert.assertEquals("0x4", iterator.next().getID().toString());
		Assert.assertEquals("0x5", iterator.next().getID().toString());
		Assert.assertEquals("0x6", iterator.next().getID().toString());
		Assert.assertEquals("0x7", iterator.next().getID().toString());
	}

	@Test
	public void testLength()
	{
		Number160 bi1 = new Number160("0x127");
		Number160 bi2 = new Number160("0x128");
		Number160 bi3 = new Number160("0x255");
		Number160 rr = PeerMapKadImpl.distance(bi1, bi2);
		Assert.assertFalse(bi3.equals(rr));
		bi1 = new Number160("0x7f");
		bi2 = new Number160("0x80");
		bi3 = new Number160("0xff");
		rr = PeerMapKadImpl.distance(bi1, bi2);
		Assert.assertTrue(bi3.equals(rr));
		Assert.assertEquals(7, PeerMapKadImpl.classMember(bi1, bi2));
		Assert.assertEquals(6, PeerMapKadImpl.classMember(bi2, bi3));
	}

	@Test
	public void testCloser() throws UnknownHostException
	{
		PeerAddress rn1 = new PeerAddress(new Number160("0x7f"));
		PeerAddress rn2 = new PeerAddress(new Number160("0x40"));
		Number160 key = new Number160("0xff");
		PeerMapKadImpl routing = new PeerMapKadImpl(new Number160("0x1"), 2, 100, 60 * 1000, 3,
				new int[] {});
		Assert.assertEquals(-1, routing.isCloser(key, rn1, rn2));
		//
		rn1 = new PeerAddress(new Number160("0x10"));
		rn2 = new PeerAddress(new Number160("0x11"));
		key = new Number160("0xff");
		System.err.println("rn1 " + rn1.getID().xor(key));
		System.err.println("rn2 " + rn2.getID().xor(key));
		Assert.assertEquals(1, routing.isCloser(key, rn1, rn2));
	}

	@Test
	public void testCloser2() throws UnknownHostException
	{
		PeerAddress rn1 = new PeerAddress(new Number160(98));
		PeerAddress rn2 = new PeerAddress(new Number160(66));
		PeerAddress rn3 = new PeerAddress(new Number160(67));
		PeerMapKadImpl routing = new PeerMapKadImpl(new Number160(999), 2, 100, 60 * 1000, 3,
				new int[] {});
		SortedSet<PeerAddress> rc = routing.closePeers(new Number160(98), 3);
		rc.add(rn2);
		rc.add(rn1);
		rc.add(rn3);
		Assert.assertTrue(rc.first().equals(rn1));
	}

	@Test
	public void testAddNode() throws UnknownHostException
	{
		PeerMapKadImpl kadRouting = new PeerMapKadImpl(new Number160("0x1"), 2, 100, 60 * 1000, 3,
				new int[] {});
		for (int i = 1; i < 12; i++)
		{
			PeerAddress r1 = new PeerAddress(new Number160(i));
			kadRouting.peerFound(r1, null);
		}
		SortedSet<PeerAddress> close = kadRouting.closePeers(new Number160(2), 2);
		Assert.assertEquals(2, close.size());
		close = kadRouting.closePeers(new Number160(6), 4);
		Assert.assertEquals(4, close.size());
	}

	@Test
	public void testAddNode2() throws UnknownHostException
	{
		PeerMapKadImpl kadRouting = new PeerMapKadImpl(new Number160("0x1"), 2, 100, 60 * 1000, 3,
				new int[] {});
		for (int i = 1; i < 12; i++)
		{
			PeerAddress r1 = new PeerAddress(new Number160((i % 6) + 1));
			kadRouting.peerFound(r1, null);
		}
		SortedSet<PeerAddress> close = kadRouting.closePeers(new Number160(2), 2);
		Assert.assertEquals(2, close.size());
		close = kadRouting.closePeers(new Number160(6), 1);
		Assert.assertEquals(3, close.size());
	}

	@Test
	public void testRemove() throws UnknownHostException
	{
		PeerMapKadImpl kadRouting = new PeerMapKadImpl(new Number160("0x1"), 2, 100, 60 * 1000, 3,
				new int[] {});
		for (int i = 1; i <= 200; i++)
		{
			PeerAddress r1 = new PeerAddress(new Number160(i + 1));
			kadRouting.peerFound(r1, null);
		}
		Assert.assertEquals(200, kadRouting.size());
		kadRouting.peerOffline(new PeerAddress(new Number160(100)), true);
		Assert.assertTrue(kadRouting.isPeerRemovedTemporarly(new PeerAddress(new Number160(100))));
		Assert.assertEquals(199, kadRouting.size());
	}

	@Test
	public void testRemoveConcurrent() throws UnknownHostException
	{
		final PeerMapKadImpl kadRouting = new PeerMapKadImpl(new Number160("0x1"), 2, 100,
				60 * 1000, 3, new int[] {});
		for (int i = 1; i <= 200; i++)
		{
			PeerAddress r1 = new PeerAddress(new Number160(i + 1));
			kadRouting.peerFound(r1, null);
		}
		Assert.assertEquals(200, kadRouting.size());
		new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				for (int i = 1; i <= 50; i++)
				{
					kadRouting.peerOffline(new PeerAddress(new Number160(i + 1)), true);
				}
			}
		}).start();
		new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				for (int i = 1; i <= 100; i++)
				{
					kadRouting.peerOffline(new PeerAddress(new Number160(i + 1)), true);
				}
			}
		}).start();
		Utils.sleep(500);
		Assert.assertTrue(kadRouting.isPeerRemovedTemporarly(new PeerAddress(new Number160(100))));
		Assert.assertEquals(100, kadRouting.size());
	}

	@Test
	public void testAddConcurrent() throws UnknownHostException
	{
		final PeerMapKadImpl kadRouting = new PeerMapKadImpl(new Number160("0x1"), 2, 100,
				60 * 1000, 3, new int[] {});
		new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				for (int i = 1; i <= 50; i++)
				{
					kadRouting.peerFound(new PeerAddress(new Number160(i + 1)), null);
				}
			}
		}).start();
		new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				for (int i = 1; i <= 100; i++)
				{
					kadRouting.peerFound(new PeerAddress(new Number160(i + 1)), null);
				}
			}
		}).start();
		Utils.sleep(500);
		Assert.assertEquals(100, kadRouting.size());
	}

	@Test
	public void testRandomAddRemove()
	{
		final PeerMapKadImpl kadRouting = new PeerMapKadImpl(new Number160("0x1"), 2, 100,
				60 * 1000, 3, new int[] {});
		new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				for (int i = 1; i <= 5000; i++)
				{
					kadRouting.peerFound(new PeerAddress(new Number160(i + 1)), null);
					if (i - 10 > 1)
						kadRouting.peerOffline(new PeerAddress(new Number160(i - 10)), true);
				}
			}
		}).start();
		new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				for (int i = 1; i <= 5000; i++)
				{
					kadRouting.peerFound(new PeerAddress(new Number160(i + 1)), null);
					if (i - 10 > 1)
						kadRouting.peerOffline(new PeerAddress(new Number160(i - 10)), true);
				}
			}
		}).start();
		Utils.sleep(2000);
		Assert.assertEquals(11, kadRouting.size());
	}

	@Test
	public void testPerformance() throws IOException
	{
		final PeerMapKadImpl kadRouting = new PeerMapKadImpl(new Number160("0x1"), 2, 100,
				60 * 1000, 3, new int[] {});
		final Random random = new Random();
		final List<PeerAddress> listAdded = new ArrayList<PeerAddress>();
		final List<PeerAddress> listRemoved = new ArrayList<PeerAddress>();
		long start = System.currentTimeMillis();
		int size = 500000;
		for (int i = 1; i <= size; i++)
		{
			PeerAddress r1 = new PeerAddress(new Number160(random));
			listAdded.add(r1);
		}
		for (PeerAddress r1 : listAdded)
			kadRouting.peerFound(r1, null);
		for (PeerAddress r1 : listAdded)
			kadRouting.peerFound(r1, null);
		for (int i = 0; i < 100; i++)
		{
			PeerAddress removed = listAdded.get(random.nextInt(i + 1));
			if (kadRouting.peerOffline(removed, true))
				listRemoved.add(removed);
		}
		for (PeerAddress r1 : listAdded)
			kadRouting.peerFound(r1, r1);
		Assert.assertEquals(320, kadRouting.size());
		for (PeerAddress r1 : listRemoved)
			Assert.assertTrue(kadRouting.isPeerRemovedTemporarly(r1));
		//
		for (PeerAddress r1 : listRemoved)
			listAdded.remove(r1);
		for (int i = 0; i < 300; i++)
		{
			PeerAddress removed = listAdded.get(random.nextInt(i + 1));
			kadRouting.peerOffline(removed, true);
		}
		for (PeerAddress r1 : listRemoved)
			Assert.assertTrue(!kadRouting.isPeerRemovedTemporarly(r1));
		System.err.println("Time used: " + (System.currentTimeMillis() - start)
				+ " ms. (time to beat ~5100ms)");
	}

	@Test
	public void testClose() throws UnknownHostException
	{
		Random rnd = new Random(43L);
		for (int j = 0; j < 10000; j++)
		{
			PeerMapKadImpl kadRouting = new PeerMapKadImpl(new Number160(rnd), 5, 100, 60 * 1000,
					3, new int[] {});
			List<PeerAddress> peers = new ArrayList<PeerAddress>();
			for (int i = 1; i < 160 * 5; i++)
			{
				PeerAddress r1 = new PeerAddress(new Number160(rnd));
				peers.add(r1);
				kadRouting.peerFound(r1, null);
			}
			Number160 key = new Number160(rnd);
			TreeSet<PeerAddress> set = new TreeSet<PeerAddress>(kadRouting
					.createPeerComparator(key));
			set.addAll(peers);
			PeerAddress closest1 = set.iterator().next();
			PeerAddress closest2 = kadRouting.closePeers(key, 1).iterator().next();
			Assert.assertEquals(closest1, closest2);
		}
	}
}
