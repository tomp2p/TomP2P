package net.tomp2p.p2p;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import net.tomp2p.Utils2;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureLateJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.config.Configurations;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMapKadImpl;
import net.tomp2p.utils.Utils;

import org.junit.Assert;
import org.junit.Test;

public class TestRouting
{
	final private static Random rnd = new Random(43L);

	@Test
	public void testDifference() throws UnknownHostException
	{
		// setup
		PeerMapKadImpl test = new PeerMapKadImpl(new Number160(77), 2, 60 * 1000, 3, new int[0], 100, false);
		
		Collection<PeerAddress> newC = new ArrayList<PeerAddress>();
		newC.add(Utils2.createAddress(12));
		newC.add(Utils2.createAddress(15));
		newC.add(Utils2.createAddress(88));
		newC.add(Utils2.createAddress(90));
		newC.add(Utils2.createAddress(91));
		SortedSet<PeerAddress> result = new TreeSet<PeerAddress>(test
				.createPeerComparator(new Number160(88)));
		SortedSet<PeerAddress> already = new TreeSet<PeerAddress>(test
				.createPeerComparator(new Number160(88)));
		already.add(Utils2.createAddress(90));
		already.add(Utils2.createAddress(15));
		// do testing
		Utils.difference(newC, result, already);
		// verification
		Assert.assertEquals(3, result.size());
		Assert.assertEquals(Utils2.createAddress(88), result.first());
	}

	@Test
	public void testMerge() throws UnknownHostException
	{
		// setup
		PeerMapKadImpl test = new PeerMapKadImpl(new Number160(77), 2, 60 * 1000, 3, new int[0], 100, false);
		
		SortedSet<PeerAddress> queue = new TreeSet<PeerAddress>(test
				.createPeerComparator(new Number160(88)));
		SortedSet<PeerAddress> neighbors = new TreeSet<PeerAddress>(test
				.createPeerComparator(new Number160(88)));
		SortedSet<PeerAddress> already = new TreeSet<PeerAddress>(test
				.createPeerComparator(new Number160(88)));
		queue.add(Utils2.createAddress(12));
		queue.add(Utils2.createAddress(14));
		queue.add(Utils2.createAddress(16));

		neighbors.add(Utils2.createAddress(88));
		neighbors.add(Utils2.createAddress(12));
		neighbors.add(Utils2.createAddress(16));
		// do testing and verification
		already.add(Utils2.createAddress(16));
		boolean testb = DistributedRouting.merge(queue, neighbors, already);
		Assert.assertEquals(true, testb);
		// next one
		neighbors.add(Utils2.createAddress(89));
		testb = DistributedRouting.merge(queue, neighbors, already);
		Assert.assertEquals(false, testb);
		// next one
		neighbors.add(Utils2.createAddress(88));
		testb = DistributedRouting.merge(queue, neighbors, already);
		Assert.assertEquals(false, testb);
	}

	@Test
	public void testEvaluate() throws UnknownHostException
	{
		// setup
		PeerMapKadImpl test = new PeerMapKadImpl(new Number160(77), 2, 60 * 1000, 3, new int[0], 100, false);
		
		SortedSet<PeerAddress> queue = new TreeSet<PeerAddress>(test
				.createPeerComparator(new Number160(88)));
		SortedSet<PeerAddress> neighbors = new TreeSet<PeerAddress>(test
				.createPeerComparator(new Number160(88)));
		SortedSet<PeerAddress> already = new TreeSet<PeerAddress>(test
				.createPeerComparator(new Number160(88)));

		queue.add(Utils2.createAddress(12));
		queue.add(Utils2.createAddress(14));
		queue.add(Utils2.createAddress(16));

		neighbors.add(Utils2.createAddress(89));
		neighbors.add(Utils2.createAddress(12));
		neighbors.add(Utils2.createAddress(16));

		already.add(Utils2.createAddress(16));
		// do testing and verification
		AtomicInteger nrNoNewInformation = new AtomicInteger();
		boolean testb = DistributedRouting.evaluateInformation(neighbors, queue, already, nrNoNewInformation,
				0);
		Assert.assertEquals(0, nrNoNewInformation.get());
		Assert.assertEquals(false, testb);
		testb = DistributedRouting.evaluateInformation(neighbors, queue, already, nrNoNewInformation, 2);
		Assert.assertEquals(1, nrNoNewInformation.get());
		Assert.assertEquals(false, testb);
		neighbors.add(Utils2.createAddress(11));
		testb = DistributedRouting.evaluateInformation(neighbors, queue, already, nrNoNewInformation, 2);
		Assert.assertEquals(2, nrNoNewInformation.get());
		Assert.assertEquals(true, testb);
		neighbors.add(Utils2.createAddress(88));
		testb = DistributedRouting.evaluateInformation(neighbors, queue, already, nrNoNewInformation, 2);
		Assert.assertEquals(0, nrNoNewInformation.get());
		Assert.assertEquals(false, testb);
		//
		testb = DistributedRouting.evaluateInformation(neighbors, queue, already, nrNoNewInformation, 2);
		Assert.assertEquals(1, nrNoNewInformation.get());
		neighbors.add(Utils2.createAddress(89));
		testb = DistributedRouting.evaluateInformation(neighbors, queue, already, nrNoNewInformation, 2);
		Assert.assertEquals(2, nrNoNewInformation.get());
		neighbors.add(Utils2.createAddress(88));
		testb = DistributedRouting.evaluateInformation(neighbors, queue, already, nrNoNewInformation, 2);
		Assert.assertEquals(3, nrNoNewInformation.get());
		Assert.assertEquals(true, testb);
	}

	@Test
	public void testRouting1TCP() throws Exception
	{
		Peer[] peers = null;
		try
		{
			// setup
			peers = createSpecialPeers(7);
			addToPeerMap(peers[0], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[1], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress());
			addToPeerMap(peers[2], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress());
			addToPeerMap(peers[3], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress(), peers[4].getPeerAddress());
			addToPeerMap(peers[4], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress(), peers[4].getPeerAddress(),
					peers[5].getPeerAddress());
			// do testing
			FutureChannelCreator fcc=peers[0].getConnectionBean().getConnectionReservation().reserve(2);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureRouting fr = peers[0].getRouting().route(peers[6].getPeerID(), null, null,
					Type.REQUEST_2, 0, 0, 0, 100, 2, false, cc);
			Utils.addReleaseListenerAll(fr, peers[0].getConnectionBean().getConnectionReservation(), cc);
			fr.awaitUninterruptibly();
			// do verification
			Assert.assertEquals(true, fr.isSuccess());
			SortedSet<PeerAddress> ns = fr.getPotentialHits();
			Assert.assertEquals(peers[5].getPeerAddress(), ns.first());
		}
		finally
		{
			for (Peer n : peers)
				n.shutdown();
		}
	}

	@Test
	public void testRouting2TCP() throws Exception
	{
		Peer[] peers = null;
		try
		{
			// setup
			peers = createSpecialPeers(7);
			addToPeerMap(peers[0], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[1], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress());
			addToPeerMap(peers[2], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress());
			addToPeerMap(peers[3], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress(), peers[4].getPeerAddress());
			addToPeerMap(peers[4], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress(), peers[4].getPeerAddress(),
					peers[5].getPeerAddress());
			// do testing
			FutureChannelCreator fcc=peers[0].getConnectionBean().getConnectionReservation().reserve(2);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureRouting fr = peers[0].getRouting().route(peers[6].getPeerID(), null, null,
					Type.REQUEST_2, 0, 0, 0, 100, 2, false, cc);
			Utils.addReleaseListenerAll(fr, peers[0].getConnectionBean().getConnectionReservation(), cc);
			fr.awaitUninterruptibly();
			// do verification
			Assert.assertEquals(true, fr.isSuccess());
			SortedSet<PeerAddress> ns = fr.getPotentialHits();
			Assert.assertEquals(peers[5].getPeerAddress(), ns.first());
		}
		finally
		{
			for (Peer n : peers)
				n.shutdown();
		}
	}

	@Test
	public void testRouting1UDP() throws Exception
	{
		Peer[] peers = null;
		try
		{
			// setup
			peers = createSpecialPeers(7);
			addToPeerMap(peers[0], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[1], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress());
			addToPeerMap(peers[2], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress());
			addToPeerMap(peers[3], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress(), peers[4].getPeerAddress());
			addToPeerMap(peers[4], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress(), peers[4].getPeerAddress(),
					peers[5].getPeerAddress());
			// do testing
			FutureChannelCreator fcc=peers[0].getConnectionBean().getConnectionReservation().reserve(2);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureRouting fr = peers[0].getRouting().route(peers[6].getPeerID(), null, null,
					Type.REQUEST_1, 0, 0, 0, 100, 2, false, cc);
			Utils.addReleaseListenerAll(fr, peers[0].getConnectionBean().getConnectionReservation(), cc);
			fr.awaitUninterruptibly();
			// do verification
			Assert.assertEquals(true, fr.isSuccess());
			SortedSet<PeerAddress> ns = fr.getPotentialHits();
			Assert.assertEquals(peers[5].getPeerAddress(), ns.first());
		}
		finally
		{
			for (Peer n : peers)
				n.shutdown();
		}
	}

	@Test
	public void testRouting2UDP() throws Exception
	{
		Peer[] peers = null;
		try
		{
			// setup
			peers = createSpecialPeers(7);
			addToPeerMap(peers[0], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[1], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress());
			addToPeerMap(peers[2], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress());
			addToPeerMap(peers[3], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress(), peers[4].getPeerAddress());
			addToPeerMap(peers[4], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress(), peers[4].getPeerAddress(),
					peers[5].getPeerAddress());
			// do testing
			FutureChannelCreator fcc=peers[0].getConnectionBean().getConnectionReservation().reserve(2);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureRouting fr = peers[0].getRouting().route(peers[6].getPeerID(), null, null,
					Type.REQUEST_1, 0, 0, 0, 100, 2, false, cc);
			Utils.addReleaseListenerAll(fr, peers[0].getConnectionBean().getConnectionReservation(), cc);
			fr.awaitUninterruptibly();
			// do verification
			Assert.assertEquals(true, fr.isSuccess());
			SortedSet<PeerAddress> ns = fr.getPotentialHits();
			Assert.assertEquals(peers[5].getPeerAddress(), ns.first());
		}
		finally
		{
			for (Peer n : peers)
				n.shutdown();
		}
	}

	@Test
	public void testRouting2() throws Exception
	{
		Peer[] peers = null;
		try
		{
			// setup
			peers = createSpecialPeers(7);
			addToPeerMap(peers[0], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[1], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress());
			addToPeerMap(peers[2], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress());
			addToPeerMap(peers[3], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress(), peers[4].getPeerAddress());
			addToPeerMap(peers[4], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress(), peers[4].getPeerAddress(), Utils2
					.createAddress("0xffffff"));
			// do testing
			FutureChannelCreator fcc=peers[0].getConnectionBean().getConnectionReservation().reserve(2);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureRouting fr = peers[0].getRouting().route(peers[6].getPeerID(), null, null,
					Type.REQUEST_2, 0, 0, 0, 100, 2, false, cc);
			Utils.addReleaseListenerAll(fr, peers[0].getConnectionBean().getConnectionReservation(), cc);
			fr.awaitUninterruptibly();
			// do verification
			Assert.assertEquals(true, fr.isSuccess());
			SortedSet<PeerAddress> ns = fr.getPotentialHits();
			// node5 cannot be reached, so it should not be part of the result
			Assert.assertEquals(false, peers[5].getPeerAddress().equals(ns.first()));
			Assert.assertEquals(true, peers[4].getPeerAddress().equals(ns.first()));
		}
		finally
		{
			for (Peer n : peers)
				n.shutdown();
		}
	}

	@Test
	public void testRouting2_detailed() throws Exception
	{
		Peer[] peers = null;
		try
		{
			// setup
			peers = createSpecialPeers(7);
			addToPeerMap(peers[0], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[1], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress());
			addToPeerMap(peers[2], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress());
			addToPeerMap(peers[3], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress(), peers[4].getPeerAddress());
			addToPeerMap(peers[4], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress(), peers[4].getPeerAddress(), Utils2
					.createAddress("0xffffff"));
			// do testing
			FutureChannelCreator fcc=peers[0].getConnectionBean().getConnectionReservation().reserve(2);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureRouting fr = peers[0].getRouting().route(peers[6].getPeerID(), null, null,
					Type.REQUEST_2, 0, 0, 0, 100, 2, false, cc);
			Utils.addReleaseListenerAll(fr, peers[0].getConnectionBean().getConnectionReservation(), cc);
			fr.awaitUninterruptibly();
			// do verification
			Assert.assertEquals(true, fr.isSuccess());
			SortedSet<PeerAddress> ns = fr.getPotentialHits();
			// node5 cannot be reached, so it should not be part of the result
			Assert.assertEquals(false, peers[5].getPeerAddress().equals(ns.first()));
			Assert.assertEquals(true, peers[4].getPeerAddress().equals(ns.first()));
		}
		finally
		{
			for (Peer n : peers)
				n.shutdown();
		}
	}

	@Test
	public void testRouting3() throws Exception
	{
		Peer[] peers = null;
		try
		{
			// setup
			peers = createSpecialPeers(7);
			addToPeerMap(peers[0], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[1], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress());
			addToPeerMap(peers[2], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress(), peers[4].getPeerAddress(),
					peers[5].getPeerAddress());
			addToPeerMap(peers[3], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[4], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[5],  peers[0].getPeerAddress(), peers[1].getPeerAddress());
			// do testing
			FutureChannelCreator fcc=peers[0].getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureRouting fr = peers[0].getRouting().route(peers[6].getPeerID(), null, null,
					Type.REQUEST_2, 0, 0, 0, 100, 1, false, cc);
			Utils.addReleaseListenerAll(fr, peers[0].getConnectionBean().getConnectionReservation(), cc);
			fr.awaitUninterruptibly();
			// do verification
			Assert.assertEquals(true, fr.isSuccess());
			SortedSet<PeerAddress> ns = fr.getPotentialHits();
			Assert.assertEquals(peers[5].getPeerAddress(), ns.first());
			Assert.assertEquals(false, ns.contains(peers[3].getPeerAddress()));
			Assert.assertEquals(false, ns.contains(peers[4].getPeerAddress()));
		}
		finally
		{
			for (Peer n : peers)
				n.shutdown();
		}
	}

	@Test
	public void testRouting3_detailed() throws Exception
	{
		Peer[] peers = null;
		try
		{
			// setup
			peers = createSpecialPeers(7);
			addToPeerMap(peers[0], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[1], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress());
			addToPeerMap(peers[2], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress(), peers[4].getPeerAddress(),
					peers[5].getPeerAddress());
			addToPeerMap(peers[3], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[4], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[5], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			// do testing
			FutureChannelCreator fcc=peers[0].getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureRouting fr = peers[0].getRouting().route(peers[6].getPeerID(), null, null,
					Type.REQUEST_2, 0, 0, 0, 100, 1, false, cc);
			Utils.addReleaseListenerAll(fr, peers[0].getConnectionBean().getConnectionReservation(), cc);
			fr.awaitUninterruptibly();
			// do verification
			Assert.assertEquals(true, fr.isSuccess());
			SortedSet<PeerAddress> ns = fr.getPotentialHits();
			Assert.assertEquals(peers[5].getPeerAddress(), ns.first());
			Assert.assertEquals(false, ns.contains(peers[3].getPeerAddress()));
			Assert.assertEquals(false, ns.contains(peers[4].getPeerAddress()));
		}
		finally
		{
			for (Peer n : peers)
				n.shutdown();
		}
	}

	@Test
	public void testRouting4() throws Exception
	{
		Peer[] peers = null;
		try
		{
			// setup
			peers = createSpecialPeers(7);
			addToPeerMap(peers[0], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[1], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress());
			addToPeerMap(peers[2], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress(), peers[4].getPeerAddress(),
					peers[5].getPeerAddress());
			addToPeerMap(peers[3], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[4], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[5], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			// do testing
			FutureChannelCreator fcc=peers[0].getConnectionBean().getConnectionReservation().reserve(2);
fcc.awaitUninterruptibly();
final ChannelCreator cc = fcc.getChannelCreator();
			FutureRouting fr = peers[0].getRouting().route(peers[6].getPeerID(), null, null,
					Type.REQUEST_1, 0, 0, 0, 100, 2, false, cc);
			Utils.addReleaseListenerAll(fr, peers[0].getConnectionBean().getConnectionReservation(), cc);
			fr.awaitUninterruptibly();
			// do verification
			Assert.assertEquals(true, fr.isSuccess());
			SortedSet<PeerAddress> ns = fr.getPotentialHits();
			Assert.assertEquals(peers[5].getPeerAddress(), ns.first());
			Assert.assertEquals(true, ns.contains(peers[0].getPeerAddress()));
			Assert.assertEquals(true, ns.contains(peers[1].getPeerAddress()));
			Assert.assertEquals(true, ns.contains(peers[2].getPeerAddress()));
			Assert.assertEquals(false, ns.contains(peers[3].getPeerAddress()));
			Assert.assertEquals(true, ns.contains(peers[4].getPeerAddress()));
		}
		finally
		{
			for (Peer n : peers)
				n.shutdown();
		}
	}

	@Test
	public void testRouting5() throws Exception
	{
		Peer[] peers = null;
		try
		{
			// setup
			peers = createSpecialPeers(7);
			addToPeerMap(peers[0], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[1], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress());
			addToPeerMap(peers[2], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress(), peers[4].getPeerAddress(),
					peers[5].getPeerAddress());
			addToPeerMap(peers[3], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[4], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[5], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			// do testing
			FutureChannelCreator fcc=peers[0].getConnectionBean().getConnectionReservation().reserve(3);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureRouting fr = peers[0].getRouting().route(peers[6].getPeerID(), null, null,
					Type.REQUEST_1, 0, 0, 0, 100, 3, false, cc);
			Utils.addReleaseListenerAll(fr, peers[0].getConnectionBean().getConnectionReservation(), cc);
			fr.awaitUninterruptibly();
			// do verification
			Assert.assertEquals(true, fr.isSuccess());
			SortedSet<PeerAddress> ns = fr.getPotentialHits();
			Assert.assertEquals(peers[5].getPeerAddress(), ns.first());
			Assert.assertEquals(true, ns.contains(peers[3].getPeerAddress()));
			Assert.assertEquals(true, ns.contains(peers[4].getPeerAddress()));
		}
		finally
		{
			for (Peer n : peers)
				n.shutdown();
		}
	}

	@Test
	public void testRouting6() throws Exception
	{
		Peer[] peers = null;
		try
		{
			// setup
			peers = createSpecialPeers(7);
			addToPeerMap(peers[0], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[1], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress());
			addToPeerMap(peers[2], peers[0].getPeerAddress(), peers[1].getPeerAddress(), peers[2]
					.getPeerAddress(), peers[3].getPeerAddress(), peers[4].getPeerAddress(),
					peers[5].getPeerAddress());
			addToPeerMap(peers[3], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[4], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			addToPeerMap(peers[5], peers[0].getPeerAddress(), peers[1].getPeerAddress());
			// do testing
			FutureChannelCreator fcc=peers[0].getConnectionBean().getConnectionReservation().reserve(3);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureRouting fr = peers[0].getRouting().route(peers[6].getPeerID(), null, null,
					Type.REQUEST_1, 0, 0, 0, 100, 3, false, cc);
			Utils.addReleaseListenerAll(fr, peers[0].getConnectionBean().getConnectionReservation(), cc);
			fr.awaitUninterruptibly();
			// do verification
			Assert.assertEquals(true, fr.isSuccess());
			SortedSet<PeerAddress> ns = fr.getPotentialHits();
			System.err.println(ns.size());
			Assert.assertEquals(6, ns.size());
		}
		finally
		{
			for (Peer n : peers)
				n.shutdown();
		}
	}

	/**
	 * Adds peers to a peer's map.
	 * @param peer The peer to which the peers will be added
	 * @param peers The peers that will be added
	 */
	private void addToPeerMap(Peer peer, PeerAddress... peers)
	{
		for (int i = 0; i < peers.length; i++)
		{
			peer.getPeerBean().getPeerMap().peerFound(peers[i], null);
		}
	}

	private Peer[] createSpecialPeers(int nr) throws Exception
	{
		StringBuilder sb = new StringBuilder("0x");
		Peer[] peers = new Peer[nr];
		for (int i = 0; i < nr; i++)
		{
			sb.append("f");
			peers[i] = new PeerMaker(new Number160(sb.toString())).setPorts(4001+i).buildAndListen();
		}
		return peers;
	}
	
	@Test
	public void testPerfectRouting() throws Exception
	{
		final Random rnd = new Random(42L);
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(1000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			Collection<PeerAddress> pas = peers[30].getPeerBean().getPeerMap().closePeers(
					peers[30].getPeerID(), 20);
			Iterator<PeerAddress> i = pas.iterator();
			PeerAddress p1 = i.next();
			Assert.assertEquals(peers[262].getPeerAddress(), p1);
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testRoutingBulkTCP() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			FutureChannelCreator fcc=peers[500].getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureRouting fr = peers[500].getRouting().route(peers[20].getPeerID(), null, null,
					Type.REQUEST_2, 0, 0, 0, 100, 1, false, cc);
			Utils.addReleaseListenerAll(fr, peers[500].getConnectionBean().getConnectionReservation(), cc);
			fr.awaitUninterruptibly();
			// do verification
			Assert.assertEquals(true, fr.isSuccess());
			SortedSet<PeerAddress> ns = fr.getPotentialHits();
			Assert.assertEquals(peers[20].getPeerAddress(), ns.first());
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testRoutingBulkUDP() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			FutureChannelCreator fcc=peers[500].getConnectionBean().getConnectionReservation().reserve(1);
			fcc.awaitUninterruptibly();
			final ChannelCreator cc = fcc.getChannelCreator();
			FutureRouting fr = peers[500].getRouting().route(peers[20].getPeerID(), null, null,
					Type.REQUEST_1, 0, 0, 0, 100, 1, false, cc);
			Utils.addReleaseListenerAll(fr, peers[500].getConnectionBean().getConnectionReservation(), cc);
			fr.awaitUninterruptibly();
			// do verification
			Assert.assertEquals(true, fr.isSuccess());
			SortedSet<PeerAddress> ns = fr.getPotentialHits();
			Assert.assertEquals(peers[20].getPeerAddress(), ns.first());
		}
		finally
		{
			master.shutdown();
		}
	}
	

	@Test
	public void testRoutingConcurrentlyTCP() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			System.err.println("do routing.");
			List<FutureRouting> frs = new ArrayList<FutureRouting>();
			for (int i = 0; i < peers.length; i++)
			{
				FutureChannelCreator fcc=peers[0].getConnectionBean().getConnectionReservation().reserve(1);
				fcc.awaitUninterruptibly();
				final ChannelCreator cc = fcc.getChannelCreator();
				FutureRouting frr = peers[((i * 7777) + 1) % peers.length].getRouting().route(
						peers[((i * 3333) + 1) % peers.length].getPeerID(), null, null,
						Type.REQUEST_2, 0, 0, 0, 100, 1, false, cc);
				Utils.addReleaseListener(frr, peers[0].getConnectionBean().getConnectionReservation(), cc, 1);
				frs.add(frr);
			}
			System.err.println("now checking if the tests were successful.");
			for (int i = 0; i < peers.length; i++)
			{
				frs.get(i).awaitUninterruptibly();
				Assert.assertEquals(true, frs.get(i).isSuccess());
				SortedSet<PeerAddress> ns = frs.get(i).getPotentialHits();
				Assert.assertEquals(peers[((i * 3333) + 1) % peers.length].getPeerAddress(), ns
						.first());
			}
			System.err.println("done!");
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testRoutingConcurrentlyTCP2() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			System.err.println("do routing.");
			Map<Integer, FutureRouting> frs = new HashMap<Integer, FutureRouting>();
			for (int i = 0; i < peers.length; i++)
			{
				FutureChannelCreator fcc=peers[0].getConnectionBean().getConnectionReservation().reserve(2);
				fcc.awaitUninterruptibly();
				final ChannelCreator cc = fcc.getChannelCreator();
				FutureRouting frr = peers[((i * 7777) + 1) % peers.length].getRouting().route(
						peers[((i * 3333) + 1) % peers.length].getPeerID(), null, null,
						Type.REQUEST_2, 0, 5, 0, 100, 2, false, cc);
				Utils.addReleaseListener(frr, peers[0].getConnectionBean().getConnectionReservation(), cc, 2);
				frs.put(i, frr);
			}
			System.err.println("now checking if the tests were successful.");
			for (int i = 0; i < peers.length; i++)
			{
				System.err.println(i);
				frs.get(i).awaitUninterruptibly();
				Assert.assertEquals(true, frs.get(i).isSuccess());
				SortedSet<PeerAddress> ns = frs.get(i).getPotentialHits();
				Assert.assertEquals(peers[((i * 3333) + 1) % peers.length].getPeerAddress(), ns
						.first());
			}
			System.err.println("done!");
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testRoutingConcurrentlyUDP() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			System.err.println("do routing.");
			List<FutureRouting> frs = new ArrayList<FutureRouting>();
			for (int i = 0; i < peers.length; i++)
			{
				FutureChannelCreator fcc=peers[0].getConnectionBean().getConnectionReservation().reserve(1);
				fcc.awaitUninterruptibly();
				final ChannelCreator cc = fcc.getChannelCreator();
				FutureRouting frr = peers[((i * 7777) + 1) % peers.length].getRouting().route(
						peers[((i * 3333) + 1) % peers.length].getPeerID(), null, null,
						Type.REQUEST_1, 0, 0, 0, 100, 1, false, cc);
				Utils.addReleaseListener(frr, peers[0].getConnectionBean().getConnectionReservation(), cc, 1);
				frs.add(frr);
			}
			System.err.println("now checking if the tests were successful.");
			for (int i = 0; i < peers.length; i++)
			{
				frs.get(i).awaitUninterruptibly();
				Assert.assertEquals(true, frs.get(i).isSuccess());
				SortedSet<PeerAddress> ns = frs.get(i).getPotentialHits();
				Assert.assertEquals(peers[((i * 3333) + 1) % peers.length].getPeerAddress(), ns
						.first());
			}
			System.err.println("done!");
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testRoutingConcurrentlyUDP2() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(2000, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			System.err.println("do routing.");
			List<FutureRouting> frs = new ArrayList<FutureRouting>();
			for (int i = 0; i < peers.length; i++)
			{
				int peerNr=((i * 7777) + 1) % peers.length;
				FutureChannelCreator fcc=peers[peerNr].getConnectionBean().getConnectionReservation().reserve(2);
				fcc.awaitUninterruptibly();
				final ChannelCreator cc = fcc.getChannelCreator();
				FutureRouting frr = peers[peerNr].getRouting().route(
						peers[((i * 3333) + 1) % peers.length].getPeerID(), null, null,
						Type.REQUEST_1, 0, 1, 0, 100, 2, false, cc);
				Utils.addReleaseListener(frr, peers[peerNr].getConnectionBean().getConnectionReservation(), cc, 2);
				frs.add(frr);
			}
			System.err.println("now checking if the tests were successful.");
			for (int i = 0; i < peers.length; i++)
			{
				frs.get(i).awaitUninterruptibly();
				Assert.assertEquals(true, frs.get(i).isSuccess());
				SortedSet<PeerAddress> ns = frs.get(i).getPotentialHits();
				Assert.assertEquals(peers[((i * 3333) + 1) % peers.length].getPeerAddress(), ns
						.first());
			}
			System.err.println("done! ");
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testRoutingBootstrap1() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(200, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			Collection<PeerAddress> peerAddresses = new ArrayList<PeerAddress>(1);
			peerAddresses.add(master.getPeerAddress());
			for (int i = 1; i < peers.length; i++)
			{
				FutureChannelCreator fcc=peers[i].getConnectionBean().getConnectionReservation().reserve(1);
				fcc.awaitUninterruptibly();
				final ChannelCreator cc = fcc.getChannelCreator();
				FutureRouting fm = peers[i].getRouting()
						.bootstrap(peerAddresses, 5, 100, 100, 1, true, false, cc);
				Utils.addReleaseListenerAll(fm, peers[i].getConnectionBean().getConnectionReservation(), cc);
				fm.awaitUninterruptibly();
				// do verification
				Assert.assertEquals(true, fm.isSuccess());
			}
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testRoutingBootstrap2() throws Exception
	{
		Peer master = null;
		try
		{
			// setup
			Peer[] peers = Utils2.createNodes(200, rnd, 4001);
			master = peers[0];
			Utils2.perfectRouting(peers);
			// do testing
			for (int i = 1; i < peers.length; i++)
			{
				Collection<PeerAddress> peerAddresses = new ArrayList<PeerAddress>(1);
				peerAddresses.add(peers[0].getPeerAddress());
				FutureChannelCreator fcc=peers[i].getConnectionBean().getConnectionReservation().reserve(1);
				fcc.awaitUninterruptibly();
				final ChannelCreator cc = fcc.getChannelCreator();
				FutureRouting fm = peers[i].getRouting().bootstrap(peerAddresses, 5, 100, 100, 1,
						false, false, cc);
				Utils.addReleaseListenerAll(fm, peers[i].getConnectionBean().getConnectionReservation(), cc);
				fm.awaitUninterruptibly();
				// do verification
				Assert.assertEquals(true, fm.isSuccess());
			}
		}
		finally
		{
			master.shutdown();
		}
	}

	@Test
	public void testBootstrap() throws Exception
	{
		Peer master = null; 
		Peer client = null;
		try
		{
			master = new PeerMaker(new Number160(rnd)).setPorts(4000).buildAndListen(); 
			client = new PeerMaker(new Number160(rnd)).setPorts(4001).buildAndListen(); 
			FutureLateJoin<FutureResponse> tmp = client.pingBroadcast(4000);
			tmp.awaitUninterruptibly();
			Assert.assertEquals(true, tmp.isSuccess());
			Assert.assertEquals(1, client.getPeerBean().getPeerMap().size());
		}
		finally
		{
			client.shutdown();
			master.shutdown();
		}
	}

	@Test
	public void testBootstrap2() throws Exception
	{
		Peer master = null;
		Peer client = null;
		try
		{
			master = new PeerMaker(new Number160(rnd)).setPorts(4002).buildAndListen(); 
			client = new PeerMaker(new Number160(rnd)).setPorts(4001).buildAndListen(); 
			FutureLateJoin<FutureResponse> tmp = client.pingBroadcast(4001);
			tmp.awaitUninterruptibly();
			Assert.assertEquals(false, tmp.isSuccess());
			Assert.assertEquals(0, client.getPeerBean().getPeerMap().size());
		}
		finally
		{
			client.shutdown();
			master.shutdown();
		}
	}

	@Test
	public void testRoutingLoop() throws Exception
	{
		final Random rnd = new Random(43L);
		for(int k=0;k<100;k++)
		{
			Number160 find = Number160.createHash("findme");
			Peer master = null;
			try
			{
				System.err.println("round "+k);
				
				// setup
				Peer[] peers = Utils2.createNodes(200, rnd, 4001);
				master = peers[0];
				Utils2.perfectRouting(peers);
				Comparator<PeerAddress> cmp=peers[50].getPeerBean().getPeerMap().createPeerComparator(find);
				SortedSet<PeerAddress> ss = new TreeSet<PeerAddress>(cmp);
				for (int i = 0; i < peers.length; i++)
				{
					ss.add(peers[i].getPeerAddress());
				}
				// do testing
				FutureChannelCreator fcc=peers[0].getConnectionBean().getConnectionReservation().reserve(2);
				fcc.awaitUninterruptibly();
				final ChannelCreator cc = fcc.getChannelCreator();
				Configurations.defaultConfigurationDirect();
				FutureRouting frr = peers[50].getRouting().route(find, null, null,
						Type.REQUEST_1, Integer.MAX_VALUE, 5, 10, 20, 2, false, cc);
				frr.awaitUninterruptibly();
				Utils.addReleaseListenerAll(frr, peers[0].getConnectionBean().getConnectionReservation(), cc);
				SortedSet<PeerAddress> ss2=frr.getPotentialHits();
				// test the first 5 peers, because we set noNewInformation to 5, which means we find at least 5 entries.
				for(int i=0;i<5;i++)
				{
					PeerAddress pa = ss.first();
					PeerAddress pa2 = ss2.first();
					System.err.println("test "+pa+" - "+pa2);
					Assert.assertEquals(pa.getID(), pa2.getID());
					ss.remove(pa);
					ss2.remove(pa2);
				}
			}
			finally
			{
				master.shutdown();
			}
		}
	}
}