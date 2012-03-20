package net.tomp2p.p2p;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.p2p.config.ConfigurationStore;
import net.tomp2p.p2p.config.Configurations;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestAddUpdateTTL
{

	private Random rnd = new Random(42L);
	private static final int DHT_UPDATE_INTERVAL = 1;
	private final ConcurrentMap<byte[], List<byte[]>> keyValueStore = new ConcurrentHashMap<byte[], List<byte[]>>();
	private Peer seed;
	private Peer peer;
	private Timer timer1;
	private Timer timer2;
	private CountDownLatch latch = new CountDownLatch(21); // wait for 21
															// results for
															// stopping the test
	private boolean testSuccess = true;

	@Before
	public void setup() throws Exception
	{
		keyValueStore.put("foo1".getBytes(),
				Arrays.asList("bla1a".getBytes(), "bla1b".getBytes()));
		keyValueStore.put("foo2".getBytes(), Arrays.asList("bla2a".getBytes()));
		keyValueStore.put(
				"foo3".getBytes(),
				Arrays.asList("bla3a".getBytes(), "bla3b".getBytes(),
						"bla3c".getBytes()));
		seed = new PeerMaker(new Number160(rnd)).setPorts(5002).buildAndListen();
	}

	@Test
	public void test() throws Exception
	{
		peer = createAndAttachRemotePeer();
		Thread.sleep(1000);
		for (byte[] key : keyValueStore.keySet())
		{
			for (byte[] value : keyValueStore.get(key))
			{
				add(peer, key, value);
			}
		}
		Thread.sleep(1000);
		timer1 = new Timer("DHT Update Service");
		timer1.schedule(new DhtUpdater(peer), Calendar
				.getInstance().getTime(), DHT_UPDATE_INTERVAL * 1000);

		timer2 = new Timer("DHT Stats Service");
		timer2.schedule(new DhtStats(peer), Calendar
				.getInstance().getTime(), 1000);

		latch.await(1, TimeUnit.MINUTES);
		Assert.assertTrue(testSuccess);
	}

	@After
	public void cleanup()
	{
		timer1.cancel();
		timer2.cancel();
		seed.shutdown();
		peer.shutdown();
	}

	public Peer createAndAttachRemotePeer()
	{
		final Peer peer;
		try
		{
			peer = new PeerMaker(new Number160(rnd)).setPorts(5003).buildAndListen();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			Assert.fail();
			return null;
		}
		final FutureBootstrap fb = peer.bootstrapBroadcast(seed
				.getPeerAddress().portTCP());
		fb.awaitUninterruptibly();
		peer.discover(fb.getBootstrapTo().iterator().next());
		fb.addListener(new BaseFutureListener<BaseFuture>()
		{
			@Override
			public void operationComplete(BaseFuture future) throws Exception
			{
				Collection<PeerAddress> addresses = fb.getBootstrapTo();
				if (addresses != null && !addresses.isEmpty())
				{
					peer.discover(addresses.iterator().next())
							.awaitUninterruptibly();
				}
				else
				{
					Assert.assertTrue(
							"Unable to boostrap to peers in the network", false);
				}
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception
			{
				t.fillInStackTrace();
			}
		});
		return peer;
	}

	private void add(Peer peer, byte[] key, byte[] value)
			throws InterruptedException
	{
		RoutingConfiguration rc = new RoutingConfiguration(1, 0, 10, 1);
		RequestP2PConfiguration pc = new RequestP2PConfiguration(3, 5, 0);
		ConfigurationStore cs = Configurations.defaultStoreConfiguration();
		cs.setRequestP2PConfiguration(pc);
		cs.setRoutingConfiguration(rc);

		Data data = new Data(value);
		data.setTTLSeconds(3);
		peer.add(new Number160(key), data, cs).addListener(
				new BaseFutureAdapter<FutureDHT>()
				{
					@Override
					public void operationComplete(final FutureDHT future)
							throws Exception
					{
						System.out.println(future.getRawKeys());
					}
				});
	}

	class DhtUpdater extends TimerTask
	{

		private Peer peer;

		public DhtUpdater(Peer peer)
		{
			this.peer = peer;
		}

		@Override
		public void run()
		{
			System.out.println("updating data");
			for (byte[] key : keyValueStore.keySet())
			{
				for (byte[] value : keyValueStore.get(key))
				{
					try
					{
						add(peer, key, value);
					}
					catch (InterruptedException e)
					{
						e.printStackTrace();
					}
				}
			}
		}
	}

	class DhtStats extends TimerTask
	{

		private Peer peer;

		public DhtStats(Peer peer)
		{
			this.peer = peer;
		}

		@Override
		public void run()
		{
			for (final byte[] key : keyValueStore.keySet())
			{
				peer.getAll(new Number160(key)).addListener(
						new BaseFutureAdapter<FutureDHT>()
						{
							@Override
							public void operationComplete(FutureDHT future)
									throws Exception
							{
								if (!future.isSuccess())
								{
									testSuccess = false;
									System.out.println("getAll failed " + future.getFailedReason());
								}
								else
								{
									System.out.println("getAll result: "
											+ future.getData().size());
								}
								latch.countDown();
							}
						});
			}
		}
	}

}