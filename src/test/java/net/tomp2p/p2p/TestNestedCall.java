package net.tomp2p.p2p;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.p2p.config.ConfigurationGet;
import net.tomp2p.p2p.config.ConfigurationStore;
import net.tomp2p.p2p.config.Configurations;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNestedCall
{
	private Peer seed;
	private Peer peer;
	private ConfigurationGet cg;
	private ConfigurationStore cs;
	private final Random rnd = new SecureRandom();

	@Before
	public void setUp() throws Exception
	{
		final int seedPort = 5001;
		seed = new Peer(new Number160(rnd));
		try
		{
			seed.listen(seedPort, seedPort);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		peer = new Peer(new Number160(rnd));
		peer.listen(seed);
		final int maxSuccess = 10;
		final int parallel = 1;
		final int maxFailure = 3;
		final int parallelDiff = 0;
		RoutingConfiguration rc = new RoutingConfiguration(1, 0, maxSuccess, parallel);
		RequestP2PConfiguration pc = new RequestP2PConfiguration(2, maxFailure, parallelDiff);
		cg = Configurations.defaultGetConfiguration();
		cg.setRoutingConfiguration(rc);
		cg.setRequestP2PConfiguration(pc);
		cs = Configurations.defaultStoreConfiguration();
		cs.setRoutingConfiguration(rc);
		cs.setRequestP2PConfiguration(pc);
	}
	
	@After
	public void tearDown()
	{
		seed.shutdown();
		peer.shutdown();
	}

	@Test
	public void testSuccess() throws InterruptedException
	{
		final CountDownLatch latch = new CountDownLatch(2);
		final byte[] key = "foo".getBytes();
		final byte[] value = "bla".getBytes();
		final byte[] otherValue = "foobla".getBytes();
		final ChannelCreator channelCreator1=peer.reserve(cg);
		final ChannelCreator channelCreator2=peer.reserve(cs);
		final ChannelCreator channelCreator3=peer.reserve(cg);
		final ChannelCreator channelCreator4=peer.reserve(cs);
		putIfAbsent(key, value, 60, channelCreator1, channelCreator2, new Callback<Boolean>()
		{
			@Override
			public void onMessage(final Boolean result)
			{
				assertTrue("The put should succeed, given this key is new", result);
				latch.countDown();
				// try another put for the same key (that should fail)
				putIfAbsent(key, otherValue, 60, channelCreator3, channelCreator4, new Callback<Boolean>()
				{
					@Override
					public void onMessage(final Boolean res)
					{
						assertFalse(
								"The put should not have happened given a value already exists",
								res);
						latch.countDown();
					}
				});
			}
		});
		assertTrue("Tests did not complete in time", latch.await(500, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testFail() throws InterruptedException
	{
		final CountDownLatch latch = new CountDownLatch(2);
		final byte[] key = "foo".getBytes();
		final byte[] value = "bla".getBytes();
		final byte[] otherValue = "foobla".getBytes();
		putIfAbsent(key, value, 60, null, null, new Callback<Boolean>()
		{
			@Override
			public void onMessage(final Boolean result)
			{
				assertTrue("The put should succeed, given this key is new", result);
				latch.countDown();
				// try another put for the same key (that should fail)
				putIfAbsent(key, otherValue, 60, null, null, new Callback<Boolean>()
				{
					@Override
					public void onMessage(final Boolean res)
					{
						assertFalse(
								"The put should not have happened given a value already exists",
								res);
						latch.countDown();
					}
				});
			}
		});
		assertFalse("Tests did complete in time", latch.await(500, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testLimit() throws InterruptedException
	{
		final byte[] key = "foo".getBytes();
		final byte[] value = "bla".getBytes();
		final byte[] otherValue = "foobla".getBytes();
		cg.setAutomaticCleanup(true);
		cs.setAutomaticCleanup(true);
		for (int i = 0; i < 100000; i++)
		{
			final ChannelCreator channelCreator1=peer.reserve(cg, "cc1/"+i);
			final ChannelCreator channelCreator2=peer.reserve(cs, "cc2/"+i);
			final ChannelCreator channelCreator3=peer.reserve(cg, "cc3/"+i);
			final ChannelCreator channelCreator4=peer.reserve(cs, "cc4/"+i);
			putIfAbsent(key, value, 60, channelCreator1, channelCreator2, new Callback<Boolean>()
			{
				@Override
				public void onMessage(final Boolean result)
				{
					// try another put for the same key (that should fail)
					putIfAbsent(key, otherValue, 60, channelCreator3, channelCreator4, new Callback<Boolean>()
					{
						@Override
						public void onMessage(final Boolean res)
						{
							//do nothing
						}
					});
				}
			});
		}
		System.err.println("done");
	}

	public void putIfAbsent(final byte[] key, final byte[] value, final int timeout, final ChannelCreator channelCreator1,
			final ChannelCreator channelCreator2, final Callback<Boolean> callback)
	{
		final Number160 tomKey = new Number160(key);
		BaseFutureAdapter<BaseFuture> adapter = new BaseFutureAdapter<BaseFuture>()
		{
			@Override
			public void operationComplete(final BaseFuture result) throws Exception
			{
				if (result.isSuccess())
				{
					// abort put operation given that the key exists
					System.out.println("Put failed because a key exists");
					
					if(channelCreator2 != null)
					{
						//we dont query a second time
						peer.release(channelCreator2);
					}
					callback.onMessage(false);
				}
				else if (result.isFailed())
				{
					Data data = new Data(value);
					data.setTTLSeconds(timeout);
					BaseFutureListener<BaseFuture> listener = new BaseFutureListener<BaseFuture>()
					{
						@Override
						public void operationComplete(BaseFuture res) throws Exception
						{
							if (res.isSuccess())
							{
								System.out.println("Put if absent succeeded");
								callback.onMessage(true);
							}
							else if (res.isFailed())
							{
								System.out.println("Put if absent failed: "
										+ res.getFailedReason());
								callback.onMessage(false);
							}
						}

						@Override
						public void exceptionCaught(Throwable t) throws Exception
						{
							throw new IllegalStateException(t);
						}
					};
					if(channelCreator2 != null)
					{
						peer.put(tomKey, data, cs, channelCreator2).addListener(listener);
					}
					else
					{
						peer.put(tomKey, data, cs).addListener(listener);
					}
				}
			}
		};
		if(channelCreator1 != null)
		{
			peer.get(tomKey, cg, channelCreator1).addListener(adapter);
		}
		else
		{
			peer.get(tomKey, cg).addListener(adapter);
		}
	}
	public interface Callback<T>
	{
		void onMessage(T data);
	}
}
