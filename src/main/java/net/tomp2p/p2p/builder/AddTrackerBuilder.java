package net.tomp2p.p2p.builder;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.hamcrest.core.IsSame;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureCreate;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.futures.FutureLateJoin;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.FutureTracker;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.config.ConfigurationTrackerStore;
import net.tomp2p.peers.Number160;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.TrackerStorage;

public class AddTrackerBuilder extends TrackerBuilder<AddTrackerBuilder>
{
	private byte[] attachement;
	private int trackerTimeoutSec = 60;
	private int pexWaitSec = 0;
	private SimpleBloomFilter<Number160> bloomFilter;
	private FutureCreate<BaseFuture> futureCreate;
	private boolean messageSign;
	public AddTrackerBuilder(Peer peer, Number160 locationKey)
	{
		super(peer, locationKey);
		self(this);
	}
	
	public FutureCreate<BaseFuture> getFutureCreate()
	{
		return futureCreate;
	}

	public AddTrackerBuilder setFutureCreate(FutureCreate<BaseFuture> futureCreate)
	{
		this.futureCreate = futureCreate;
		return this;
	}
	
	@Override
	public FutureTracker build()
	{
		preBuild("add-tracker-build");
		
		if(bloomFilter == null)
		{
			bloomFilter = new SimpleBloomFilter<Number160>(1024, 1024);
		}
		// add myself to my local tracker, since we use a mesh we are part of
		// the tracker mesh as well.
		peer.getPeerBean().getTrackerStorage().put(locationKey, domainKey, peer.getPeerAddress(),
				peer.getPeerBean().getKeyPair().getPublic(), attachement);
		final FutureTracker futureTracker = peer.getDistributedTracker().addToTracker(locationKey,
				domainKey,
				attachement, routingConfiguration,
				trackerConfiguration,
				messageSign, futureCreate, bloomFilter, futureChannelCreator, 
				peer.getConnectionBean().getConnectionReservation());
		
		if (trackerTimeoutSec > 0)
		{
			final ScheduledFuture<?> tmp = scheduleAddTracker(locationKey, config, futureTracker);
			setupCancel(futureTracker, tmp);
		}
		if (pexWaitSec > 0)
		{
			final ScheduledFuture<?> tmp = schedulePeerExchange(locationKey, config, futureTracker);
			setupCancel(futureTracker, tmp);
		}
		return futureTracker;
	
	}
	
	private ScheduledFuture<?> scheduleAddTracker(final Number160 locationKey,
			final ConfigurationTrackerStore config,
			final FutureTracker futureTracker)
	{
		Runnable runner = new Runnable()
		{
			@Override
			public void run()
			{
				// make a good guess based on the config and the maxium tracker
				// that can be found
				SimpleBloomFilter<Number160> bloomFilter = new SimpleBloomFilter<Number160>(
						BLOOMFILTER_SIZE, 1024);
				int conn = Math.max(config.getRoutingConfiguration().getParallel(), config
						.getTrackerConfiguration().getParallel());
				final FutureChannelCreator futureChannelCreator = getConnectionBean().getConnectionReservation().reserve(conn);

				FutureTracker futureTracker2 = getDistributedTracker().addToTracker(locationKey,
						config.getDomain(), config.getAttachement(),
						config.getRoutingConfiguration(), config.getTrackerConfiguration(),
						config.isSignMessage(), config.getFutureCreate(), bloomFilter,
						futureChannelCreator, getConnectionBean().getConnectionReservation());
				futureTracker.repeated(futureTracker2);
			}
		};
		int refresh = getPeerBean().getTrackerStorage().getTrackerTimoutSeconds() * 3 / 4;
		ScheduledFuture<?> tmp = getConnectionBean().getScheduler().getScheduledExecutorServiceReplication().scheduleAtFixedRate(runner,
				refresh, refresh,
				TimeUnit.SECONDS);
		scheduledFutures.add(tmp);
		return tmp;
	}	
	
	//TODO: enable this via AddBuilder
		public ScheduledFuture<?> schedulePeerExchange(final Number160 locationKey,
				final ConfigurationTrackerStore config, final FutureTracker futureTracker)
		{
			Runnable runner = new Runnable()
			{
				@Override
				public void run()
				{
					final FutureChannelCreator futureChannelCreator = getConnectionBean().getConnectionReservation().reserve(
							TrackerStorage.TRACKER_SIZE);
					FutureLateJoin<FutureResponse> futureLateJoin = getDistributedTracker().startPeerExchange(
							locationKey, config.getDomain(), futureChannelCreator, 
							getConnectionBean().getConnectionReservation(), config.getTrackerConfiguration().isForceTCP());
					futureTracker.repeated(futureLateJoin);
				}
			};
			int refresh = config.getWaitBeforeNextSendSeconds();
			ScheduledFuture<?> tmp = getConnectionBean().getScheduler().getScheduledExecutorServiceReplication().scheduleAtFixedRate(runner,
					refresh, refresh,
					TimeUnit.SECONDS);
			scheduledFutures.add(tmp);
			return tmp;
		}
}