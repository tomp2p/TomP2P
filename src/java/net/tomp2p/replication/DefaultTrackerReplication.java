package net.tomp2p.replication;
import java.util.Map;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Statistics;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.TrackerRPC;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.Storage;
import net.tomp2p.storage.StorageRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultTrackerReplication implements ResponsibilityListener
{
	final private static Logger logger = LoggerFactory.getLogger(DefaultTrackerReplication.class);
	final private Storage storage;
	final private TrackerRPC trackerRPC;
	final private Map<BaseFuture, Long> pendingFutures;
	final private Statistics statistics;

	public DefaultTrackerReplication(final Storage storage, TrackerRPC trackerRPC,
			Map<BaseFuture, Long> pendingFutures, Statistics statistics)
	{
		this.storage = storage;
		this.trackerRPC = trackerRPC;
		this.pendingFutures = pendingFutures;
		this.statistics = statistics;
	}

	@Override
	public void otherResponsible(final Number160 locationKey, final PeerAddress other)
	{
		if (logger.isDebugEnabled())
			logger.debug("[tracker] Other peer " + other + " is responsible for " + locationKey);
		storage.iterateAndRun(locationKey, new StorageRunner()
		{
			@Override
			public void call(Number160 locationKey, Number160 domainKey, Number160 contentKey,
					Data data)
			{
				if (isPrimaryTracker(locationKey, trackerRPC.getPeerAddress()))
				{
					FutureResponse futureResponse = trackerRPC.addToTrackerReplication(other,
							locationKey, domainKey, data, false);
					//futureResponse.awaitUninterruptibly();
					pendingFutures.put(futureResponse, System.currentTimeMillis());
					if (logger.isDebugEnabled())
						logger.debug("transfer from " + trackerRPC.getPeerAddress() + " to "
								+ other);
				}
				else
				{
					if (logger.isDebugEnabled())
						logger.debug("No transfer from " + trackerRPC.getPeerAddress() + " to "
								+ other + " becaues I'm a secondary tracker");
				}
			}
		});
	}

	private boolean isPrimaryTracker(Number160 locationKey, PeerAddress self)
	{
		double distance = self.getID().xor(locationKey).doubleValue();
		if (logger.isDebugEnabled())
			logger.debug("distance: "+distance+" avgGap*5:"+(statistics.getAvgGap() * 10));
		//return distance < (statistics.getAvgGap() * 10);
		return true;
	}

	@Override
	public void meResponsible(Number160 locationKey)
	{
		if (logger.isDebugEnabled())
			logger.debug("[tracker] I now responsible for " + locationKey);
		// we do not care, since the peer should repulish data on the
		// tracker.
	}
}
