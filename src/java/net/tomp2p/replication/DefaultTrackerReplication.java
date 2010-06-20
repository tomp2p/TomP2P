package net.tomp2p.replication;
import java.util.Map;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.TrackerRPC;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.StorageRunner;
import net.tomp2p.storage.TrackerStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultTrackerReplication implements ResponsibilityListener
{
	final private static Logger logger = LoggerFactory.getLogger(DefaultTrackerReplication.class);
	final private TrackerStorage trackerStorage;
	final private TrackerRPC trackerRPC;
	final private Map<BaseFuture, Long> pendingFutures;

	public DefaultTrackerReplication(final TrackerStorage trackerStorage, TrackerRPC trackerRPC,
			Map<BaseFuture, Long> pendingFutures)
	{
		this.trackerStorage = trackerStorage;
		this.trackerRPC = trackerRPC;
		this.pendingFutures = pendingFutures;
	}

	@Override
	public void otherResponsible(Number160 locationKey, final PeerAddress other)
	{
		if (logger.isDebugEnabled())
			logger.debug("[tracker] Other peer " + other + " is responsible for " + locationKey);
		trackerStorage.iterateAndRun(locationKey, new StorageRunner()
		{
			@Override
			public void call(Number160 locationKey, Number160 domainKey, Number160 contentKey,
					Data data)
			{
				pendingFutures.put(trackerRPC.addToTrackerReplication(other, locationKey,
						domainKey, data, false), System.currentTimeMillis());
				if (logger.isDebugEnabled())
					logger.debug("transfer from " + trackerRPC.getPeerAddress() + " to " + other);
			}
		});
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
