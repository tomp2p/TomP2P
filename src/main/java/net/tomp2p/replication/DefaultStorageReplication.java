package net.tomp2p.replication;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.config.ConfigurationStore;
import net.tomp2p.p2p.config.Configurations;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.StorageRPC;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.Storage;
import net.tomp2p.storage.StorageRunner;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultStorageReplication implements ResponsibilityListener, Runnable
{
	final private static Logger logger = LoggerFactory.getLogger(DefaultStorageReplication.class);
	private final Storage storage;
	private final StorageRPC storageRPC;
	private final Peer peer;
	private final Map<BaseFuture, Long> pendingFutures;

	public DefaultStorageReplication(Peer peer, Storage storage, StorageRPC storageRPC,
			Map<BaseFuture, Long> pendingFutures)
	{
		this.peer = peer;
		this.storage = storage;
		this.storageRPC = storageRPC;
		this.pendingFutures = pendingFutures;
	}

	@Override
	public void otherResponsible(Number160 locationKey, final PeerAddress other)
	{
		if (logger.isDebugEnabled())
			logger.debug("[storage] Other peer " + other + " is responsible for " + locationKey
					+ " I'm " + storageRPC.getPeerAddress());
		storage.iterateAndRun(locationKey, new StorageRunner()
		{
			@Override
			public void call(final Number160 locationKey, final Number160 domainKey, final Number160 contentKey,
					Data data)
			{
				// TODO do a diff or similar
				final Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
				dataMap.put(contentKey, data);
				if (logger.isDebugEnabled())
					logger.debug("transfer from " + storageRPC.getPeerAddress() + " to " + other
							+ " for key " + locationKey);
				
				
				peer.getConnectionBean().getReservation().reserve(1).addListener(new BaseFutureAdapter<FutureChannelCreator>()
				{
					@Override
					public void operationComplete(FutureChannelCreator future) throws Exception
					{
						if(future.isSuccess())
						{
							FutureResponse futureResponse=storageRPC.put(other, locationKey, domainKey, dataMap, false,
								false, false, future.getChannelCreator());
							Utils.addReleaseListener(futureResponse, peer.getConnectionBean().getReservation(), future.getChannelCreator(), 1);
							pendingFutures.put(futureResponse, System.currentTimeMillis());
						}
						else
						{
							if(logger.isErrorEnabled())
							{
								logger.error("otherResponsible failed "+future.getFailedReason());
							}
						}
					}
				});
			}
		});
	}

	@Override
	public void meResponsible(Number160 locationKey)
	{
		if (logger.isDebugEnabled())
			logger.debug("[storage] I (" + storageRPC.getPeerAddress() + ") now responsible for "
					+ locationKey);
		//TODO: we could speed this up a little and trigger the maintenance right away
	}

	@Override
	public void run()
	{
		// we get called every x seconds for content we are responsible for. So
		// we need to make sure that there are enough copies. The easy way is to
		// publish it again... The good way is to do a diff
		Collection<Number160> locationKeys = storage.findContentForResponsiblePeerID(peer.getPeerID());
		if (locationKeys == null)
			return;
		for (Number160 locationKey : locationKeys)
		{
			storage.iterateAndRun(locationKey, new StorageRunner()
			{
				@Override
				public void call(Number160 locationKey, Number160 domainKey, Number160 contentKey,
						Data data)
				{
					if (logger.isDebugEnabled())
						logger.debug("[storage refresh] I (" + storageRPC.getPeerAddress()
								+ ") restore " + locationKey);
					ConfigurationStore config = Configurations.defaultStoreConfiguration();
					config.setDomain(domainKey);
					config.setContentKey(contentKey);
					config.setStoreIfAbsent(true);
					pendingFutures.put(peer.put(locationKey, data, config), System
							.currentTimeMillis());
				}
			});
		}
	}
}
