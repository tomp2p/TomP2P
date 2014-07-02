package net.tomp2p.tracker;

import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.TrackerData;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.Shutdown;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

public class PeerTracker implements Runnable {
	private final DistributedTracker distributedTracker;

	private final TrackerRPC trackerRPC;
	private final TrackerStorage trackerStorage;
	private final Peer peer;
	private final Random rnd;

	private PeerExchangeRPC peerExchangeRPC = null;
	private PeerExchangeHandler peerExchangeHandler = null;
	private int peerExchangeRefreshSec = -1;
	private ScheduledFuture<?> scheduledFuture = null;
	private ConnectionConfiguration connectionConfiguration = null;
	
	public PeerTracker(Peer peer) {
		this(peer, 60, 20, new int[]{2,4,8,16,32,64});
	}

	public PeerTracker(Peer peer, int ttl, int replicationFactor, int[] maintenanceInterval) {
		this.peer = peer;
		this.rnd = new Random(peer.peerID().longValue());
		trackerStorage = new TrackerStorage(ttl, maintenanceInterval, this, replicationFactor);
		peer.peerBean().addPeerStatusListeners(trackerStorage);
		peer.peerBean().peerMap().addPeerMapChangeListener(trackerStorage);
		peer.peerBean().maintenanceTask().addMaintainable(trackerStorage);
		trackerRPC = new TrackerRPC(peer.peerBean(), peer.connectionBean(), trackerStorage);
		distributedTracker = new DistributedTracker(peer.peerBean(), peer.distributedRouting(), trackerRPC,
		        trackerStorage);

		peer.addShutdownListener(new Shutdown() {
			@Override
			public BaseFuture shutdown() {
				PeerTracker.this.shutdown();
				return new FutureDone<Void>().done();
			}
		});
	}

	public PeerTracker startPeerExchange() {
		if (peerExchangeHandler == null) {
			peerExchangeHandler = new DefaultPeerExchangeHandler();
		}
		peerExchangeRPC = new PeerExchangeRPC(peer.peerBean(), peer.connectionBean(), peerExchangeHandler);
		if (peerExchangeRefreshSec == -1) {
			peerExchangeRefreshSec = 60;
		}
		if (connectionConfiguration == null) {
			connectionConfiguration = new DefaultConnectionConfiguration();
		}
		if (peerExchangeRefreshSec > 0) {
			scheduledFuture = peer.connectionBean().timer()
			        .scheduleAtFixedRate(this, peerExchangeRefreshSec, peerExchangeRefreshSec, TimeUnit.SECONDS);
		}
		return this;
	}

	@Override
	public void run() {
		TrackerTriple trackerTriple = peerExchangeHandler.get();
		if (trackerTriple != null) {
			FutureDone<Void> future = peerExchange(trackerTriple.remotePeer(), trackerTriple.key(),
			        trackerTriple.data());
			peer().notifyAutomaticFutures(future);
		}
	}

	public FutureDone<Void> peerExchange(final PeerAddress remotePeer, final Number320 key, final TrackerData data) {
		return peerExchange(remotePeer, key, data, connectionConfiguration);
	}

	public FutureDone<Void> peerExchange(final PeerAddress remotePeer, final Number320 key, final TrackerData data,
	        final ConnectionConfiguration connectionConfiguration) {
		final FutureDone<Void> futureDone = new FutureDone<Void>();
		FutureChannelCreator futureChannelCreator = peer.connectionBean().reservation().create(1, 0);
		futureChannelCreator.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			@Override
			public void operationComplete(FutureChannelCreator future) throws Exception {
				if (future.isSuccess()) {
					final ChannelCreator channelCreator = future.channelCreator();
					FutureResponse futureResponse = peerExchangeRPC.peerExchange(remotePeer, key, channelCreator, data,
					        connectionConfiguration);
					futureResponse.addListener(new BaseFutureAdapter<FutureResponse>() {
						@Override
						public void operationComplete(FutureResponse future) throws Exception {
							if (future.isSuccess()) {
								futureDone.done();
							} else {
								futureDone.failed(future);
							}
							channelCreator.shutdown();
						}
					});
				} else {
					futureDone.failed(future);
				}
			}
		});
		return futureDone;
	}

	public ConnectionConfiguration connectionConfiguration() {
		return connectionConfiguration;
	}

	public PeerTracker connectionConfiguration(ConnectionConfiguration connectionConfiguration) {
		this.connectionConfiguration = connectionConfiguration;
		return this;
	}

	public PeerExchangeHandler peerExchangeHandler() {
		return peerExchangeHandler;
	}

	public PeerTracker peerExchangeHandler(PeerExchangeHandler peerExchangeHandler) {
		this.peerExchangeHandler = peerExchangeHandler;
		return this;
	}

	public PeerExchangeRPC peerExchangeRPC() {
		if (peerExchangeRPC == null) {
			throw new IllegalStateException("Cannot use peer exchange without staring it.");
		}
		return peerExchangeRPC;
	}

	public int peerExchangeRefreshSec() {
		return peerExchangeRefreshSec;
	}

	public PeerTracker peerExchangeRefreshSec(int peerExchangeRefreshSec) {
		this.peerExchangeRefreshSec = peerExchangeRefreshSec;
		return this;
	}

	public TrackerRPC trackerRPC() {
		return trackerRPC;
	}

	public DistributedTracker distributedTracker() {
		return distributedTracker;
	}

	public TrackerStorage trackerStorage() {
		return trackerStorage;
	}

	public PeerMap peerMap() {
		return peer.peerBean().peerMap();
	}

	public PeerAddress peerAddress() {
		return peer.peerAddress();
	}

	public Peer peer() {
		return peer;
	}

	public AddTrackerBuilder addTracker(Number160 locationKey) {
		return new AddTrackerBuilder(this, locationKey);
	}

	public GetTrackerBuilder getTracker(Number160 locationKey) {
		return new GetTrackerBuilder(this, locationKey);
	}

	public void shutdown() {
		if (scheduledFuture != null) {
			scheduledFuture.cancel(false);
		}
	}
	
	class DefaultPeerExchangeHandler implements PeerExchangeHandler {
		@Override
		public boolean put(Number320 key, TrackerData trackerData, PeerAddress referrer) {
			for (Map.Entry<PeerStatatistic, Data> entry : trackerData.peerAddresses().entrySet()) {
				trackerStorage.put(key, entry.getKey().peerAddress(), null, entry.getValue());
			}
			return false;
		}

		@Override
		public TrackerTriple get() {
			Collection<Number320> keys = trackerStorage.keys();
			if (keys == null || keys.size() == 0) {
				return null;
			}
			Number320 key = Utils.pollRandom(keys, rnd);
			TrackerData trackerData = trackerStorage.peers(key);
			if (trackerData == null) {
				return null;
			}
			Collection<PeerStatatistic> peerStatatistics = trackerData.peerAddresses().keySet();
			if (peerStatatistics == null || peerStatatistics.size() == 0) {
				return null;
			}
			peerStatatistics.remove(new PeerStatatistic(peer.peerAddress()));
			if (peerStatatistics.size() == 0) {
				return null;
			}
			PeerStatatistic peerStatatistic = Utils.pollRandom(peerStatatistics, rnd);
			return new TrackerTriple().key(key).data(trackerData).remotePeer(peerStatatistic.peerAddress());
		}
	}
}
