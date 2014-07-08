package net.tomp2p.tracker;

import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.message.TrackerData;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.Shutdown;
import net.tomp2p.peers.Number320;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerStatatistic;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Utils;

public class PeerBuilderTracker {

	private final Peer peer;
	private final Random rnd;

	private PeerExchangeHandler peerExchangeHandler = null;
	private int peerExchangeRefreshSec = -1;
	private ConnectionConfiguration connectionConfiguration = null;
	private int ttl = -1;
	private int replicationFactor = -1;
	private int[] maintenanceInterval = null;

	public PeerBuilderTracker(Peer peer) {
		this.peer = peer;
		this.rnd = new Random(peer.peerID().longValue());
	}

	public PeerTracker start() {
		if (connectionConfiguration == null) {
			connectionConfiguration = new DefaultConnectionConfiguration();
		}
		if (peerExchangeRefreshSec == -1) {
			peerExchangeRefreshSec = 60;
		}
		if (replicationFactor == -1) {
			replicationFactor = 20;
		}
		if (ttl == -1) {
			ttl = 60;
		}
		if (maintenanceInterval == null) {
			maintenanceInterval = new int[] { 2, 4, 8, 16, 32, 64 };
		}
		TrackerStorage trackerStorage = new TrackerStorage(ttl, maintenanceInterval, replicationFactor, peer);
		if (peerExchangeHandler == null) {
			peerExchangeHandler = new DefaultPeerExchangeHandler(trackerStorage, peer.peerAddress(), rnd);
		}
		PeerExchangeRPC peerExchangeRPC = new PeerExchangeRPC(peer.peerBean(), peer.connectionBean(),
		        peerExchangeHandler);
		final PeerExchange peerExchange = new PeerExchange(peer, peerExchangeRPC, connectionConfiguration);
		trackerStorage.peerExchange(peerExchange);

		final ScheduledFuture<?> scheduledFuture;
		if (peerExchangeRefreshSec > 0) {
			scheduledFuture = peer.connectionBean().timer().scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					TrackerTriple trackerTriple = peerExchangeHandler.get();
					if (trackerTriple != null) {
						FutureDone<Void> future = peerExchange.peerExchange(trackerTriple.remotePeer(),
						        trackerTriple.key(), trackerTriple.data());
						peer.notifyAutomaticFutures(future);
					}
				}
			}, peerExchangeRefreshSec, peerExchangeRefreshSec, TimeUnit.SECONDS);
		} else {
			scheduledFuture = null;
		}

		peer.peerBean().addPeerStatusListeners(trackerStorage);
		peer.peerBean().peerMap().addPeerMapChangeListener(trackerStorage);
		peer.peerBean().maintenanceTask().addMaintainable(trackerStorage);
		peer.peerBean().digestTracker(trackerStorage);
		TrackerRPC trackerRPC = new TrackerRPC(peer.peerBean(), peer.connectionBean(), trackerStorage);
		DistributedTracker distributedTracker = new DistributedTracker(peer.peerBean(), peer.distributedRouting(),
		        trackerRPC, trackerStorage);

		final PeerTracker peerTracker = new PeerTracker(peer, scheduledFuture, trackerRPC, trackerStorage,
		        peerExchange, distributedTracker);
		peer.addShutdownListener(new Shutdown() {
			@Override
			public BaseFuture shutdown() {
				peerTracker.shutdown();
				return new FutureDone<Void>().done();
			}
		});
		return peerTracker;
	}

	public ConnectionConfiguration connectionConfiguration() {
		return connectionConfiguration;
	}

	public PeerBuilderTracker connectionConfiguration(ConnectionConfiguration connectionConfiguration) {
		this.connectionConfiguration = connectionConfiguration;
		return this;
	}

	public PeerExchangeHandler peerExchangeHandler() {
		return peerExchangeHandler;
	}

	public PeerBuilderTracker peerExchangeHandler(PeerExchangeHandler peerExchangeHandler) {
		this.peerExchangeHandler = peerExchangeHandler;
		return this;
	}

	public int peerExchangeRefreshSec() {
		return peerExchangeRefreshSec;
	}

	public PeerBuilderTracker peerExchangeRefreshSec(int peerExchangeRefreshSec) {
		this.peerExchangeRefreshSec = peerExchangeRefreshSec;
		return this;
	}

	public static class DefaultPeerExchangeHandler implements PeerExchangeHandler {

		private final TrackerStorage trackerStorage;
		private final PeerAddress self;
		private final Random rnd;

		public DefaultPeerExchangeHandler(TrackerStorage trackerStorage, PeerAddress self, Random rnd) {
			this.trackerStorage = trackerStorage;
			this.self = self;
			this.rnd = rnd;
		}

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
			peerStatatistics.remove(new PeerStatatistic(self));
			if (peerStatatistics.size() == 0) {
				return null;
			}
			PeerStatatistic peerStatatistic = Utils.pollRandom(peerStatatistics, rnd);
			return new TrackerTriple().key(key).data(trackerData).remotePeer(peerStatatistic.peerAddress());
		}
	}
}
