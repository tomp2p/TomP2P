package relay;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReferenceArray;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FutureForkJoin;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelayPeersManager {

	final private static Logger logger = LoggerFactory.getLogger(RelayPeersManager.class);

	// settings
	private final int maxRelays;

	private final Peer peer;
	private PeerAddress peerAddress;
	private final Queue<PeerAddress> relayCandidates;
	private RelayConnections relayConnections;

	public RelayPeersManager(final Peer peer, PeerAddress peerAddress, int maxRelays) {
		this.peer = peer;
		this.peerAddress = peerAddress;
		this.relayCandidates = new ConcurrentLinkedQueue<PeerAddress>();

		if (maxRelays > PeerAddress.MAX_RELAYS || maxRelays < 0) {
			logger.warn("at most {} relays are allowed.", PeerAddress.MAX_RELAYS);
			maxRelays = PeerAddress.MAX_RELAYS;
		}

		this.maxRelays = maxRelays;
		this.relayConnections = new RelayConnections(maxRelays);
	}

	public RelayPeersManager(final Peer peer, PeerAddress peerAddress) {
		this(peer, peerAddress, PeerAddress.MAX_RELAYS);
	}

	public RelayFuture setupRelays() {

		final RelayFuture rf = new RelayFuture(this);

		// Set firewalled flag to avoid that other peers add this peer to their
		// routing tables
		PeerAddress serverAddress = peer.getPeerBean().serverPeerAddress();
		serverAddress.changeFirewalledTCP(true).changeFirewalledUDP(true);

		FutureChannelCreator fcc = peer.getConnectionBean().reservation().create(1, 2);
		fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {

			public void operationComplete(final FutureChannelCreator future) throws Exception {
				if (future.isSuccess()) {
					bootstrap(rf, future.getChannelCreator());
				} else {
					rf.setFailed(future);
				}
			}

		});

		return rf;
	}

	private void bootstrap(final RelayFuture rf, final ChannelCreator cc) {

		// bootstrap
		FutureBootstrap fb = peer.bootstrap().setPeerAddress(peerAddress).start();
		fb.addListener(new BaseFutureListener<FutureBootstrap>() {
			public void operationComplete(FutureBootstrap future) throws Exception {
				relayCandidates.addAll(peer.getDistributedRouting().peerMap().getAll());
				logger.debug("Found {} peers that could act as relays", relayCandidates.size());
				setupPeerConnections(rf, cc);
			}

			public void exceptionCaught(Throwable t) throws Exception {
				logger.error("Bootstrapping failed: {}", t);
				rf.setFailed(t);
				rf.done();
			}
		});
	}

	private void relaySetupLoop(final RelayConnectionFuture rcf, final FutureResponse[] futureResponses, final Queue<PeerAddress> relayCandidates, 
			final ChannelCreator cc, final int numberOfRelays) {
		int active = 0;
		for (int i = 0; i < numberOfRelays; i++) {
			if (futureResponses[i] == null) {
				futureResponses[i] = new RelayRPC(peer).setupRelay(relayCandidates.poll(), cc);
				if (futureResponses[i] != null) {
					active++;
				}
			} else if (futureResponses[i] != null) {
				active++;
			}
		}
		if (active == 0) {
			rcf.done();
		}
		FutureForkJoin<FutureResponse> ffj = new FutureForkJoin<>(new AtomicReferenceArray<FutureResponse>(futureResponses));
		ffj.addListener(new BaseFutureAdapter<FutureForkJoin<FutureResponse>>() {
			@Override
			public void operationComplete(FutureForkJoin<FutureResponse> future) throws Exception {
				if (future.isSuccess()) {
					List<FutureResponse> reponses = future.getCompleted();
					for(FutureResponse fr : reponses) {
						PeerAddress relayCandidate = fr.getRequest().getRecipient();
						if(fr.isSuccess()) {
							if(fr.getResponse().getType() == Type.OK) {
								logger.debug("Adding peer {} as a relay", relayCandidate);
								rcf.addRelayAddress(relayCandidate);
							} else {
								logger.debug("Peer {} denied relay request", relayCandidate);
							}
						}
					}
					rcf.done();
				} else {
					relaySetupLoop(rcf, futureResponses, relayCandidates, cc, numberOfRelays);
				}
			}
		});
	}

	private void setupPeerConnections(final RelayFuture rf, final ChannelCreator cc) {

		// recursive loop to establish relay connection in parallel
		final int targetRelayCount = Math.min(maxRelays, relayCandidates.size());
		FutureResponse[] futureResponses = new FutureResponse[targetRelayCount];
		RelayConnectionFuture rcf = new RelayConnectionFuture();
		relaySetupLoop(rcf, futureResponses, relayCandidates, cc, targetRelayCount);
		rcf.addListener(new BaseFutureListener<RelayConnectionFuture>() {
			@Override
			public void operationComplete(RelayConnectionFuture future) throws Exception {
				System.err.println(future.getRelayAddresses());
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {
				
			}
		});
		
		/*
		while (!relayCandidates.isEmpty() && relayConnections.connectionCount().getAndIncrement() < maxRelays) {
			final PeerAddress candidate = relayCandidates.poll();
			RelayRPC relayRPC = new RelayRPC(peer);
			FutureResponse futureResponse = relayRPC.setupRelay(candidate, cc);
			futureResponse.addListener(new BaseFutureListener<FutureResponse>() {

				public void operationComplete(FutureResponse future) throws Exception {
					if (future.isSuccess() && future.getResponse().getType() == Type.OK) {
						createPermanentConnection(rf, candidate);
						relayConnections.connectionEstablished();
					} else {
						logger.debug("Peer {} denied relay request", candidate);
						relayConnections.connectionCount().decrementAndGet();
						setupPeerConnections(rf, cc);
					}
				}

				public void exceptionCaught(Throwable t) throws Exception {
					logger.debug("Relay setup failed: {}", t.getCause());
					relayConnections.connectionCount().decrementAndGet();
					setupPeerConnections(rf, cc);
				}

			});
		}
		*/

	}

	private void createPermanentConnection(final RelayFuture rf, PeerAddress relayPeer) {
		//create permanent peer connection to relay peer
		FuturePeerConnection fpc = peer.createPeerConnection(relayPeer);
		FutureDirect fd = peer.sendDirect(fpc).setObject(true).start(); 
		fd.addListener(new BaseFutureListener<FutureDirect>() {
			@Override
			public void operationComplete(FutureDirect future) throws Exception {
				relayConnections.connectionEstablished();
				rf.done();
			}

			@Override
			public void exceptionCaught(Throwable t) throws Exception {

			}
		});
		// add listener for this connection. How??? //TODO
	}

}
