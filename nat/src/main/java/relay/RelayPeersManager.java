package relay;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureChannelCreator;
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
			throw new IllegalArgumentException("Invalid value for maxRelays, at most " + PeerAddress.MAX_RELAYS + " are allowed");
		}

		this.maxRelays = maxRelays;
		this.relayConnections = new RelayConnections(maxRelays);
	}

	public RelayPeersManager(final Peer peer, PeerAddress peerAddress) {
		this(peer, peerAddress, PeerAddress.MAX_RELAYS);
	}

	public RelayFuture setupRelays() {

		final RelayFuture rf = new RelayFuture();

		// get channel creator
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

		PeerAddress serverAddress = peer.getPeerBean().serverPeerAddress();
		serverAddress.changeFirewalledTCP(true).changeFirewalledUDP(true);

		return rf;
	}

	private void bootstrap(final RelayFuture rf, final ChannelCreator cc) {
		// bootstrap
		FutureBootstrap fb = peer.bootstrap().setPeerAddress(peerAddress).start();
		fb.addListener(new BaseFutureListener<FutureBootstrap>() {
			public void operationComplete(FutureBootstrap future) throws Exception {
				relayCandidates.addAll(peer.getDistributedRouting().peerMap().getAll());
				logger.debug("Found {} peers that could act as relays", relayCandidates.size());
				//peer.setObjectDataReply(new RelayObjectReply(peer));
				setupPeerConnections(rf, cc);
			}

			public void exceptionCaught(Throwable t) throws Exception {
				rf.setFailed(t);
				rf.done();
			}
		});
	}

	private void setupPeerConnections(final RelayFuture rf, final ChannelCreator cc) {
		
		while (!relayCandidates.isEmpty() && relayConnections.connectionCount().getAndIncrement() < maxRelays) {
			final PeerAddress candidate = relayCandidates.poll();
			RelayRPC relayRPC = new RelayRPC(peer);
			FutureResponse futureResponse = relayRPC.setupRelay(candidate, cc);
			futureResponse.addListener(new BaseFutureListener<FutureResponse>() {

				public void operationComplete(FutureResponse future) throws Exception {
					if(future.isSuccess() && future.getResponse().getType() == Type.OK) {
						createPermanentConnection(rf, candidate);
					} else {
						logger.debug("Relay setup failed");
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
		
		try {
			Thread.sleep(2000);
			rf.done();
			System.out.println("relay future done");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private void createPermanentConnection(RelayFuture rf, PeerAddress relayPeer) {
		FuturePeerConnection fpc = peer.createPeerConnection(relayPeer);
		peer.sendDirect(fpc).setObject("trolololo").start();
	}

}
