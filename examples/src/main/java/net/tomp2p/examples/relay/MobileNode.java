package net.tomp2p.examples.relay;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import net.tomp2p.dht.FuturePut;
import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.dht.PeerDHT;
import net.tomp2p.dht.PutBuilder;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.nat.FutureRelayNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.p2p.RequestP2PConfiguration;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.RelayType;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts a peer that is firewalled and can only be reached by the relay peer.
 * 
 * @author Nico Rutishauser
 *
 */
public class MobileNode {

	private static final Logger LOG = LoggerFactory.getLogger(MobileNode.class);
	private static final int PEER_MAP_UPDATE_INTERVAL_S = 60;
	private static final String GCM_REGISTRATION_ID = "abc";

	private final Number160 peerId;
	private final int port;

	private PeerDHT peerDHT;

	public MobileNode(Number160 peerId, int port) {
		this.peerId = peerId;
		this.port = port;
	}

	public void start(Number160 relayPeerId, int relayPort) throws UnknownHostException {
		Peer peer;
		try {
			// ChannelClientConfiguration ccc = PeerBuilder.createDefaultChannelClientConfiguration();
			// ccc.pipelineFilter(new CountingPipelineFilter(CountingPipelineFilter.CounterType.INBOUND));
			//
			// ChannelServerConficuration csc = PeerBuilder.createDefaultChannelServerConfiguration();
			// csc.pipelineFilter(new CountingPipelineFilter(CountingPipelineFilter.CounterType.OUTBOUND));
			// csc.ports(new Ports(port, port));
			// csc.portsForwarding(new Ports(port, port));

			peer = new PeerBuilder(peerId).ports(port).start();
		} catch (IOException e) {
			LOG.error("Cannot create peer", e);
			return;
		}

		peerDHT = new PeerBuilderDHT(peer).storageLayer(new LoggingStorageLayer("MOBILE", true)).start();

		InetAddress bootstrapTo = InetAddress.getLocalHost();
		BootstrapBuilder bootstrapBuilder = peer.bootstrap().inetAddress(bootstrapTo).ports(relayPort);
		BaseFuture bootstrap = bootstrapBuilder.start().awaitUninterruptibly();

		if (bootstrap == null) {
			LOG.error("Cannot bootstrap");
			return;
		} else if (bootstrap.isFailed()) {
			LOG.error("Cannot bootstrap. Reason: {}", bootstrap.failedReason());
			return;
		}

		LOG.debug("Peer has bootstrapped. Connecting to Relay now");
		Set<PeerAddress> relays = new HashSet<PeerAddress>(1);
		relays.add(new PeerAddress(relayPeerId, InetAddress.getLocalHost(), relayPort, relayPort));
		PeerNAT peerNat = new PeerBuilderNAT(peer).peerMapUpdateInterval(PEER_MAP_UPDATE_INTERVAL_S).relays(relays)
				.relayType(RelayType.OPENTCP).gcmRegistrationId(GCM_REGISTRATION_ID).start();
		FutureRelayNAT futureRelayNAT = peerNat.startRelay(bootstrapBuilder).awaitUninterruptibly();
		if (!futureRelayNAT.isSuccess()) {
			LOG.error("Cannot connect to Relay. Reason: {}", futureRelayNAT.failedReason());
			return;
		}

		LOG.debug("Peer successfully connected to DHT and Relay");
	}

	public Number640 putOwnStorage(Data data) {
		// put some contnet
		Number640 key = new Number640(peerId, Number160.ZERO, Number160.ZERO, Number160.ZERO);
		PutBuilder putBuilder = peerDHT.put(key.locationKey()).domainKey(key.domainKey()).versionKey(key.versionKey())
				.data(key.contentKey(), data);
		putBuilder.requestP2PConfiguration(new RequestP2PConfiguration(1, 1, 1));
		FuturePut success = putBuilder.start().awaitUninterruptibly();
		LOG.debug("Result of put: {}. {}", success.isSuccess(), success.failedReason());
		return key;
	}
}
