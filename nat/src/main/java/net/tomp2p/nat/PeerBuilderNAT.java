package net.tomp2p.nat;

import java.util.Collection;
import java.util.Collections;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.Shutdown;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.RconRPC;
import net.tomp2p.relay.RelayRPC;
import net.tomp2p.relay.RelayType;
import net.tomp2p.relay.android.MessageBufferConfiguration;
import net.tomp2p.relay.android.GCMServerCredentials;

public class PeerBuilderNAT {

	final private Peer peer;

	private boolean manualPorts = false;
	private Collection<PeerAddress> manualRelays;

	private int failedRelayWaitTime = -1;
	private int maxFail = -1;
	private int peerMapUpdateInterval = -1;

	private RelayType relayType = RelayType.OPENTCP;
	private MessageBufferConfiguration bufferConfig = new MessageBufferConfiguration();
	private GCMServerCredentials gcmServerCredentials = new GCMServerCredentials();

	public PeerBuilderNAT(Peer peer) {
		this.peer = peer;
	}

	public boolean isManualPorts() {
		return manualPorts;
	}

	public PeerBuilderNAT manualPorts() {
		return manualPorts(true);
	}

	public PeerBuilderNAT manualPorts(boolean manualPorts) {
		this.manualPorts = manualPorts;
		return this;
	}

	public Collection<PeerAddress> manualRelays() {
		return manualRelays;
	}

	public PeerBuilderNAT relays(Collection<PeerAddress> manualRelays) {
		this.manualRelays = manualRelays;
		return this;
	}

	/**
	 * Defines how many seconds to wait at least until asking a relay that
	 * denied a relay request or a relay that failed to act as a relay again
	 * 
	 * @param failedRelayWaitTime
	 *            wait time in seconds
	 * @return this instance
	 */
	public PeerBuilderNAT failedRelayWaitTime(int failedRelayWaitTime) {
		this.failedRelayWaitTime = failedRelayWaitTime;
		return this;
	}

	/**
	 * @return How many seconds to wait at least until asking a relay that
	 *         denied a relay request or a relay that failed to act as a relay
	 *         again
	 */
	public int failedRelayWaitTime() {
		return failedRelayWaitTime;
	}

	public PeerBuilderNAT maxFail(int maxFail) {
		this.maxFail = maxFail;
		return this;
	}

	public int maxFail() {
		return maxFail;
	}

	/**
	 * Set the kind of relaying. For example mobile devices need special treatment
	 * to save energy. The type needs to be set at the peer behind the NAT only
	 * (not the relay peer).
	 */
	public PeerBuilderNAT relayType(RelayType relayType) {
		this.relayType = relayType;
		return this;
	}

	/**
	 * @return the kind of relaying.
	 */
	public RelayType relayType() {
		return relayType;
	}

	/**
	 * @return the android relay configuration.
	 */
	public MessageBufferConfiguration bufferConfiguration() {
		return bufferConfig;
	}

	/**
	 * Set the android relay buffer configuration. This needs to be set on relay nodes only, not on mobile peers.
	 * It is only used with {@link RelayType#ANDROID}.
	 * 
	 * @param bufferConfiguration the configuration
	 * @return this instance
	 */
	public PeerBuilderNAT bufferConfiguration(MessageBufferConfiguration bufferConfiguration) {
		this.bufferConfig = bufferConfiguration;
		return this;
	}

	/**
	 * Set the Google Cloud Messaging server credentials. A GCM server can handle 4.
	 * If this peer is a unreachable Android device, the {@link GCMServerCredentials} must be provided.
	 * 
	 * @param gcmServerCredentials
	 *            the GCM server credentials
	 * @return this instance
	 */
	public PeerBuilderNAT gcmServerCredentials(GCMServerCredentials gcmServerCredentials) {
		this.gcmServerCredentials = gcmServerCredentials;
		return this;
	}

	/**
	 * @return the currently configured {@link GCMServerCredentials}. If this peer is an unreachable
	 * Android device, these credentials need to be provided.
	 */
	public GCMServerCredentials gcmServerCredentials() {
		return gcmServerCredentials;
	}

	/**
	 * Defines the time interval of sending the peer map of the unreachable peer
	 * to its relays. The routing requests are not relayed to the unreachable
	 * peer but handled by the relay peers. Therefore, the relay peers should
	 * always have an up-to-date peer map of the relayed peer
	 * 
	 * @param peerMapUpdateInterval
	 *            interval of updates in seconds
	 * @return this instance
	 */
	public PeerBuilderNAT peerMapUpdateInterval(int peerMapUpdateInterval) {
		this.peerMapUpdateInterval = peerMapUpdateInterval;
		return this;
	}

	/**
	 * @return the peer map update interval in seconds
	 */
	public int peerMapUpdateInterval() {
		return peerMapUpdateInterval;
	}

	public PeerNAT start() {
		ConnectionConfiguration connectionConfiguration = new DefaultConnectionConfiguration();

		if(bufferConfig == null) {
			bufferConfig = new MessageBufferConfiguration();
		}
		
		final NATUtils natUtils = new NATUtils();
		final RconRPC rconRPC = new RconRPC(peer);
		final RelayRPC relayRPC = new RelayRPC(peer, rconRPC, bufferConfig, connectionConfiguration);

		if (failedRelayWaitTime == -1) {
			failedRelayWaitTime = 60;
		}

		if (maxFail == -1) {
			maxFail = 2;
		}

		if (manualRelays == null) {
			manualRelays = Collections.emptyList();
		}

		if (relayType == null) {
			relayType = RelayType.OPENTCP;
		}
		
		if (peerMapUpdateInterval == -1) {
			peerMapUpdateInterval = relayType.defaultMapUpdateInterval();
		}
		
		peer.addShutdownListener(new Shutdown() {
			@Override
			public BaseFuture shutdown() {
				natUtils.shutdown();
				return new FutureDone<Void>().done();
			}
		});

		return new PeerNAT(peer, natUtils, relayRPC, manualRelays, failedRelayWaitTime, maxFail, peerMapUpdateInterval,
				manualPorts, relayType, gcmServerCredentials, connectionConfiguration);
	}
}
