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
import net.tomp2p.relay.RelayConfig;
import net.tomp2p.relay.RelayRPC;
import net.tomp2p.relay.android.MessageBufferConfiguration;

public class PeerBuilderNAT {

	final private Peer peer;

	private boolean manualPorts = false;
	private Collection<PeerAddress> manualRelays;

	private int failedRelayWaitTime = -1;
	private int maxFail = -1;

	// Android configuration
	private String gcmAuthenticationKey;
	private int gcmSendRetries = 5;
	private MessageBufferConfiguration bufferConfig = new MessageBufferConfiguration();

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
	 * Set the authentication key, which is used by relay peers to send messages to
	 * {@link RelayConfig#ANDROID} devices. This key needs to
	 * be kept secret.
	 * 
	 * @param gcmAuthenticationKey the api key / authentication token for Google Cloud Messaging. The key
	 *            can be obtained through Google's developer console
	 * @return this instance
	 */
	public PeerBuilderNAT gcmAuthenticationKey(String gcmAuthenticationKey) {
		this.gcmAuthenticationKey = gcmAuthenticationKey;
		return this;
	}

	/**
	 * @return the {@link RelayConfig#ANDROID} authentication key
	 */
	public String gcmAuthenticationKey() {
		return gcmAuthenticationKey;
	}

	/**
	 * <strong>Only used for {@link RelayConfig#ANDROID} and if this peer should act as a relay for android
	 * devices.</strong><br>
	 * 
	 * @return the number of retires sending a GCM message
	 */
	public int gcmSendRetries() {
		return gcmSendRetries;
	}

	/**
	 * <strong>Only used for {@link RelayConfig#ANDROID} and if this peer should act as a relay for android
	 * devices.</strong><br>
	 * 
	 * @param gcmSendRetries the number of retries sending a GCM message
	 * @return this instance
	 */
	public PeerBuilderNAT gcmSendRetries(int gcmSendRetries) {
		this.gcmSendRetries = gcmSendRetries;
		return this;
	}

	/**
	 * @return the {@link RelayConfig#ANDROID} buffer configuration.
	 */
	public MessageBufferConfiguration bufferConfiguration() {
		return bufferConfig;
	}

	/**
	 * Set the android relay buffer configuration. This needs to be set on relay nodes only, not on mobile
	 * peers.
	 * It is only used with {@link RelayConfig#ANDROID}.
	 * 
	 * @param bufferConfiguration the configuration
	 * @return this instance
	 */
	public PeerBuilderNAT bufferConfiguration(MessageBufferConfiguration bufferConfiguration) {
		this.bufferConfig = bufferConfiguration;
		return this;
	}

	public PeerNAT start() {
		ConnectionConfiguration connectionConfiguration = new DefaultConnectionConfiguration();

		if (bufferConfig == null) {
			bufferConfig = new MessageBufferConfiguration();
		}
		
		if(gcmSendRetries <= 0) {
			gcmSendRetries = 5;
		}

		final NATUtils natUtils = new NATUtils();
		final RconRPC rconRPC = new RconRPC(peer);
		final RelayRPC relayRPC = new RelayRPC(peer, rconRPC, bufferConfig, connectionConfiguration, gcmAuthenticationKey, gcmSendRetries);

		if (failedRelayWaitTime == -1) {
			failedRelayWaitTime = 60;
		}

		if (maxFail == -1) {
			maxFail = 2;
		}

		if (manualRelays == null) {
			manualRelays = Collections.emptyList();
		}

		peer.addShutdownListener(new Shutdown() {
			@Override
			public BaseFuture shutdown() {
				natUtils.shutdown();
				return new FutureDone<Void>().done();
			}
		});

		return new PeerNAT(peer, natUtils, relayRPC, manualRelays, failedRelayWaitTime, maxFail, manualPorts,
				connectionConfiguration);
	}
}
