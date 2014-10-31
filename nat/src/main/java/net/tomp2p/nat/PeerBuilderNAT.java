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
import net.tomp2p.relay.android.MessageBufferConfiguration;

public class PeerBuilderNAT {

	final private Peer peer;

	private boolean manualPorts = false;
	private Collection<PeerAddress> manualRelays;

	private int failedRelayWaitTime = -1;
	private int maxFail = -1;

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
	 * @return the android relay configuration.
	 */
	public MessageBufferConfiguration bufferConfiguration() {
		return bufferConfig;
	}

	/**
	 * Set the android relay buffer configuration. This needs to be set on relay nodes only, not on mobile peers.
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
		
		peer.addShutdownListener(new Shutdown() {
			@Override
			public BaseFuture shutdown() {
				natUtils.shutdown();
				return new FutureDone<Void>().done();
			}
		});

		return new PeerNAT(peer, natUtils, relayRPC, manualRelays, failedRelayWaitTime, maxFail,
				manualPorts, connectionConfiguration);
	}
}
