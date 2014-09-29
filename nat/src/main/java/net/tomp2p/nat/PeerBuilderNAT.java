package net.tomp2p.nat;

import java.util.ArrayList;
import java.util.Collection;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.Shutdown;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rcon.RconRPC;
import net.tomp2p.relay.RelayRPC;
import net.tomp2p.relay.RelayType;

public class PeerBuilderNAT {

	final private Peer peer;

	private boolean manualPorts = false;
	private Collection<PeerAddress> manualRelays;

	private int failedRelayWaitTime = -1;
	private int maxFail = -1;
	private int peerMapUpdateInterval = -1;
	
	private RelayType relayType = RelayType.OPENTCP;
	private String gcmAuthToken;
	private String gcmRegistrationId;

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
	 * Set the Google Cloud Messaging authentication token (API key).
	 * This is used at the Relay server to serve {@link RelayType#ANDROID} relay requests.
	 * @param gcmAuthToken the API key for GCM
	 * @return
	 */
	public PeerBuilderNAT gcmAuthToken(String gcmAuthToken) {
		this.gcmAuthToken = gcmAuthToken;
		return this;
	}
	
	/**
	 * @return the Google Cloud Messaging Authentication token (API key)
	 */
	public String gcmAuthToken() {
		return gcmAuthToken;
	}
	
	/**
	 * Set the Google Cloud Messaging registration id. The registration id is a unique token
	 * that is received from Google's registration server. This id needs to be set at
	 * the peer behind the NAT only. It will be sent to the relay peers during the
	 * setup phase.
	 * 
	 * @param gcmRegistrationId
	 * 				the registration id of the mobile device
	 * @return this instance
	 */
	public PeerBuilderNAT gcmRegistrationId(String gcmRegistrationId) {
		this.gcmRegistrationId = gcmRegistrationId;
		return this;
	}
	
	/**
	 * @return the registration id of the device from GCM
	 */
	public String gcmRegistrationId() {
		return gcmRegistrationId;
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
		
		final NATUtils natUtils = new NATUtils();
		final RconRPC rconRPC = new RconRPC(peer);
		final RelayRPC relayRPC = new RelayRPC(peer, rconRPC, gcmAuthToken, connectionConfiguration);

		if (failedRelayWaitTime == -1) {
			failedRelayWaitTime = 60;
		}

		if (maxFail == -1) {
			maxFail = 2;
		}

		if (peerMapUpdateInterval == -1) {
			peerMapUpdateInterval = 60;
		}
		
		if(manualRelays == null) {
			manualRelays = new ArrayList<PeerAddress>(1);
		}
		
		if(relayType == null) {
			relayType = RelayType.OPENTCP;
		}
		
		peer.addShutdownListener(new Shutdown() {
			@Override
			public BaseFuture shutdown() {
				natUtils.shutdown();
				return new FutureDone<Void>().done();
			}
		});

		return new PeerNAT(peer, natUtils, relayRPC, manualRelays, failedRelayWaitTime,
		        maxFail, peerMapUpdateInterval, manualPorts, relayType, gcmRegistrationId, connectionConfiguration);
	}
}
