package net.tomp2p.nat;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.holep.HolePInitiatorImpl;
import net.tomp2p.holep.HolePRPC;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.Shutdown;
import net.tomp2p.relay.RconRPC;
import net.tomp2p.relay.RelayRPC;
import net.tomp2p.relay.RelayServerConfig;
import net.tomp2p.relay.RelayType;
import net.tomp2p.relay.tcp.TCPRelayServerConfig;

public class PeerBuilderNAT {

	private static final Logger LOG = LoggerFactory.getLogger(PeerBuilderNAT.class);
	final private Peer peer;

	private boolean manualPorts = false;

	// holds multiple implementations for serving relay peers
	private Map<RelayType, RelayServerConfig> relayServerConfigurations;

	private static final int DEFAULT_NUMBER_OF_HOLEP_HOLES = 3;
	private int holePNumberOfHoles = DEFAULT_NUMBER_OF_HOLEP_HOLES;
	private static final int DEFAULT_NUMBER_OF_HOLE_PUNCHES = 3;
	private int holePNumberOfPunches = DEFAULT_NUMBER_OF_HOLE_PUNCHES;

	public PeerBuilderNAT(Peer peer) {
		this.peer = peer;

		// add TCP server by default
		this.relayServerConfigurations = new HashMap<RelayType, RelayServerConfig>();
		relayServerConfigurations.put(RelayType.OPENTCP, new TCPRelayServerConfig());
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

	/**
	 * @return the relay server configurations. By default,
	 *         {@link RelayType#OPENTCP} is implemented.
	 */
	public Map<RelayType, RelayServerConfig> relayServerConfigurations() {
		return relayServerConfigurations;
	}

	/**
	 * Set all relay server configurations
	 * 
	 * @return this instance
	 */
	public PeerBuilderNAT relayServerConfigurations(Map<RelayType, RelayServerConfig> relayServerConfigurations) {
		this.relayServerConfigurations = relayServerConfigurations;
		return this;
	}

	/**
	 * Add a new server configuration (e.g. for {@link RelayType#ANDROID}).
	 * 
	 * @return this instance
	 */
	public PeerBuilderNAT addRelayServerConfiguration(RelayType relayType, RelayServerConfig configuration) {
		relayServerConfigurations.put(relayType, configuration);
		return this;
	}

	/**
	 * This method specifies the amount of holes, which shall be punched by the
	 * {@link HolePStrategy}.
	 * 
	 * @param holePNumberOfHoles
	 * @return this instance
	 */
	public PeerBuilderNAT holePNumberOfHoles(final int holePNumberOfHoles) {
		if (holePNumberOfHoles < 1 || holePNumberOfHoles > 100) {
			LOG.error("The specified number of holes is invalid. holePNumberOfHoles must be between 1 and 100. The value is changed back to the default value (which is currently "
					+ DEFAULT_NUMBER_OF_HOLEP_HOLES + ")");
			return this;
		} else {
			this.holePNumberOfHoles = holePNumberOfHoles;
			return this;
		}
	}

	public int holePNumberOfHoles() {
		return this.holePNumberOfHoles;
	}

	/**
	 * specifies how many times the hole will be punched (e.g. if
	 * holePNumberOfPunches = 3, then then all holes will be punched with dummy
	 * messages 3 times in a row with 1 second delay (see {@link HolePScheduler}
	 * ).
	 * 
	 * @param holePNumberOfPunches
	 * @return this instance
	 */
	public PeerBuilderNAT holePNumberOfHolePunches(final int holePNumberOfPunches) {
		this.holePNumberOfPunches = holePNumberOfPunches;
		return this;
	}

	public int holePNumberOfPunches() {
		return holePNumberOfPunches;
	}

	public PeerNAT start() {
		final NATUtils natUtils = new NATUtils();
		final RconRPC rconRPC = new RconRPC(peer);
		final HolePRPC holePunchRPC = new HolePRPC(peer);
		
		peer.peerBean().holePunchInitiator(new HolePInitiatorImpl(peer));
		peer.peerBean().holePNumberOfHoles(holePNumberOfHoles);
		peer.peerBean().holePNumberOfPunches(holePNumberOfPunches);

		if (relayServerConfigurations == null) {
			relayServerConfigurations = new HashMap<RelayType, RelayServerConfig>(0);
		} else {
			// start the server configurations
			for (RelayServerConfig config : relayServerConfigurations.values()) {
				config.start(peer);
			}
		}
		final RelayRPC relayRPC = new RelayRPC(peer, rconRPC, holePunchRPC, relayServerConfigurations);

		peer.addShutdownListener(new Shutdown() {
			@Override
			public BaseFuture shutdown() {
				natUtils.shutdown();
				return new FutureDone<Void>().done();
			}
		});

		return new PeerNAT(peer, natUtils, relayRPC, manualPorts);
	}
}
