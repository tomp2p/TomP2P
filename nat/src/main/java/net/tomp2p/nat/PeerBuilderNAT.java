package net.tomp2p.nat;

import java.util.HashMap;
import java.util.Map;

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

	final private Peer peer;

	private boolean manualPorts = false;

	// holds multiple implementations for serving relay peers
	private Map<RelayType, RelayServerConfig> relayServerConfigurations;

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
	 * @return the relay server configurations. By default, {@link RelayType#OPENTCP} is implemented.
	 */
	public Map<RelayType, RelayServerConfig> relayServerConfigurations() {
		return relayServerConfigurations;
	}

	/**
	 * Set all relay server configurations
	 * @return this instance
	 */
	public PeerBuilderNAT relayServerConfigurations(Map<RelayType, RelayServerConfig> relayServerConfigurations) {
		this.relayServerConfigurations = relayServerConfigurations;
		return this;
	}
	
	/**
	 * Add a new server configuration (e.g. for {@link RelayType#ANDROID}).
	 * @return  this instance
	 */
	public PeerBuilderNAT addRelayServerConfiguration(RelayType relayType, RelayServerConfig configuration) {
		relayServerConfigurations.put(relayType, configuration);
		return this;
	}

	public PeerNAT start() {
		final NATUtils natUtils = new NATUtils();
		final RconRPC rconRPC = new RconRPC(peer);
		final HolePRPC holePunchRPC = new HolePRPC(peer);
		
		if(relayServerConfigurations == null) {
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
		
		peer.peerBean().holePunchInitiator(new HolePInitiatorImpl(peer));

		return new PeerNAT(peer, natUtils, relayRPC, manualPorts);
	}
}
