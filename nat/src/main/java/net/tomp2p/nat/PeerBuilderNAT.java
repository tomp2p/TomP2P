package net.tomp2p.nat;

import java.util.HashMap;
import java.util.Map;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.holep.HolePunchInitiatorImpl;
import net.tomp2p.holep.HolePunchRPC;
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
		relayServerConfigurations.put(RelayType.OPENTCP, new TCPRelayServerConfig(peer));
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
	 */
	public void relayServerConfigurations(Map<RelayType, RelayServerConfig> relayServerConfigurations) {
		this.relayServerConfigurations = relayServerConfigurations;
	}
	
	/**
	 * Add a new server configuration (e.g. for {@link RelayType#ANDROID}).
	 */
	public void addRelayServerConfiguration(RelayType relayType, RelayServerConfig configuration) {
		relayServerConfigurations.put(relayType, configuration);
	}

	public PeerNAT start() {
		final NATUtils natUtils = new NATUtils();
		final RconRPC rconRPC = new RconRPC(peer);
		final HolePunchRPC holePunchRPC = new HolePunchRPC(peer);
		final RelayRPC relayRPC = new RelayRPC(peer, rconRPC, holePunchRPC, relayServerConfigurations);

		peer.addShutdownListener(new Shutdown() {
			@Override
			public BaseFuture shutdown() {
				natUtils.shutdown();
				return new FutureDone<Void>().done();
			}
		});
		
		peer.peerBean().holePunchInitiator(new HolePunchInitiatorImpl(peer));

		return new PeerNAT(peer, natUtils, relayRPC, manualPorts);
	}
}
