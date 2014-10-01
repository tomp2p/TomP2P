package net.tomp2p.examples.relay;

import java.io.IOException;

import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.relay.android.AndroidRelayConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A node that forwards request to the firewalled node
 * 
 * @author Nico Rutishauser
 * 
 */
public class RelayNode {

	private static final Logger logger = LoggerFactory.getLogger(RelayNode.class);

	public void start(Number160 peerId, int port, String gcmAPIKey) {
//		ChannelClientConfiguration ccc = PeerBuilder.createDefaultChannelClientConfiguration();
//		ccc.pipelineFilter(new CountingPipelineFilter(CounterType.INBOUND));
//
//		ChannelServerConficuration csc = PeerBuilder.createDefaultChannelServerConfiguration();
//		csc.pipelineFilter(new CountingPipelineFilter(CounterType.OUTBOUND));
//		csc.ports(new Ports(port, port));
//		csc.portsForwarding(new Ports(port, port));

		try {
			Peer peer = new PeerBuilder(peerId).ports(port).start();
			// Note: Does not work if relay does not have a PeerDHT
			new PeerBuilderDHT(peer).storageLayer(new LoggingStorageLayer("RELAY", false)).start();
			new PeerBuilderNAT(peer).androidRelayConfiguration(new AndroidRelayConfiguration()).start();
			logger.debug("Peer started");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
