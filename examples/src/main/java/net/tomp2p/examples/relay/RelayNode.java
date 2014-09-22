package net.tomp2p.examples.relay;

import java.io.IOException;

import net.tomp2p.dht.PeerBuilderDHT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Just starts a Peer and randomly makes a put / get / remove onto the DHT
 * 
 * @author Nico Rutishauser
 * 
 */
public class RelayNode {

	private static final Logger logger = LoggerFactory.getLogger(RelayNode.class);
	private static final String GCM_API_KEY = "AIzaSyDZn076gOPgwJPKXtwoUjd0xLFq4XMK5jM";

	public void start(Number160 peerId, int port) {
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
			new PeerBuilderNAT(peer).gcmAuthToken(GCM_API_KEY).start();
			logger.debug("Peer started");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
