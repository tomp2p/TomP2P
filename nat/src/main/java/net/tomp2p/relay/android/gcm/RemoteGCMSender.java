package net.tomp2p.relay.android.gcm;

import java.util.Collection;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.rpc.DispatchHandler;
import net.tomp2p.rpc.RPC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Send GCM messages to other well-known peers (direct messages) which then send it to the Google Cloud
 * Messaging servers. This is basically used if one of the relay peers serving an Android device does not have
 * the GCM authentication key. This key is needed to send messages over GCM and can be obtained at Google's
 * developer console.
 * 
 * @author Nico Rutishauser
 *
 */
public class RemoteGCMSender implements IGCMSender {

	private static final Logger LOG = LoggerFactory.getLogger(RemoteGCMSender.class);
	private static final int TIMEOUT_MS = 10000;
	
	private final Peer peer;
	private final ConnectionConfiguration config;
	private final DispatchHandler dispatchHandler;
	private Collection<PeerAddress> gcmServers;

	public RemoteGCMSender(Peer peer, DispatchHandler dispatchHandler, ConnectionConfiguration config, Collection<PeerAddress> gcmServers) {
		this.dispatchHandler = dispatchHandler;
		this.config = config;
		this.gcmServers = gcmServers;
		this.peer = peer;
	}

	@Override
	public void send(FutureGCM futureGCM) {
		if(gcmServers == null || gcmServers.isEmpty()) {
			LOG.error("Cannot send GCM messages because no GCM server is known");
			futureGCM.failed("Cannot send GCM messages because no GCM server is known");
			return;
		}
		
		// send to one of the servers
		for (PeerAddress gcmServer : gcmServers) {
			LOG.debug("Try sending message to {}", gcmServer);
			Message message = dispatchHandler.createMessage(gcmServer, RPC.Commands.GCM.getNr(), Type.REQUEST_1);
			FutureResponse futureResponse = RelayUtils.connectAndSend(peer, message, config);
			if(futureResponse.awaitUninterruptibly(TIMEOUT_MS) && futureResponse.isSuccess()) {
				LOG.debug("GCM server {} sent the message successfully", gcmServer);
				return;
			} else {
				LOG.debug("GCM server {} did not accept the message. Reason: {}", futureResponse.failedReason());
				// go to next server
			}
		}
		
		LOG.error("Could not send the message to any of the {} GCM servers", gcmServers.size());
		futureGCM.failed("Could not send the message to any of the GCM servers");
	}

	/**
	 * Update the gcm servers
	 */
	public void gcmServers(Collection<PeerAddress> gcmServers) {
		this.gcmServers = gcmServers;
	}
}
