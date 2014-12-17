package net.tomp2p.relay.tcp;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;

import net.tomp2p.connection.ConnectionConfiguration;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Buffer;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Type;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.relay.BaseRelayForwarderRPC;
import net.tomp2p.relay.RelayType;
import net.tomp2p.relay.RelayUtils;
import net.tomp2p.rpc.RPC;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The RelayForwarder is responsible for forwarding all messages that are
 * received on a relay peer, but are intended for an unreachable peer that is
 * connected to the relay peer. Every unreachable node has an own instance of
 * this class at the relay server.
 * 
 * @author Raphael Voellmy
 * @author Nico Rutishauser
 * 
 */
public class OpenTCPForwarderRPC extends BaseRelayForwarderRPC {

	private final static Logger LOG = LoggerFactory.getLogger(OpenTCPForwarderRPC.class);

	// connection to unreachable peer
	private final PeerConnection peerConnection;
	private final ConnectionConfiguration config;

	/**
	 * 
	 * @param peerConnection
	 *            A peer connection to an unreachable peer that is permanently
	 *            open
	 * @param peer
	 *            The relay peer
	 */
	public OpenTCPForwarderRPC(final PeerConnection peerConnection, final Peer peer, ConnectionConfiguration config) {
		super(peer, peerConnection.remotePeer(), RelayType.OPENTCP);
		this.config = config;
		this.peerConnection = peerConnection.changeRemotePeer(unreachablePeerAddress());

		// add a listener when the connection is closed
		peerConnection.closeFuture().addListener(new BaseFutureAdapter<FutureDone<Void>>() {
			@Override
			public void operationComplete(FutureDone<Void> future) throws Exception {
				peerBean().removePeerStatusListener(OpenTCPForwarderRPC.this);
				connectionBean().dispatcher().removeIoHandler(relayPeerId(), unreachablePeerId());
			}
		});

		LOG.debug("Created TCP forwarder from peer {} to peer {}", peer.peerAddress(), unreachablePeerAddress());
	}

	@Override
	public FutureDone<Message> forwardToUnreachable(final Message message) {
		// Send message via direct message through the open connection to the unreachable peer
		LOG.debug("Sending {} to unreachable peer {}", message, peerConnection.remotePeer());
		final Message envelope = createMessage(peerConnection.remotePeer(), RPC.Commands.RELAY.getNr(), Type.REQUEST_2);
		try {
			message.restoreContentReferences();
			// add the message into the payload
			envelope.buffer(RelayUtils.encodeMessage(message, connectionBean().channelServer().channelServerConfiguration().signatureFactory()));
		} catch (Exception e) {
			LOG.error("Cannot encode the message", e);
			return new FutureDone<Message>().failed(e);
		}

		// always keep the connection open
		envelope.keepAlive(true);

		// this will be read RelayRPC.handlePiggyBackMessage
		Collection<PeerSocketAddress> peerSocketAddresses = new ArrayList<PeerSocketAddress>(1);
		peerSocketAddresses.add(new PeerSocketAddress(message.sender().inetAddress(), 0, 0));
		envelope.peerSocketAddresses(peerSocketAddresses);
		
		// holds the message that will be returned to he requester
		final FutureDone<Message> futureDone = new FutureDone<Message>();

		// Forward a message through the open peer connection to the unreachable peer.
		FutureResponse fr = RelayUtils.send(peerConnection, peerBean(), connectionBean(), config, envelope);
		fr.addListener(new BaseFutureAdapter<FutureResponse>() {
			public void operationComplete(FutureResponse future) throws Exception {
				if (future.isSuccess()) {
					InetSocketAddress senderSocket = message.recipientSocket();
					if (senderSocket == null) {
						senderSocket = unreachablePeerAddress().createSocketTCP();
					}
					InetSocketAddress recipientSocket = message.senderSocket();
					if (recipientSocket == null) {
						recipientSocket = message.sender().createSocketTCP();
					}

					Buffer buffer = future.responseMessage().buffer(0);
					Message responseFromUnreachablePeer = RelayUtils.decodeMessage(buffer.buffer(), recipientSocket, senderSocket, connectionBean().channelServer().channelServerConfiguration().signatureFactory());
					responseFromUnreachablePeer.restoreContentReferences();
					futureDone.done(responseFromUnreachablePeer);
				} else {
					futureDone.failed("Could not forward message over TCP channel");
				}
			}
		});

		return futureDone;
	}

	@Override
	protected void peerMapUpdated() {
		// ignore
	}

	@Override
	protected boolean isAlive() {
		LOG.trace("peerconnection open? {}", peerConnection.isOpen());
		return peerConnection.isOpen();
	}
}
