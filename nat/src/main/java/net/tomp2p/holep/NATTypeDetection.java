package net.tomp2p.holep;

import java.util.List;

import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NATTypeDetection {
	
	private static final int SEQ_PORT_TOLERANCE = 5;
	private NATType natType = null;
	private static final Logger LOG = LoggerFactory.getLogger(NATTypeDetection.class);
	private Peer peer;
	
	
	public NATTypeDetection(Peer peer) {
		this.peer = peer;
		this.natType = NATType.UNKNOWN;
	}
	
	public NATType natType() {
		return natType;
	}
	
	/**
	 * This method contacts a Relay {@link Peer} in order to find out the NAT
	 * port assignement behaviour. There are five possible NAT behaviours: <br />
	 * NO_NAT = There is no NAT in use. <br />
	 * PORT_PRESERVING = The NAT preserves the port which a peer uses to send
	 * messages from. <br />
	 * NON_PRESERVING_SEQUENTIAL = The NAT doesn't preserve the port and assigns
	 * another port in a sequential fashion (e.g. 1234). <br />
	 * NON_PRESERVING_RANDOM = The NAT doesn't preserve the port and assigns
	 * another random port instead. <br />
	 * UNKNOWN = We don't know anything about the NAT <br />
	 * 
	 * @param relayPeer
	 *            The {@link PeerAddress} of a relay peer.
	 * @return futureDone A FutureDone to check if the method succeded.
	 */
	public FutureDone<NATType> checkNATType(final PeerAddress relayPeer) {
		final FutureDone<NATType> futureDone = new FutureDone<NATType>();
		pingRelayNATTest(futureDone, relayPeer, null, null);
		return futureDone;
	}

	/**
	 * This method is always executed twice in a recursive manner. It contacts a
	 * given relay {@link Peer} on it's {@link PeerAddress}. The relay peer then
	 * simply returns the port number and the IP address the requesting peer was
	 * contacting from.
	 * 
	 * @param fd
	 *            The corresponding {@link FutureDone}
	 * @param relayPeer
	 *            The {@link PeerAddress} of the relay
	 * @param senderPsa
	 *            The senders {@link PeerSocketAddress}
	 * @param recipientPsa
	 *            The recipients {@link PeerSocketAddress}
	 */
	private void pingRelayNATTest(final FutureDone<NATType> fd, final PeerAddress relayPeer,
			final PeerSocketAddress senderPsa, final PeerSocketAddress recipientPsa) {
		// watch out for sideEffects
		// test NATType
		FutureChannelCreator fcc1 = peer.connectionBean().reservation().create(1, 0);
		fcc1.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			@Override
			public void operationComplete(FutureChannelCreator future) throws Exception {
				if (future.isSuccess()) {
					FutureDone<List<PeerSocketAddress>> fDone = peer.pingRPC().pingNATType(relayPeer,
							future.channelCreator(), new DefaultConnectionConfiguration(), peer);
					fDone.addListener(new BaseFutureAdapter<FutureDone<List<PeerSocketAddress>>>() {
						@Override
						public void operationComplete(FutureDone<List<PeerSocketAddress>> future) throws Exception {
							if (future.isSuccess()) {
								List<PeerSocketAddress> addresses = future.object();
								// we need to contact the relay twice in order
								// to distinguish between a sequential and a
								// random port assignement
								if (senderPsa == null || recipientPsa == null) {
									pingRelayNATTest(fd, relayPeer, addresses.get(0), addresses.get(1));
								} else {
									checkNATType(fd, senderPsa, recipientPsa, addresses.get(0), addresses.get(1));
								}
							} else {
								fd.failed("Could not emit NAT type!");
							}
						}
					});
				} else {
					fd.failed("Could not emit NAT type!");
				}
			}
		});
	}

	/**
	 * This method is called from pingRelayNATTest(...). It checks what type of
	 * NAT the local {@link Peer} is using based on the information from the
	 * pingRelayNATTest(...).
	 * 
	 * @param fd
	 *            The corresponding {@link FutureDone}
	 * @param senderPsa
	 *            The senders {@link PeerSocketAddress} from the first ping.
	 * @param recipientPsa
	 *            The recipients {@link PeerSocketAddress} from the first ping.
	 * @param senderPsa2
	 *            The senders {@link PeerSocketAddress} from the second ping.
	 * @param recipientPsa2
	 *            The recipients {@link PeerSocketAddress} from the second ping.
	 */
	private void checkNATType(FutureDone<NATType> fd, PeerSocketAddress senderPsa, PeerSocketAddress recipientPsa,
			PeerSocketAddress senderPsa2, PeerSocketAddress recipientPsa2) {
		if (senderPsa.inetAddress().equals(recipientPsa.inetAddress())) {
			signalNAT("there is no NAT to be traversed!", NATType.NO_NAT, fd);
		} else if (senderPsa.udpPort() == recipientPsa.udpPort() && senderPsa2.udpPort() == recipientPsa2.udpPort()) {
			signalNAT("Port preserving NAT detected. UDP hole punching is possible", NATType.PORT_PRESERVING, fd);
		} else if (recipientPsa2.udpPort() - recipientPsa.udpPort() < SEQ_PORT_TOLERANCE) {
			signalNAT("NAT with sequential port multiplexing detected. UDP hole punching is still possible",
					NATType.NON_PRESERVING_SEQUENTIAL, fd);
		} else {
			signalNAT("Symmetric NAT detected (assumed since all other tests failed)", NATType.NON_PRESERVING_OTHER, fd);
		}
	}

	/**
	 * This method sets the {@link NATType} on the
	 * {@link HolePunchInitiatorImpl} object.
	 * 
	 * @param debugMsg
	 * @param natType
	 * @param fd
	 */
	private void signalNAT(final String debugMsg, final NATType natType, final FutureDone<NATType> fd) {
		LOG.debug(debugMsg);
		this.natType = natType;
		fd.done(natType);
	}
}