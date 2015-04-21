package net.tomp2p.holep;

import java.util.Iterator;

import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.futures.Futures;
import net.tomp2p.message.NeighborSet;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for finding out the NATType which the peer is
 * using.
 * 
 * @author jonaswagner
 * 
 */
public class NATTypeDetection {

	private static final Logger LOG = LoggerFactory.getLogger(NATTypeDetection.class);
	private static final int SEQ_PORT_TOLERANCE = 5;
	private final Peer peer;

	public NATTypeDetection(final Peer peer) {
		this.peer = peer;
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
	 * This method is always executed twice in a recursive manner. It contacts a
	 * given relay {@link Peer} on it's {@link PeerAddress}. The relay peer then
	 * simply returns the port number and the IP address the requesting peer was
	 * contacting from.
	 * 
	 * @param relayPeer
	 *            The {@link PeerAddress} of a relay peer.
	 * @return futureDone A FutureDone to check if the method succeded.
	 */
	public FutureDone<NATType> checkNATType(final PeerAddress relayPeer) {
		final FutureDone<NATType> futureDone = new FutureDone<NATType>();
		final FutureChannelCreator fcc1 = peer.connectionBean().reservation().create(1, 0);
		fcc1.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
			@Override
			public void operationComplete(FutureChannelCreator future) throws Exception {
				if (future.isSuccess()) {
					
					FutureResponse futureResponse1 =  peer.pingRPC().pingUDPDiscover(relayPeer, future.channelCreator(),
							new DefaultConnectionConfiguration());
					FutureResponse futureResponse2 =  peer.pingRPC().pingUDPDiscover(relayPeer, future.channelCreator(),
							new DefaultConnectionConfiguration());
					
					FutureDone<FutureResponse[]> fdd = Futures.whenAllSuccess(futureResponse1, futureResponse2);
					Utils.addReleaseListener(fcc1, fdd);
					fdd.addListener(new BaseFutureAdapter<FutureDone<FutureResponse[]>>() {

						@Override
                        public void operationComplete(FutureDone<FutureResponse[]> future) throws Exception {
	                        if(future.isSuccess()) {
	                        	if(future.object().length !=2) {
	                        		futureDone.failed("expected exactly two futures");
	                        		return;
	                        	}
	                        	final int port1 = future.object()[0].responseMessage().intAt(0);
	                        	final int port2 = future.object()[1].responseMessage().intAt(0);
	                        	
	                        	//NATType natType = checkNATType(relayPeer, pa2.peerSocketAddress(), pa4.peerSocketAddress());
	                        	//futureDone.done(natType);
	                        	futureDone.failed("not implemented yet");
	                        } else {
	                        	futureDone.failed("expected two successful futures", future);
	                        }
                        }
					});
					
					
				} else {
					futureDone.failed("Could not emit NAT type! Channel creation failed", future);
				}
			}
		});
		return futureDone;
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
	private NATType checkNATType(final PeerSocketAddress senderPsa, final PeerSocketAddress recipientPsa,
			final PeerSocketAddress senderPsa2, final PeerSocketAddress recipientPsa2) {
		if (peer.peerAddress().peerSocketAddress().inetAddress().equals(recipientPsa.inetAddress())) {
			LOG.debug("there is no NAT to be traversed!");
			return NATType.NO_NAT;
		} else if (senderPsa.udpPort() == recipientPsa.udpPort() && senderPsa2.udpPort() == recipientPsa2.udpPort()) {
			LOG.debug("Port preserving NAT detected. UDP hole punching is possible");
			return NATType.PORT_PRESERVING;
		} else if (Math.abs(recipientPsa2.udpPort() - recipientPsa.udpPort()) < SEQ_PORT_TOLERANCE) {
			LOG.debug("NAT with sequential port multiplexing detected. UDP hole punching is still possible");
			return NATType.NON_PRESERVING_SEQUENTIAL;
		} else {
			LOG.debug("Symmetric NAT detected (assumed since all other tests failed)");
			return NATType.NON_PRESERVING_OTHER;
		}
	}
}