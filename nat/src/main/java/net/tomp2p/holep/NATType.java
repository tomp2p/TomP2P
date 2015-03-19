package net.tomp2p.holep;

import net.tomp2p.holep.strategy.HolePStrategy;
import net.tomp2p.holep.strategy.NonPreservingSequentialStrategy;
import net.tomp2p.holep.strategy.PortPreservingStrategy;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;

/**
 * This class is responsible for two things. First, it holds all known NATTypes
 * which exist in TomP2P. Second, it creates the correct HolePuncher object to
 * connect to other peers.
 * 
 * @author Jonas Wagner
 */

public enum NATType {
	/**
	 * UNKNOWN means, that we do not know anything about the NAT we're using. We
	 * also don't know if we even use a NAT.
	 */
	UNKNOWN {
		@Override
		public HolePStrategy holePuncher(final Peer peer, int numberOfHoles, final int idleUDPSeconds, final Message originalMessage) {
			return new PortPreservingStrategy(peer, numberOfHoles, idleUDPSeconds, originalMessage);
		}
	},
	/**
	 * NO_NAT means that we aren't using any NAT at all.
	 */
	NO_NAT {
		@Override
		public HolePStrategy holePuncher(final Peer peer, int numberOfHoles, final int idleUDPSeconds, final Message originalMessage) {
			return new PortPreservingStrategy(peer, numberOfHoles, idleUDPSeconds, originalMessage);
		}
	},
	/**
	 * PORT_PRESERVING means, that the NAT we're using does map the same port
	 * numbers to its outside (public) address as we use for our private
	 * address.
	 */
	PORT_PRESERVING {
		@Override
		public HolePStrategy holePuncher(final Peer peer, int numberOfHoles, final int idleUDPSeconds, final Message originalMessage) {
			return new PortPreservingStrategy(peer, numberOfHoles, idleUDPSeconds, originalMessage);
		}
	},
	/**
	 * NON_PRESERVING_SEQUENTIAL means, that a NAT will assign port number to
	 * its public endpoint in an continous increasing manner (e.g. 1234).
	 */
	NON_PRESERVING_SEQUENTIAL {
		@Override
		public HolePStrategy holePuncher(final Peer peer, int numberOfHoles, final int idleUDPSeconds, final Message originalMessage) {
			return new NonPreservingSequentialStrategy(peer, numberOfHoles, idleUDPSeconds, originalMessage);
		}
	},
	/**
	 * NON_PRESERVING_OTHER means that the NAT will assign the port mappings to
	 * its public endpoint in a random way.
	 */
	NON_PRESERVING_OTHER {
		@Override
		public HolePStrategy holePuncher(final Peer peer, int numberOfHoles, final int idleUDPSeconds, final Message originalMessage) {
			return null; //there is currently no strategy which can handle such a NATType
		}
	};

	/**
	 * This method makes sure, that the correct Strategy is created for each
	 * NATType.
	 * 
	 * @param peer
	 * @param numberOfHoles
	 * @param idleUDPSeconds
	 * @param originalMessage
	 * @return holePStrategy
	 */
	public abstract HolePStrategy holePuncher(final Peer peer, int numberOfHoles, final int idleUDPSeconds, final Message originalMessage);
}
