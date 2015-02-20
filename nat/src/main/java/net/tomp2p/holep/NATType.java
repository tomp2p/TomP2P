package net.tomp2p.holep;

import net.tomp2p.holep.strategy.NonPreservingSequentialStrategy;
import net.tomp2p.holep.strategy.PortPreservingStrategy;
import net.tomp2p.message.Message;
import net.tomp2p.p2p.Peer;

/**
 * This class is responsible for two things. First, it holds all known NATTypes
 * which exist in TomP2P. Second, it creates the correct HolePuncher
 * object to connect to other peers.
 * 
 * @author Jonas Wagner
 */

public enum NATType {
	UNKNOWN {
		@Override
		public HolePuncherStrategy getHolePuncher(final Peer peer, int numberOfHoles, final int idleUDPSeconds,
				final Message originalMessage) {
			return new PortPreservingStrategy(peer, numberOfHoles, idleUDPSeconds, originalMessage);
		}
	},
	NO_NAT {
		@Override
		public HolePuncherStrategy getHolePuncher(final Peer peer, int numberOfHoles, final int idleUDPSeconds,
				final Message originalMessage) {
			return new PortPreservingStrategy(peer, numberOfHoles, idleUDPSeconds, originalMessage);
		}
	},
	PORT_PRESERVING {
		@Override
		public HolePuncherStrategy getHolePuncher(final Peer peer, int numberOfHoles, final int idleUDPSeconds,
				final Message originalMessage) {
			return new PortPreservingStrategy(peer, numberOfHoles, idleUDPSeconds, originalMessage);
		}
	}, // NAT takes the same port as source port on peer
	NON_PRESERVING_SEQUENTIAL {
		@Override
		public HolePuncherStrategy getHolePuncher(final Peer peer, int numberOfHoles, final int idleUDPSeconds,
				final Message originalMessage) {
			return new NonPreservingSequentialStrategy(peer, numberOfHoles, idleUDPSeconds, originalMessage);
		}
	}, // NAT assigns new port for each mapping starting at a defined number,
		// and increasing by one (e.g. 1234).
	NON_PRESERVING_OTHER {
		@Override
		public HolePuncherStrategy getHolePuncher(final Peer peer, int numberOfHoles, final int idleUDPSeconds,
				final Message originalMessage) {
			return null;
		}
	};

	public abstract HolePuncherStrategy getHolePuncher(final Peer peer, int numberOfHoles,
			final int idleUDPSeconds, final Message originalMessage);
}
