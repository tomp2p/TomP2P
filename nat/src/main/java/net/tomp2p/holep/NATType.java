package net.tomp2p.holep;

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
	UNKNOWN,
	/**
	 * NO_NAT means that we aren't using any NAT at all.
	 */
	NO_NAT,
	/**
	 * PORT_PRESERVING means, that the NAT we're using does map the same port
	 * numbers to its outside (public) address as we use for our private
	 * address.
	 */
	PORT_PRESERVING,
	/**
	 * NON_PRESERVING_SEQUENTIAL means, that a NAT will assign port number to
	 * its public endpoint in an continuous increasing manner (e.g. 1234).
	 */
	NON_PRESERVING_SEQUENTIAL,
	/**
	 * NON_PRESERVING_OTHER means that the NAT will assign the port mappings to
	 * its public endpoint in a random way.
	 */
	NON_PRESERVING_OTHER;
}
