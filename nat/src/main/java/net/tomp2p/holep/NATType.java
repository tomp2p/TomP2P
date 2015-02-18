package net.tomp2p.holep;

/**
 * 
 * @author jonaswagner
 *
 */

public enum NATType{
	UNKNOWN,
	NO_NAT,
	PORT_PRESERVING, //NAT takes the same port as source port on peer
	NON_PRESERVING_SEQUENTIAL, //NAT assigns new port for each mapping starting at a defined number, and increasing by one (e.g. 1234).
	NON_PRESERVING_OTHER //NAT assigns a new (random or other) port for each mapping.
;

}
