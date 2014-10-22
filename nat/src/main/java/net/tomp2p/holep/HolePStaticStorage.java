package net.tomp2p.holep;

import java.util.HashMap;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class HolePStaticStorage {

	private static final HolePStaticStorage instance = new HolePStaticStorage();
	private static final HashMap<Number160, PeerAddress> peerAddresses = new HashMap<Number160, PeerAddress>();
	
	private HolePStaticStorage() {
		
	}
	
	public static HolePStaticStorage instance() {
		return instance;
	}
	
	public static HashMap<Number160, PeerAddress> peerAdresses() {
		return peerAddresses;
	}
}
