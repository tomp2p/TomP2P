package net.tomp2p.connection;

import java.net.InetSocketAddress;

import net.tomp2p.peers.PeerAddress;

public interface RelaySender {
	
	InetSocketAddress createSocketUPD(PeerAddress peerAddress);
	InetSocketAddress createSocketTCP(PeerAddress peerAddress);

}
