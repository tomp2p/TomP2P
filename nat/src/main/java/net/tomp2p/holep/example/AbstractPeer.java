package net.tomp2p.holep.example;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.Random;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public abstract class AbstractPeer {

	
	protected static final Random RND = new Random(new Date().getTime());
	protected static final String MASTER_SEED = "master";
	protected final InetSocketAddress local;
	protected final PeerAddress masterPeerAddress;
	protected final Number160 peerId;
	
	public AbstractPeer(InetSocketAddress local) {
		this.local = local;
		this.masterPeerAddress = PeerAddress.create(Number160.createHash(MASTER_SEED), local);
		this.peerId = Number160.createHash(RND.nextInt());
	}
}
