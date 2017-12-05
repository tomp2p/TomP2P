package net.tomp2p.holep.example;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.Random;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public abstract class AbstractPeer {

	protected static final String HELLO_WORLD = 
			"  _    _ ______ _      _      ____   __          ______  _____  _      _____  \n" + 
			" | |  | |  ____| |    | |    / __ \\  \\ \\        / / __ \\|  __ \\| |    |  __ \\ \n" + 
			" | |__| | |__  | |    | |   | |  | |  \\ \\  /\\  / / |  | | |__) | |    | |  | |\n" + 
			" |  __  |  __| | |    | |   | |  | |   \\ \\/  \\/ /| |  | |  _  /| |    | |  | |\n" + 
			" | |  | | |____| |____| |___| |__| |    \\  /\\  / | |__| | | \\ \\| |____| |__| |\n" + 
			" |_|  |_|______|______|______\\____/      \\/  \\/   \\____/|_|  \\_\\______|_____/ \n" + 
			"                                                                              \n" + 
			"                                                                              ";
	protected static final String MASTER_SEED = "master";
	protected static final String UNREACHABLE_1_SEED = "u1";
	protected static final String UNREACHABLE_2_SEED = "u2";
	
	protected final InetSocketAddress local;
	protected final Number160 masterPeerId;
	protected final Number160 unreachablePeerId1;
	protected final Number160 unreachablePeerId2;
	
	public AbstractPeer(InetSocketAddress local) {
		this.local = local;
		this.masterPeerId = Number160.createHash(MASTER_SEED);
		this.unreachablePeerId1 = Number160.createHash(UNREACHABLE_1_SEED);
		this.unreachablePeerId2 = Number160.createHash(UNREACHABLE_2_SEED);
	}
}
