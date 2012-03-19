package net.tomp2p.p2p;

import java.io.File;
import java.security.KeyPair;

import net.tomp2p.connection.Bindings;
import net.tomp2p.peers.Number160;
import net.tomp2p.utils.Utils;

public class PeerMaker
{
	final private static KeyPair EMPTY_KEYPAIR = new KeyPair(null, null);
	//required
	final private Number160 peerId;
	//optional with reasonable defaults
	private int p2pID = 1;
	private KeyPair keyPair = EMPTY_KEYPAIR;
	private int workerThreads = Runtime.getRuntime().availableProcessors() + 1;
	//
	private int maintenanceThreads = 5;
	private int replicationThreads = 5;
	private boolean startMaintenance = true;
	// indirect replication
	private int replicationRefreshMillis=60 * 1000;
	private int tcpPort = Peer.DEFAULT_PORT;
	private int udpPort = Peer.DEFAULT_PORT;
	private Bindings bindings = new Bindings();
	private File fileMessageLogger = null;
	private Configuration configuration = new Configuration();
	
	public PeerMaker(Number160 peerId)
	{
		this.peerId = peerId;
	}
	
	public PeerInitializer build() throws Exception
	{
		Peer peer = new Peer(p2pID, peerId, keyPair, getWorkerThreads(), getMaintenanceThreads(), 
				getReplicationThreads(), isStartMaintenance(), getReplicationRefreshMillis(), getConfiguration());
		return new PeerInitializer(peer, getUdpPort(), getTcpPort(), getBindings(), getFileMessageLogger());
	}
	
	public PeerMaker(final KeyPair keyPair)
	{
		this.peerId = Utils.makeSHAHash(keyPair.getPublic().getEncoded());
		this.keyPair = keyPair;
	}
	
	public PeerMaker setP2PId(int p2pID)
	{
		this.p2pID = p2pID;
		return this;
	}
	
	public int getP2PId()
	{
		return p2pID;
	}
	
	public PeerMaker setKeyPair(KeyPair keyPair)
	{
		this.keyPair = keyPair;
		return this;
	}
	
	public KeyPair getKeyPair()
	{
		return keyPair;
	}

	public int getWorkerThreads()
	{
		return workerThreads;
	}

	public PeerMaker setWorkerThreads(int workerThreads)
	{
		this.workerThreads = workerThreads;
		return this;
	}

	public int getMaintenanceThreads()
	{
		return maintenanceThreads;
	}

	public PeerMaker setMaintenanceThreads(int maintenanceThreads)
	{
		this.maintenanceThreads = maintenanceThreads;
		return this;
	}

	public int getReplicationThreads()
	{
		return replicationThreads;
	}

	public PeerMaker setReplicationThreads(int replicationThreads)
	{
		this.replicationThreads = replicationThreads;
		return this;
	}

	public boolean isStartMaintenance()
	{
		return startMaintenance;
	}

	public PeerMaker setStartMaintenance(boolean startMaintenance)
	{
		this.startMaintenance = startMaintenance;
		return this;
	}

	public int getReplicationRefreshMillis()
	{
		return replicationRefreshMillis;
	}

	public PeerMaker setReplicationRefreshMillis(int replicationRefreshMillis)
	{
		this.replicationRefreshMillis = replicationRefreshMillis;
		return this;
	}

	public int getTcpPort()
	{
		return tcpPort;
	}

	public PeerMaker setTcpPort(int tcpPort)
	{
		this.tcpPort = tcpPort;
		return this;
	}

	public int getUdpPort()
	{
		return udpPort;
	}

	public PeerMaker setUdpPort(int udpPort)
	{
		this.udpPort = udpPort;
		return this;
	}

	public Bindings getBindings()
	{
		return bindings;
	}

	public PeerMaker setBindings(Bindings bindings)
	{
		this.bindings = bindings;
		return this;
	}

	public File getFileMessageLogger()
	{
		return fileMessageLogger;
	}

	public PeerMaker setFileMessageLogger(File fileMessageLogger)
	{
		this.fileMessageLogger = fileMessageLogger;
		return this;
	}

	public Configuration getConfiguration()
	{
		return configuration;
	}

	public PeerMaker setConfiguration(Configuration configuration)
	{
		this.configuration = configuration;
		return this;
	}
}