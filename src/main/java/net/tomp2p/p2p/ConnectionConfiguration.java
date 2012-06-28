package net.tomp2p.p2p;

public class ConnectionConfiguration
{
	// discover timeout
	private int discoverTimeoutSec=5;
	//private int maxNrBeforeExclude = 2;
	// The default is not to assume that you are behind firewall
	private boolean behindFirewall=false;
	private int trackerTimoutSeconds=60;
	private boolean disableBind = false;
	//disabel or enable the limitation of tracker results. If set to true, the tracker will return 35 entries. If set to false, it will return all of them.
	private boolean limitTracker = true;
	
	//connection configuration
	// idle needs to be larger than timeout for TCP
	
	// doing tests on localhost, we open 2 * maxOpenConnection
	private int maxOpenConnection = 400;
	private int maxCreating = 100;
	// these values depend on how many connections we create
	private int idleTCPMillis = (maxOpenConnection + maxCreating) * 20;
	private int idleUDPMillis = (maxOpenConnection + maxCreating) * 10;
	private int connectTimeouMillis = (maxOpenConnection + maxCreating) * 10;
	
	// force TCP or UDP
	private boolean forceTrackerTCP = false;
	private boolean forceStorageUDP = false;
	
	public void setDiscoverTimeoutSec(int discoverTimeoutSec)
	{
		this.discoverTimeoutSec=discoverTimeoutSec;
	}

	public int getDiscoverTimeoutSec()
	{
		return discoverTimeoutSec;
	}
	
	/**
	 * By setting this flag, the peer assumes that it is behind a firewall and
	 * will announce itself as unreachable. As soon as this peer receives an
	 * incoming message from its advertised address, the peer marks itself as
	 * reachable. To receive an incoming message, the peer has to call
	 * {@link Peer#discover(net.tomp2p.peers.PeerAddress)} to mark itself as
	 * reachable.
	 * 
	 * @param behindFirewall If set to true, peer is assumed to be behind
	 *        firewall and is unreable.
	 */
	public void setBehindFirewall(boolean behindFirewall)
	{
		this.behindFirewall = behindFirewall;
	}

	public boolean isBehindFirewall()
	{
		return behindFirewall;
	}

	public int getTrackerTimoutSeconds()
	{
		return trackerTimoutSeconds;
	}

	public void setTrackerTimoutSeconds(int trackerTimoutSeconds)
	{
		this.trackerTimoutSeconds = trackerTimoutSeconds;
	}

	public boolean isDisableBind()
	{
		return disableBind;
	}

	public void setDisableBind(boolean disableBind)
	{
		this.disableBind = disableBind;
	}

	public boolean isLimitTracker()
	{
		return limitTracker;
	}

	public void setLimitTracker(boolean limitTracker)
	{
		this.limitTracker = limitTracker;
	}
	
	public int getIdleTCPMillis()
	{
		return idleTCPMillis;
	}

	public void setIdleTCPMillis(int idleTCPMillis)
	{
		this.idleTCPMillis = idleTCPMillis;
	}

	public int getIdleUDPMillis()
	{
		return idleUDPMillis;
	}

	public void setIdleUDPMillis(int idleUDPMillis)
	{
		this.idleUDPMillis = idleUDPMillis;
	}

	public int getConnectTimeoutMillis()
	{
		return connectTimeouMillis;
	}

	public void setConnectTimeoutMillis(int connectTimeouMillist)
	{
		this.connectTimeouMillis = connectTimeouMillist;
	}

	public void setMaxOpenConnection(int maxOpenConnection)
	{
		this.maxOpenConnection = maxOpenConnection;
	}

	public int getMaxOpenConnection()
	{
		return maxOpenConnection;
	}

	public int getMaxCreating()
	{
		return maxCreating;
	}

	public void setMaxCreating(int maxCreating)
	{
		this.maxCreating = maxCreating;
	}

	public boolean isForceTrackerTCP()
	{
		return forceTrackerTCP;
	}

	public void setForceTrackerTCP(boolean forceTrackerTCP)
	{
		this.forceTrackerTCP = forceTrackerTCP;
	}

	public boolean isForceStorageUDP()
	{
		return forceStorageUDP;
	}

	public void setForceStorageUDP(boolean forceStorageUDP)
	{
		this.forceStorageUDP = forceStorageUDP;
	}
}
