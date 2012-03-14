package net.tomp2p.p2p;

public class P2PConfiguration
{
	private final int[] waitingTimeBetweenNodeMaintenenceSeconds = { 5, 10, 20, 40, 80, 160 };
	// cannot be changed during runtime
	private final int maintenanceThreads;
	private final int replicationThreads;
	private boolean startMaintenance = true;
	// peermap configuration
	private int bagSize = 2;
	private int cacheSize = 100;
	private int cacheTimeoutMillis = 60 * 1000;
	// indirect replication
	private int replicationRefreshMillis=60 * 1000;
	// discover timeout
	private int discoverTimeoutSec=5;
	private int maxNrBeforeExclude = 2;
	// The default is not to assume that you are behind firewall
	private boolean behindFirewall=false;
	private int trackerTimoutSeconds=60;
	private boolean disableBind = false;
	//disabel or enable the limitation of tracker results. If set to true, the tracker will return 35 entries. If set to false, it will return all of them.
	private boolean limitTracker = true;
	public P2PConfiguration()
	{
		this.maintenanceThreads = 5;
		replicationThreads=5;
	}
	
	public P2PConfiguration(int maintenanceThreads, int replicationThreads, boolean startMaintenance)
	{
		this.maintenanceThreads = maintenanceThreads;
		this.replicationThreads = replicationThreads;
		this.startMaintenance = startMaintenance;
	}
	
	public int[] getWaitingTimeBetweenNodeMaintenenceSeconds()
	{
		return waitingTimeBetweenNodeMaintenenceSeconds;
	}

	public int getMaintenanceThreads()
	{
		return maintenanceThreads;
	}

	public void setStartMaintenance(boolean startMaintenance)
	{
		this.startMaintenance = startMaintenance;
	}

	public boolean isStartMaintenance()
	{
		return startMaintenance;
	}

	public void setBagSize(int bagSize)
	{
		this.bagSize = bagSize;
	}

	public int getBagSize()
	{
		return bagSize;
	}

	public void setCacheSize(int cacheSize)
	{
		this.cacheSize = cacheSize;
	}

	public int getCacheSize()
	{
		return cacheSize;
	}

	public void setCacheTimeoutMillis(int cacheTimeoutMillis)
	{
		this.cacheTimeoutMillis = cacheTimeoutMillis;
	}

	public int getCacheTimeoutMillis()
	{
		return cacheTimeoutMillis;
	}

	public int getReplicationThreads()
	{
		return replicationThreads;
	}

	public void setReplicationRefreshMillis(int replicationRefreshMillis)
	{
		this.replicationRefreshMillis = replicationRefreshMillis;
	}

	public int getReplicationRefreshMillis()
	{
		return replicationRefreshMillis;
	}
	
	public void setDiscoverTimeoutSec(int discoverTimeoutSec)
	{
		this.discoverTimeoutSec=discoverTimeoutSec;
	}

	public int getDiscoverTimeoutSec()
	{
		return discoverTimeoutSec;
	}
	
	public void setMaxNrBeforeExclude(int maxNrBeforeExclude)
	{
		this.maxNrBeforeExclude = maxNrBeforeExclude;
	}

	public int getMaxNrBeforeExclude()
	{
		return maxNrBeforeExclude;
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
}
