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
}
