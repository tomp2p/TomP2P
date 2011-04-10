package net.tomp2p.p2p.config;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureCreate;
import net.tomp2p.p2p.TrackerConfiguration;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

public class ConfigurationTrackerStore extends ConfigurationBase
{
	private Data attachement;
	private TrackerConfiguration trackerConfiguration;
	private FutureCreate<BaseFuture> futureCreate;
	// used for peer exchange
	private int waitBeforeNextSendSeconds=0;
	

	public ConfigurationTrackerStore setAttachement(Data attachement)
	{
		this.attachement = attachement;
		return this;
	}

	public Data getAttachement()
	{
		return attachement;
	}

	public ConfigurationTrackerStore setTrackerConfiguration(
			TrackerConfiguration trackerConfiguration)
	{
		this.trackerConfiguration = trackerConfiguration;
		return this;
	}

	public TrackerConfiguration getTrackerConfiguration()
	{
		return trackerConfiguration;
	}

	@Override
	public ConfigurationBase setContentKey(Number160 contentKey)
	{
		throw new UnsupportedOperationException("the tracker sets its own content key");
	}

	@Override
	public Number160 getContentKey()
	{
		throw new UnsupportedOperationException("the tracker sets its own content key");
	}

	public ConfigurationTrackerStore setFutureCreate(FutureCreate<BaseFuture> futureCreate)
	{
		this.futureCreate = futureCreate;
		return this;
	}

	public FutureCreate<BaseFuture> getFutureCreate()
	{
		return futureCreate;
	}

	public void setWaitBeforeNextSendSeconds(int waitBeforeNextSendSeconds)
	{
		this.waitBeforeNextSendSeconds = waitBeforeNextSendSeconds;
	}

	public int getWaitBeforeNextSendSeconds()
	{
		return waitBeforeNextSendSeconds;
	}
}
