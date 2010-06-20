package net.tomp2p.storage;
import net.tomp2p.peers.Number160;

public class DataSet
{
	final private Data data;
	final private Number160 contentKey;
	final private Number160 domainKey;
	final private Number160 locationKey;

	public DataSet(Data data, Number160 contentKey, Number160 domainKey, Number160 locationKey)
	{
		this.data = data;
		this.contentKey = contentKey;
		this.domainKey = domainKey;
		this.locationKey = locationKey;
	}

	public Data getData()
	{
		return data;
	}

	public Number160 getContentKey()
	{
		return contentKey;
	}

	public Number160 getDomainKey()
	{
		return domainKey;
	}

	public Number160 getLocationKey()
	{
		return locationKey;
	}

	@Override
	public int hashCode()
	{
		return contentKey.hashCode() ^ domainKey.hashCode() ^ locationKey.hashCode();
	}

	@Override
	public boolean equals(Object obj)
	{
		if (!(obj instanceof DataSet))
			return false;
		DataSet ds = (DataSet) obj;
		return contentKey.equals(ds.contentKey) && domainKey.equals(ds.domainKey)
				&& locationKey.equals(ds.locationKey);
	}
}
