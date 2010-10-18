package net.tomp2p.peers;
public class Number480 implements Comparable<Number480>
{
	private final Number160 locationKey;
	private final Number160 domainKey;
	private final Number160 contentKey;

	public Number480(Number160 locationKey, Number160 domainKey, Number160 contentKey)
	{
		if (locationKey == null)
			throw new RuntimeException("locationKey cannot be null");
		this.locationKey = locationKey;
		if (domainKey == null)
			throw new RuntimeException("domainKey cannot be null");
		this.domainKey = domainKey;
		if (contentKey == null)
			throw new RuntimeException("contentKey cannot be null");
		this.contentKey = contentKey;
	}

	public Number480(Number320 key, Number160 contentKey)
	{
		this(key.getLocationKey(), key.getDomainKey(), contentKey);
	}

	public Number160 getLocationKey()
	{
		return locationKey;
	}

	public Number160 getDomainKey()
	{
		return domainKey;
	}

	public Number160 getContentKey()
	{
		return contentKey;
	}

	@Override
	public int hashCode()
	{
		return locationKey.hashCode() ^ domainKey.hashCode() ^ contentKey.hashCode();
	}

	@Override
	public boolean equals(Object obj)
	{
		if (!(obj instanceof Number480))
			return false;
		Number480 cmp = (Number480) obj;
		return locationKey.equals(cmp.locationKey) && domainKey.equals(cmp.domainKey)
				&& contentKey.equals(cmp.contentKey);
	}

	@Override
	public int compareTo(Number480 o)
	{
		int diff = locationKey.compareTo(o.locationKey);
		if (diff != 0)
			return diff;
		diff = domainKey.compareTo(o.domainKey);
		if (diff != 0)
			return diff;
		return contentKey.compareTo(o.contentKey);
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder("[");
		sb.append(locationKey.toString()).append(",");
		sb.append(domainKey.toString()).append(",");
		sb.append(contentKey.toString()).append("]");
		return sb.toString();
	}
}
