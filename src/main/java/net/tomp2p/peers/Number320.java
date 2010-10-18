package net.tomp2p.peers;
public class Number320 implements Comparable<Number320>
{
	private final Number160 locationKey;
	private final Number160 domainKey;

	public Number320(Number160 locationKey, Number160 domainKey)
	{
		this.locationKey = locationKey;
		this.domainKey = domainKey;
	}

	public Number160 getLocationKey()
	{
		return locationKey;
	}

	public Number160 getDomainKey()
	{
		return domainKey;
	}

	@Override
	public int hashCode()
	{
		return locationKey.hashCode() ^ domainKey.hashCode();
	}

	@Override
	public boolean equals(Object obj)
	{
		if (!(obj instanceof Number320))
			return false;
		Number320 cmp = (Number320) obj;
		return locationKey.equals(cmp.locationKey) && domainKey.equals(cmp.domainKey);
	}

	@Override
	public int compareTo(Number320 o)
	{
		int diff = locationKey.compareTo(o.locationKey);
		if (diff != 0)
			return diff;
		return domainKey.compareTo(o.domainKey);
	}
	
	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder("[");
		sb.append(locationKey.toString()).append(",");
		sb.append(domainKey.toString()).append("]");
		return sb.toString();
	}
	
	public Number480 min()
	{
		return new Number480(locationKey, domainKey, Number160.ZERO);
	}
	
	public Number480 max()
	{
		return new Number480(locationKey, domainKey, Number160.MAX_VALUE);
	}
	
}
