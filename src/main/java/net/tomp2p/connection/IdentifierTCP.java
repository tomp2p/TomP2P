package net.tomp2p.connection;

import java.net.InetAddress;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class IdentifierTCP implements Comparable<IdentifierTCP>
{
	public static final String DEFAULT = "default-tcp";
	final private Number160 recipientId;
	final private Number160 senderId;
	final private Number160 both;
	final private InetAddress inetAddress;
	final private String channelName;
	final private long timestamp;

	public IdentifierTCP(Number160 recipientId, Number160 senderId, InetAddress inetAddress,
			String channelName)
	{
		this.recipientId = recipientId;
		this.senderId = senderId;
		this.both = senderId.xor(recipientId);
		this.inetAddress = inetAddress;
		this.channelName = channelName;
		this.timestamp=System.currentTimeMillis();
	}
	
	public IdentifierTCP(PeerAddress remotePeer, PeerAddress senderPeer, String channelName)
	{
		this.recipientId = remotePeer.getID();
		this.senderId = senderPeer.getID();
		this.both = senderId.xor(recipientId);
		this.inetAddress = remotePeer.getInetAddress();
		this.channelName = channelName;
		this.timestamp=System.currentTimeMillis();
	}

	@Override
	public int hashCode()
	{
		return both.hashCode() ^ inetAddress.hashCode() ^ channelName.hashCode();
	}

	@Override
	public boolean equals(Object obj)
	{
		if (!(obj instanceof IdentifierTCP))
			return false;
		IdentifierTCP i = (IdentifierTCP) obj;
		return i.both.equals(both) && i.inetAddress.equals(inetAddress)
				&& i.channelName.equals(channelName);
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder("recipientID:");
		sb.append(recipientId).append(",senderID:").append(senderId).append(",inet:").append(
				inetAddress).append(",name:").append(channelName);
		return sb.toString();
	}

	@Override
	public int compareTo(IdentifierTCP o) {
		long diff = timestamp-o.timestamp;
		if(diff>0) return 1;
		else if(diff<0) return -1;
		else return recipientId.compareTo(o.recipientId);
	}
}
