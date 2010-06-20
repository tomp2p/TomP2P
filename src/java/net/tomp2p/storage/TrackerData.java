package net.tomp2p.storage;
import java.io.Serializable;

import net.tomp2p.peers.PeerAddress;


public class TrackerData implements Serializable
{
	private static final long serialVersionUID = -1227213456149705603L;
	final private PeerAddress peerAddress;
	final private Data attachement;

	public TrackerData(PeerAddress peerAddress, Data attachement)
	{
		this.peerAddress = peerAddress;
		this.attachement = attachement;
	}

	public PeerAddress getPeerAddress()
	{
		return peerAddress;
	}

	public Data getAttachement()
	{
		return attachement;
	}
}
