/*
 * @(#) $CVSHeader:  $
 *
 * Copyright (C) 2011 by Netcetera AG.
 * All rights reserved.
 *
 * The copyright to the computer program(s) herein is the property of
 * Netcetera AG, Switzerland.  The program(s) may be used and/or copied
 * only with the written permission of Netcetera AG or in accordance
 * with the terms and conditions stipulated in the agreement/contract
 * under which the program(s) have been supplied.
 *
 * @(#) $Id: codetemplates.xml,v 1.5 2004/06/29 12:49:49 hagger Exp $
 */
package net.tomp2p.storage;

import net.tomp2p.peers.PeerAddress;

public class TrackerData implements Comparable<TrackerData>
{
	final private PeerAddress peerAddress;
	final private PeerAddress referrer;
	final private byte[] attachement;
	final private int offset;
	final private int length;

	public TrackerData(PeerAddress peerAddress, PeerAddress referrer, byte[] attachement, int offset, int legth)
	{
		this.peerAddress = peerAddress;
		this.referrer = referrer;
		this.attachement = attachement;
		this.offset = offset;
		this.length = legth;
	}

	public PeerAddress getPeerAddress()
	{
		return peerAddress;
	}

	public PeerAddress getReferrer()
	{
		return referrer;
	}

	public byte[] getAttachement()
	{
		return attachement;
	}

	public int getOffset()
	{
		return offset;
	}

	public int getLength()
	{
		return length;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (!(obj instanceof TrackerData))
			return false;
		return ((TrackerData) obj).getPeerAddress().getID().equals(getPeerAddress().getID());
	}

	@Override
	public int compareTo(TrackerData o)
	{
		return getPeerAddress().getID().compareTo(o.getPeerAddress().getID());
	}
	
	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("p:").append(peerAddress).append(",l:").append(length);
		return sb.toString();
	}
}
