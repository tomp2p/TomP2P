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
package net.tomp2p.rpc;

import java.util.SortedMap;

import net.tomp2p.peers.Number480;
import net.tomp2p.storage.Data;

public class TrackerData
{
	final private SortedMap<Number480, Data> peerDataMap;
	final private boolean couldProvideMoreData;
	public TrackerData(SortedMap<Number480, Data> peerDataMap, boolean couldProvideMoreData)
	{
		this.peerDataMap=peerDataMap;
		this.couldProvideMoreData=couldProvideMoreData;
	}
	public SortedMap<Number480, Data> getPeerDataMap()
	{
		return peerDataMap;
	}
	public boolean couldProvideMoreData()
	{
		return couldProvideMoreData;
	}
	
}
