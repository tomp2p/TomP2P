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
package net.tomp2p.replication;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.PeerExchangeRPC;
import net.tomp2p.storage.TrackerStorage;
import net.tomp2p.utils.Utils;

public class TrackerStorageReplication implements ResponsibilityListener
{
	final private static Logger logger = LoggerFactory.getLogger(TrackerStorageReplication.class);
	final private PeerExchangeRPC peerExchangeRPC;
	final private Map<BaseFuture, Long> pendingFutures;
	final private TrackerStorage trackerStorage;
	final private Peer peer;

	public TrackerStorageReplication(Peer peer, PeerExchangeRPC peerExchangeRPC, Map<BaseFuture, Long> pendingFutures,
			TrackerStorage trackerStorage)
	{
		this.peer = peer;
		this.peerExchangeRPC = peerExchangeRPC;
		this.pendingFutures = pendingFutures;
		this.trackerStorage = trackerStorage;
	}

	@Override
	public void meResponsible(Number160 locationKey)
	{
		// the other has to send us the data, so do nothing
	}

	@Override
	public void otherResponsible(final Number160 locationKey, final PeerAddress other)
	{
		// do pex here, but with mesh peers!
		if (logger.isDebugEnabled())
		{
			logger.debug("other peer became responsibel and we thought we were responsible, so move the data to this peer");
		}
		for (final Number160 domainKey : trackerStorage.responsibleDomains(locationKey))
		{
			final ChannelCreator cc=peer.getConnectionHandler().getConnectionReservation().reserve(1);
			FutureResponse futureResponse = peerExchangeRPC.peerExchange(other, locationKey, domainKey, true, cc);
			Utils.addReleaseListener(futureResponse, cc, 1);
			pendingFutures.put(futureResponse, System.currentTimeMillis());
		}
	}
}
