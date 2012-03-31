package net.tomp2p.replication;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapChangeListener;
import net.tomp2p.storage.ReplicationStorage;

/**
 * This class has 3 methods that are called from outside eventes: check,
 * peerInsert, peerRemoved.
 */
public class Replication implements PeerMapChangeListener
{
	final private List<ResponsibilityListener> listeners = new ArrayList<ResponsibilityListener>();
	final private PeerMap peerMap;
	final private PeerAddress selfAddress;
	final private ReplicationStorage replicationStorage;

	public Replication(ReplicationStorage replicationStorage, PeerAddress selfAddress, PeerMap peerMap)
	{
		this.replicationStorage = replicationStorage;
		this.selfAddress = selfAddress;
		this.peerMap = peerMap;
		peerMap.addPeerMapChangeListener(this);
	}

	public boolean isReplicationEnabled()
	{
		return peerMap != null && selfAddress != null;
	}

	public void addResponsibilityListener(ResponsibilityListener responsibilityListener)
	{
		listeners.add(responsibilityListener);
	}

	public void removeResponsibilityListener(ResponsibilityListener responsibilityListener)
	{
		listeners.remove(responsibilityListener);
	}

	public void checkResponsibility(Number160 locationKey)
	{
		if (!isReplicationEnabled())
			return;
		PeerAddress closest = closest(locationKey);
		if (closest.getID().equals(selfAddress.getID()))
		{
			// if(peerMap.isCloser(locationKey, key1, key2)

			if (replicationStorage.updateResponsibilities(locationKey, closest.getID()))
				notifyMeResponsible(locationKey);
		}
		else
		{
			if (replicationStorage.updateResponsibilities(locationKey, closest.getID()))
				// notify that someone else is now responsible for the
				// content with key responsibleLocations
				notifyOtherResponsible(locationKey, closest);
		}
	}

	public void updatePeerMapIfCloser(Number160 locationKey, Number160 current)
	{
		// we need to exclude ourselfs to get the "I'm responsible" notification
		if (!isReplicationEnabled() || current.equals(selfAddress.getID()))
			return;
		Number160 test = replicationStorage.findPeerIDForResponsibleContent(locationKey);
		if (test == null || peerMap.isCloser(locationKey, current, test) == -1)
		{
			replicationStorage.updateResponsibilities(locationKey, current);
		}
	}

	@Override
	public void peerInserted(PeerAddress peerAddress)
	{
		if (!isReplicationEnabled())
			return;
		// check if we should change responibility.
		Collection<Number160> myResponsibleLocations = replicationStorage.findContentForResponsiblePeerID(selfAddress
				.getID());
		for (Number160 myResponsibleLocation : myResponsibleLocations)
		{
			PeerAddress closest = closest(myResponsibleLocation);
			if (!closest.getID().equals(selfAddress.getID()))
			{
				if (replicationStorage.updateResponsibilities(myResponsibleLocation, closest.getID()))
					// notify that someone else is now responsible for the
					// content with key responsibleLocations
					notifyOtherResponsible(myResponsibleLocation, closest);
			}
		}
	}

	@Override
	public void peerRemoved(PeerAddress peerAddress)
	{
		if (!isReplicationEnabled())
			return;
		// check if we should change responibility.
		Collection<Number160> otherResponsibleLocations = replicationStorage.findContentForResponsiblePeerID(peerAddress
				.getID());
		if (otherResponsibleLocations == null)
			return;
		for (Number160 otherResponsibleLocation : otherResponsibleLocations)
		{
			PeerAddress closest = closest(otherResponsibleLocation);
			if (closest.getID().equals(selfAddress.getID()))
			{
				if (replicationStorage.updateResponsibilities(otherResponsibleLocation, closest.getID()))
					// notify that someone I'm now responsible for the
					// content
					// with key responsibleLocations
					notifyMeResponsible(otherResponsibleLocation);
			}
		}
	}

	@Override
	public void peerUpdated(PeerAddress peerAddress)
	{
	}

	private void notifyMeResponsible(Number160 locationKey)
	{
		for (ResponsibilityListener responsibilityListener : listeners)
		{
			responsibilityListener.meResponsible(locationKey);
		}
	}

	private void notifyOtherResponsible(Number160 locationKey, PeerAddress other)
	{
		for (ResponsibilityListener responsibilityListener : listeners)
		{
			responsibilityListener.otherResponsible(locationKey, other);
		}
	}

	private PeerAddress closest(Number160 locationKey)
	{
		SortedSet<PeerAddress> tmp = peerMap.closePeers(locationKey, 1);
		tmp.add(selfAddress);
		return tmp.iterator().next();
	}
}
