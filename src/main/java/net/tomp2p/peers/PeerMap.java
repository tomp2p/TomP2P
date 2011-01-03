/*
 * Copyright 2009 Thomas Bocek
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package net.tomp2p.peers;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Comparator;
import java.util.SortedSet;

import net.tomp2p.p2p.Statistics;

/**
 * Defines a map to store neighbor peers. The metric is defined in the
 * implementation.
 * 
 * @author Thomas Bocek
 * 
 */
public interface PeerMap
{
	/**
	 * Each node that has a bag has an ID itself to define what is close. This
	 * method returns this ID.
	 * 
	 * @return The id of this node
	 */
	public Number160 self();

	/**
	 * Adds a neighbor to the neighbor list. If the bag is full, the id zero or
	 * the same as our id, the neighbor is not added. This method is tread-safe
	 * 
	 * @param node The node that should be added
	 * @param firstHand If we had direct contact and we know for sure that this
	 *        node is online, we set firsthand to true. Information from 3rd
	 *        party peers are always second hand and treated as such
	 * @return True if the neighbor could be added, otherwise false.
	 */
	public boolean peerOnline(PeerAddress node, PeerAddress referer);

	/**
	 * Remove a peer from the list. In order to not reappear, the node is put
	 * for a certain time in a cache list to keep the node removed. This method
	 * is thread-safe.
	 * 
	 * @param node The node that should be removed
	 * @return True if the neighbor was removed and added to a cache list. False
	 *         if peer has not been removed or is already in the peer removed
	 *         temporarly list.
	 */
	public boolean peerOffline(PeerAddress node, boolean force);

	/**
	 * Checks if this peer has been removed. A peer that has been removed will
	 * be stored in a cache list for a certain time. This method is tread-safe
	 * 
	 * @param node The node to check
	 * @return True if the peer has been removed recently
	 */
	public boolean isPeerRemovedTemporarly(PeerAddress node);

	/**
	 * Returns close peer from the set to a given key. This method is
	 * tread-safe. You can use the returned set as its a copy of the actual
	 * PeerMap and changes in the return set do not affect PeerMap.
	 * 
	 * @param key The key that should be close to the keys in the map
	 * @param number The number we want to find at least
	 * @return A navigable set with close peers first in this set.
	 */
	public SortedSet<PeerAddress> closePeers(Number160 key, int number);

	public Collection<PeerAddress> peersForMaintenance();

	/**
	 * Returns -1 if the first remote node is closer to the key, if the second
	 * is closer, then 1 is returned. If both are equal, 0 is returned
	 * 
	 * @param key The key to search for
	 * @param nodeAddress1 The remote node on the routing path to node close to
	 *        key
	 * @param nodeAddress2 An other remote node on the routing path to node
	 *        close to key
	 * @return -1 if nodeAddress1 is closer to the key than nodeAddress2,
	 *         otherwise 1. 0 is returned if both are equal.
	 */
	public int isCloser(Number160 key, PeerAddress nodeAddress1, PeerAddress nodeAddress2);

	/**
	 * Returns -1 if the first key is closer to the key, if the second is
	 * closer, then 1 is returned. If both are equal, 0 is returned
	 * 
	 * @param key The key to search for
	 * @param key1 The first key
	 * @param key2 The second key
	 * @return -1 if key1 is closer to key, otherwise 1. 0 is returned if both
	 *         are equal.
	 */
	public int isCloser(Number160 key, Number160 key1, Number160 key2);

	/**
	 * Creates a comparator that orders to peers according to their distance to
	 * the specified id.
	 * 
	 * @param id The id that defines the metric
	 * @return The comparator to be used with the collection framework
	 */
	public Comparator<PeerAddress> createPeerComparator(Number160 id);

	public Comparator<PeerAddress> createPeerComparator();

	public int size();

	public boolean contains(PeerAddress peerAddress);

	public void addPeerMapChangeListener(PeerMapChangeListener peerMapChangeListener);

	public void removePeerMapChangeListener(PeerMapChangeListener peerMapChangeListener);

	public void addPeerOfflineListener(PeerOfflineListener peerListener);

	public void removePeerOfflineListener(PeerOfflineListener peerListener);

	/**
	 * Return all addresses from the neighbor list. The collection is partially
	 * sorted.
	 * 
	 * @return All neighbors
	 */
	public Collection<PeerAddress> getAll();

	public void addAddressFilter(InetAddress address);
	
	public Statistics getStatistics();
}