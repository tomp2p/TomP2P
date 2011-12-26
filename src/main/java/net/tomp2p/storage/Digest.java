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
package net.tomp2p.storage;

import java.util.Collection;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number320;
import net.tomp2p.rpc.DigestInfo;

/**
 * The storage stores Number480, which is separated in 3 Number160. The first
 * Number160 is the location key, the second is the domain key, and the third is
 * the content key. A digest can be calculated over all content keys for a
 * specific location and domain. The digest can also be calculated over a
 * selected range of content keys for a specific location and domain.
 * 
 * @author Thomas Bocek
 * 
 */
public interface Digest
{
	/**
	 * Calculates a digest over a specific location and domain. It will return
	 * those content keys that are stored.
	 * 
	 * @param key The location and domain key
	 * @return A list of all hashes for the content keys. To return a
	 *         predictable amount (important for routing), the hashes can be
	 *         xored.
	 */
	public abstract DigestInfo digest(Number320 key);

	/**
	 * Calculates a digest over a specific location and domain. It will return
	 * those content keys that are stored. Those keys that are not stored are
	 * ignored
	 * 
	 * @param key The location and domain key
	 * @param contentKeys The content keys to look for. Those keys that are not
	 *        found are ignored.
	 * @return A list of all hashes for the content keys. To return a
	 *         predictable amount (important for routing), the hashes can be
	 *         xored.
	 */
	public abstract DigestInfo digest(Number320 key, Collection<Number160> contentKeys);

}
