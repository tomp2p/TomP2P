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
package net.tomp2p.rpc;

import java.util.ArrayList;
import java.util.List;

import net.tomp2p.peers.Number160;

/**
 * Calculates or sets a global hash. The digest is used in two places: for
 * routing, where a message needs to have a predictable size. Thus in this case
 * a global hash is calculated. The second usage is get() for getting a list of
 * hashes from peers. Here we don't need to restrict ourself, since we use TCP.
 * 
 * @author Thomas Bocek
 * 
 */
public class DigestInfo
{
	private volatile Number160 keyDigest = null;
	private volatile int size = -1;
	private final List<Number160> keyDigests = new ArrayList<Number160>();

	/**
	 * Empty constructor is used to add the hashes to the list.
	 */
	public DigestInfo()
	{}

	/**
	 * If a global hash has already been calculated, then this constructor is
	 * used to store those. Note that once a global hash is set it cannot be
	 * unset.
	 * 
	 * @param keyDigest
	 * @param size
	 */
	public DigestInfo(Number160 keyDigest, int size)
	{
		this.keyDigest = keyDigest;
		this.size = size;
	}

	/**
	 * @return Returns or calculates the global hash. The global hash will be
	 *         calculated if the empty constructor is used.
	 */
	public Number160 getKeyDigest()
	{
		if (keyDigest == null)
		{
			Number160 hash = Number160.ZERO;
			for (Number160 key2 : keyDigests)
			{
				hash = hash.xor(key2);
			}
			keyDigest = hash;
		}
		return keyDigest;
	}

	/**
	 * @return The list of hashes
	 */
	public List<Number160> getKeyDigests()
	{
		return keyDigests;
	}

	/**
	 * @return The number of hashes
	 */
	public int getSize()
	{
		if (size == -1)
		{
			size = keyDigests.size();
		}
		return size;
	}

	/**
	 * @return True is the digest information has not been provided.
	 */
	public boolean isEmpty()
	{
		return size <= 0;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (!(obj instanceof DigestInfo))
			return false;
		DigestInfo other = (DigestInfo) obj;
		return getKeyDigest().equals(other.getKeyDigest()) && getSize() == other.getSize();
	}

	@Override
	public int hashCode()
	{
		return getKeyDigest().hashCode() ^ getSize();
	}
}
