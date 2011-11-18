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

import net.tomp2p.peers.Number160;

public class DigestInfo
{
	private final Number160 keyDigest;
	private final int size;

	public DigestInfo(Number160 keyDigest, int size)
	{
		this.keyDigest = keyDigest;
		this.size = size;
	}

	public Number160 getKeyDigest()
	{
		return keyDigest;
	}

	public int getSize()
	{
		return size;
	}
	
	public boolean isEmpty()
	{
		return keyDigest == null && size == 0;
	}
	
	@Override
	public boolean equals(Object obj)
	{
		if(!(obj instanceof DigestInfo))
			return false;
		DigestInfo other=(DigestInfo)obj;
		if(keyDigest == null)
			return true;
		return keyDigest.equals(other.keyDigest) && size == other.size;
	}
	
	@Override
	public int hashCode() 
	{
		if(keyDigest == null)
			return 0;
		return keyDigest.hashCode() ^ size;
	}
}
