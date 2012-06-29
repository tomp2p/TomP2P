/*
 * Copyright 2011 Thomas Bocek
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
package net.tomp2p.p2p;
import java.net.UnknownHostException;
import java.util.Random;

import net.tomp2p.Utils2;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;

import org.junit.Assert;
import org.junit.Test;

public class TestStatistics
{
	@Test
	public void testCountPeers1() throws UnknownHostException
	{
		Random rnd = new Random(42L);
		Number160 n = new Number160(rnd);
		PeerMap peerMapKadImpl = new PeerMap(n, 20, 50, 10, new int[0], 100, false);
		Statistics statistics = peerMapKadImpl.getStatistics();
		for (int i = 0; i < 100; i++)
		{
			PeerAddress pa = Utils2.createAddress(new Number160(rnd));
			peerMapKadImpl.peerFound(pa, null);
		}
		Assert.assertEquals(100d, statistics.getEstimatedNumberOfNodes(), 0.01);
	}

	@Test
	public void testCountPeers2() throws UnknownHostException
	{
		Random rnd = new Random(42L);
		Number160 n = new Number160(rnd);
		PeerMap peerMapKadImpl = new PeerMap(n, 20, 50, 10, new int[0], 100, false);
		Statistics statistics = peerMapKadImpl.getStatistics();
		for (int i = 0; i < 100; i++)
		{
			PeerAddress pa = Utils2.createAddress(new Number160(rnd));
			peerMapKadImpl.peerFound(pa, null);
		}
		Assert.assertEquals(100d, statistics.getEstimatedNumberOfNodes(), 0.01);
	}

	@Test
	public void testNodesEstimation() throws UnknownHostException
	{
		for (int j = 1; j < 5; j++)
		{
			int maxNr = 10000 * j;
			Random rnd = new Random(42L);
			Number160 id = new Number160(rnd);
			PeerMap kadRouting = new PeerMap(id, 20, 0, 0, new int[0], 0, false);
			Statistics statistics = kadRouting.getStatistics();
			for (int i = 0; i < maxNr; i++)
			{
				Number160 id1 = new Number160(rnd);
				PeerAddress remoteNode1 = Utils2.createAddress(id1);
				kadRouting.peerFound(remoteNode1, null);
			}
			double diff = (maxNr + 1) / statistics.getEstimatedNumberOfNodes();
			//System.err.println("est:"+statistics.getEstimatedNumberOfNodes()+", real"+maxNr);
			Assert.assertEquals(true, diff < 1.1 && diff > 0.9);
		}
	}
}
