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
import net.tomp2p.peers.DefaultMaintenance;
import net.tomp2p.peers.DefaultPeerFilter;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.peers.PeerMapConfiguration;

import org.junit.Assert;
import org.junit.Test;

public class TestStatistics {

	private static final Number160 ID = new Number160("0x1");

	@Test
	public void testCountPeers1() throws UnknownHostException {
		Random rnd = new Random(42L);

		for (int j = 1; j < 10; j++) {
			int nr = 100000 * j;
			PeerMapConfiguration conf = new PeerMapConfiguration(ID);
			conf.setFixedVerifiedBagSizes(20).setFixedOverflowBagSizes(20);
			conf.offlineCount(1000).offlineTimeout(60);
			conf.addPeerFilter(new DefaultPeerFilter()).maintenance(new DefaultMaintenance(0, new int[] {}));
			PeerMap peerMap = new PeerMap(conf);

			Statistics statistics = new Statistics(peerMap);
			for (int i = 0; i < nr; i++) {
				PeerAddress pa = Utils2.createAddress(new Number160(rnd));
				peerMap.peerFound(pa, null, null, null);
			}

			double diff = nr / statistics.estimatedNumberOfNodes();
			System.err.println("diff: " + diff);
			System.err.println("estimated: " + statistics.estimatedNumberOfNodes() + " actual: " + (nr-1));
			Assert.assertTrue(diff < 1.5 && diff > 0.5);
		}
	}
}
