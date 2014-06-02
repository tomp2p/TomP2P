/*
 * Copyright 2013 Thomas Bocek
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

import java.util.Collection;

import net.tomp2p.p2p.Statistics;

/**
 * The default filter accepts all peers.
 * 
 * @author Thomas Bocek
 * @author Thibault Cholez
 * 
 */
public class PeerStatRoutingFilter implements PeerFilter {

	//final private Statistics statistics;
	//final private int replicationRate;

	public PeerStatRoutingFilter(final Statistics statistics, final int replicationRate) {
		//this.statistics = statistics;
		//this.replicationRate = replicationRate;
	}

	// see
	// http://hal.inria.fr/docs/00/78/64/38/PDF/detection_mitigation_sybil_attacks.pdf
	@Override
	public boolean reject(final PeerAddress peerAddress, Collection<PeerAddress> all, Number160 target) {
		// immediate check
		/*double numberOfPeers = statistics.getEstimatedNumberOfNodes();
		int e = (int) (Math.log(numberOfPeers / replicationRate) / Math.log(2));
		int toExclude = e + 10; // e.g. 28
		// e.g. 14 is fine, 29 is not fine
		int toTest = target.xor(peerAddress.getPeerId()).bitLength();
		if (toTest > toExclude) {
			return true;
		}
		// detailed check with KL
		final double[] t = new double[10];
		final double[] m = new double[10];
		for (int i = 1; i <= 10; i++) {
			t[i] = 1 / Math.pow(2, i);
		}
		int total = all.size();

		toTest = target.xor(peerAddress.getPeerId()).bitLength();
		toTest -= e;
		m[toTest] += 1.0d;

		for (PeerAddress test : all) {
			toTest = target.xor(test.getPeerId()).bitLength();
			toTest -= e;
			m[toTest] += 1.0d;
		}
		for (int i = 1; i <= 10; i++) {
			m[i] = m[i] / total;
		}
		double kb = 0;
		for (int i = 1; i <= 10; i++) {
			kb += m[i] * Math.log(m[i] / t[i]);
		}

		if (kb > 0.7) {
			return true;
		}*/

		return false;
	}
}
