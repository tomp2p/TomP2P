/*
 * Copyright 2013 Maxat Pernebayev, Thomas Bocek
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

package net.tomp2p.synchronization;

import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.PeerAddress;

public class PeerSync {

	private final SyncRPC syncRPC;
	private final Peer peer;
	private final int blockSize;

	/**
	 * Create a PeerSync class and register the RPC. Be aware that if you use
	 * {@link ReplicationSync}, than use the PeerSync that was created in that
	 * class.
	 * 
	 * @param peer
	 *            The peer
	 * @param blockSize
	 *            The block size as the basis for the checksums, RSync uses a
	 *            default of 700
	 */
	public PeerSync(Peer peer, final int blockSize) {
		this.peer = peer;
		this.syncRPC = new SyncRPC(peer.peerBean(), peer.connectionBean(), blockSize);
		this.blockSize = blockSize;
	}

	public Peer peer() {
		return peer;
	}

	public SyncRPC syncRPC() {
		return syncRPC;
	}

	public SyncBuilder synchronize(PeerAddress other) {
		return new SyncBuilder(this, other, blockSize);
	}
}
