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
package net.tomp2p.examples;

import java.net.InetAddress;
import java.util.Random;

import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.nat.FutureNAT;
import net.tomp2p.nat.FutureRelayNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.RelayClientConfig;

public class ExampleNAT {
	private final static int PORT_SERVER = 4000;
	private final static int PORT_CLIENT = 4000;
	public static void startServer() throws Exception {
		Random r = new Random(42L);
		Peer peer = new PeerBuilder(new Number160(r)).ports(PORT_SERVER).start();
		System.out.println("peer started.");
		for (;;) {
			for (PeerAddress pa : peer.peerBean().peerMap().all()) {
					System.out.println("peer online (TCP):" + pa);
			}
			Thread.sleep(2000);
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length > 0) {
			startClientNAT(args[0]);
		} else {
			startServer();
		}
	}

	public static void startClientNAT(String ip) throws Exception {
		Random r = new Random(43L);
		Peer peer = new PeerBuilder(new Number160(r)).ports(PORT_CLIENT).behindFirewall().start();
		PeerNAT peerNAT = new PeerBuilderNAT(peer).start();
		PeerAddress pa = new PeerAddress(Number160.ZERO, InetAddress.getByName(ip), PORT_SERVER, PORT_SERVER);
		
		FutureDiscover fd = peer.discover().peerAddress(pa).start();
		FutureNAT fn = peerNAT.startSetupPortforwarding(fd);
		FutureRelayNAT frn = peerNAT.startRelay(RelayClientConfig.OpenTCP(), fd, fn);
		
		frn.awaitUninterruptibly();
		if (fd.isSuccess()) {
			System.out.println("found that my outside address is " + fd.peerAddress());
		} else {
			System.out.println("failed " + fd.failedReason());
		}
		
		if (fn.isSuccess()) {
			System.out.println("NAT success: " + fn.peerAddress());
		} else {
			System.out.println("failed " + fn.failedReason());
		}
		
		peer.shutdown();
	}
}
