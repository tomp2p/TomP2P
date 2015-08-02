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

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.nat.FutureNAT;
import net.tomp2p.nat.PeerBuilderNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.relay.RelayCallback;

public class ExampleNAT {
	private final static int PORT_SERVER = 4000;
	private final static int PORT_CLIENT = 4001;
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
		final PeerNAT peerNAT = new PeerBuilderNAT(peer).relayCallback(new RelayCallback() {
			
			@Override
			public void onRelayRemoved(PeerAddress relay, PeerConnection object) {
				System.out.println("relay removed: "+relay);
			}
			
			@Override
			public void onRelayAdded(PeerAddress relay, PeerConnection object) {
				System.out.println("relay added: "+relay);
			}
			
			@Override
			public void onNoMoreRelays(int activeRelays) {
				System.out.println("could not find more relays: "+activeRelays);
			}
			
			@Override
			public void onFullRelays(int activeRelays) {
				System.out.println("could find all relays: "+activeRelays);
			}
			
			@Override
			public void onFailure(Exception e) {
				e.printStackTrace();
			}

			@Override
			public void onShutdown() {
				System.out.println("shutdown");
			}
		}).start();
		final PeerAddress pa = new PeerAddress(Number160.ZERO, InetAddress.getByName(ip), PORT_SERVER, PORT_SERVER);
		
		final FutureDiscover fd = peer.discover().peerAddress(pa).start();
		final FutureNAT fn = peerNAT.portForwarding(fd);
		fn.addListener(new BaseFutureAdapter<FutureNAT>() {

			@Override
			public void operationComplete(FutureNAT future) throws Exception {
				if(future.isFailed()) {
					peerNAT.startRelay(fd.reporter());
				}
				
			}
		});
		
		if (fd.isSuccess()) {
			System.out.println("found that my outside address is " + fd.peerAddress());
		} else {
			System.out.println("failed " + fd.failedReason());
		}
		
		if (fn.isSuccess()) {
			System.out.println("NAT success: " + fn.peerAddress());
		} else {
			System.out.println("failed " + fn.failedReason());
			//this is enough time to print out the status of the relay search
			Thread.sleep(5000);
		}
		
		peer.shutdown();
	}
}
