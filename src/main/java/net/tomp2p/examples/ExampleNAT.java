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

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDiscover;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class ExampleNAT 
{
	public void startServer() throws Exception 
	{
		Peer peer = null;
		try 
		{
			Random r = new Random(42L);			
			//peer.getP2PConfiguration().setBehindFirewall(true);
			Bindings b = new Bindings();
			b.addInterface("eth0");
			peer = new PeerMaker(new Number160(r)).setBindings(b).setPorts(4000).buildAndListen();
			System.out.println("peer started.");
			for (;;) 
			{
				for (PeerAddress pa : peer.getPeerBean().getPeerMap().getAll()) 
				{
					FutureChannelCreator fcc=peer.getConnectionBean().getConnectionReservation().reserve(1);
					fcc.awaitUninterruptibly();
					ChannelCreator cc = fcc.getChannelCreator();
					FutureResponse fr1 = peer.getHandshakeRPC().pingTCP(pa, cc);
					fr1.awaitUninterruptibly();
					if (fr1.isSuccess())
						System.out.println("peer online T:" + pa);
					else
						System.out.println("offline " + pa);
					FutureResponse fr2 = peer.getHandshakeRPC().pingUDP(pa, cc);
					fr2.awaitUninterruptibly();
					peer.getConnectionBean().getConnectionReservation().release(cc);
					if (fr2.isSuccess())
						System.out.println("peer online U:" + pa);
					else
						System.out.println("offline " + pa);
				}
				Thread.sleep(1500);
			}
		}
		finally 
		{
			peer.shutdown();
		}
	}
	
	public static void main(String[] args) throws Exception 
	{
		if(args.length>0)
		{
			startClientNAT(args[0]);
		}
		else
		{
			ExampleNAT t = new ExampleNAT();
			t.startServer();
		}
	}
	
	public static void startClientNAT(String ip) throws Exception 
	{
		Random r = new Random(43L);
		Peer peer = new PeerMaker(new Number160(r)).setPorts(4000).buildAndListen();
		peer.getConfiguration().setBehindFirewall(true);
		PeerAddress pa = new PeerAddress(Number160.ZERO,
				InetAddress.getByName(ip), 4000, 4000);
		FutureDiscover fd = peer.discover(pa);
		fd.awaitUninterruptibly();
		if (fd.isSuccess()) 
		{
			System.out.println("found that my outside address is "
					+ fd.getPeerAddress());
		} 
		else 
		{
			System.out.println("failed " + fd.getFailedReason());
		}
		peer.shutdown();
	}
}
