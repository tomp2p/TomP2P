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

import java.io.IOException;

import net.tomp2p.futures.FutureBootstrap;
import net.tomp2p.futures.FutureDHT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

/**
 * See http://tomp2p.net/doc/quick/ for more information on this
 */
public class ExampleDNS
{
	final private Peer peer;
	public ExampleDNS(int nodeId) throws Exception {
		peer=new PeerMaker(Number160.createHash(nodeId)).setPorts(4000+nodeId).makeAndListen();
		FutureBootstrap fb=this.peer.bootstrap().setBroadcast(true).setPorts(4001).start();
		fb.awaitUninterruptibly();
		peer.discover().setPeerAddress(fb.getBootstrapTo().iterator().next()).start().awaitUninterruptibly();
		
	}
	public static void main(String[] args) throws NumberFormatException, Exception {
		ExampleDNS dns=new ExampleDNS(Integer.parseInt(args[0]));
		if(args.length==3) {
			dns.store(args[1],args[2]);
		}
		if(args.length==2) {
			System.out.println("Name:"+args[1]+" IP:"+dns.get(args[1]));
		}
	}
	private String get(String name) throws ClassNotFoundException, IOException
	{
		FutureDHT futureDHT=peer.get(Number160.createHash(name)).start();
		futureDHT.awaitUninterruptibly();
		if(futureDHT.isSuccess()) {
			return futureDHT.getDataMap().values().iterator().next().getObject().toString();
		}
		return "not found";
	}
	private void store(String name, String ip) throws IOException
	{
		peer.put(Number160.createHash(name)).setData(new Data(ip)).start().awaitUninterruptibly();
	}
}
