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

import java.util.Random;

import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;

public class ExamplePersistentConnection 
{
	final private static Random rnd = new Random(42L);
	
	public static void main(String[] args) throws Exception 
	{
		examplePersistentConnection();
	}
	
	private static void examplePersistentConnection() throws Exception
	{
		Peer peer1 = null;
		Peer peer2 = null;
		try
		{
			peer1 = new PeerMaker(new Number160(rnd)).setPorts(4001).makeAndListen();
			peer2 = new PeerMaker(new Number160(rnd)).setPorts(4002).makeAndListen();
			//
			peer2.setObjectDataReply(new ObjectDataReply() 
			{
				@Override
				public Object reply(PeerAddress sender, Object request) throws Exception
				{
					return "world!";
				}
			});
			//keep the connection for 20s alive. Setting -1 means to keep it open as long as possible
			PeerConnection peerConnection = peer1.createPeerConnection(peer2.getPeerAddress(), 20);
			String sentObject = "Hello";
			FutureResponse fd=peer1.sendDirect().setConnection(peerConnection).setObject(sentObject).start();
			System.out.println("send "+ sentObject);
			fd.awaitUninterruptibly();
			System.out.println("received "+fd.getObject() + " connections: "+peer1.getPeerBean().getStatistics().getTCPChannelCreationCount());
			//we reuse the connection
			fd=peer1.sendDirect().setConnection(peerConnection).setObject(sentObject).start();
			System.out.println("send "+ sentObject);
			fd.awaitUninterruptibly();
			System.out.println("received "+fd.getObject()  + " connections: "+peer1.getPeerBean().getStatistics().getTCPChannelCreationCount());
			// now we don't want to keep the connection open anymore:
			peerConnection.close();
		}
		finally
		{
			peer1.shutdown();
			peer2.shutdown();
		}
	}
	
	
}
