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

import io.netty.channel.ChannelHandler;
import io.netty.util.concurrent.EventExecutorGroup;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import net.tomp2p.connection.ChannelClientConfiguration;
import net.tomp2p.connection.ChannelServerConficuration;
import net.tomp2p.connection.PipelineFilter;
import net.tomp2p.futures.FutureDirect;
import net.tomp2p.futures.FuturePeerConnection;
import net.tomp2p.message.CountConnectionOutboundHandler;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.utils.Pair;

/**
 * Example how to use persistent connection with Peer.sendDirect().
 * 
 * @author Thomas Bocek
 * 
 */
public final class ExamplePersistentConnection {

    /**
     * Empty constructor.
     */
    private ExamplePersistentConnection() {
    }

    private static final Random RND = new Random(42L);
    private static final CountConnectionOutboundHandler ccohTCP = new CountConnectionOutboundHandler();
    private static final CountConnectionOutboundHandler ccohUDP = new CountConnectionOutboundHandler();

    /**
     * Start the example with persistent connections.
     * 
     * @param args
     *            Empty
     * @throws Exception .
     */
    public static void main(final String[] args) throws Exception {
        examplePersistentConnection();
    }

    /**
     * Sends a message to a peer directly using peer connection. The peer remains the connection in an open state and
     * sends a second request that uses the same connection.
     * 
     * @throws Exception .
     */
    private static void examplePersistentConnection() throws Exception {
        Peer peer1 = null;
        Peer peer2 = null;
        try {
            final int port1 = 4001;
            final int port2 = 4002;
            final int timeout = 20;
            
            ChannelServerConficuration csc = PeerMaker.createDefaultChannelServerConfiguration();
    		ChannelClientConfiguration ccc = PeerMaker.createDefaultChannelClientConfiguration();
    		csc.pipelineFilter(createFilter());
    		ccc.pipelineFilter(createFilter());
            
            peer1 = new PeerMaker(new Number160(RND)).ports(port1).channelClientConfiguration(ccc).channelServerConfiguration(csc).makeAndListen();
            peer2 = new PeerMaker(new Number160(RND)).ports(port2).makeAndListen();
            //
            peer2.setObjectDataReply(new ObjectDataReply() {
                @Override
                public Object reply(final PeerAddress sender, final Object request) throws Exception {
                    return "world!";
                }
            });
            
            String sentObject = "Hello";
            FutureDirect fd = peer1.sendDirect(peer2.getPeerAddress()).setObject(sentObject).start();
            System.out.println("send " + sentObject);
            fd.awaitUninterruptibly();
            
            System.out.println("received " + fd.object() + " connections: "
                    + ccohTCP.total()+ "/"+ccohUDP.total());
            
            // keep the connection for 20s alive. Setting -1 means to keep it open as long as possible
            FuturePeerConnection futurePeerConnection = peer1.createPeerConnection(peer2.getPeerAddress(), timeout);
            
            fd = peer1.sendDirect(futurePeerConnection).setObject(sentObject).start();
            System.out.println("send " + sentObject);
            fd.awaitUninterruptibly();
            System.out.println("received " + fd.object() + " connections: "
                    + ccohTCP.total()+ "/"+ccohUDP.total());
            // we reuse the connection
            fd = peer1.sendDirect(futurePeerConnection).setObject(sentObject).start();
            System.out.println("send " + sentObject);
            fd.awaitUninterruptibly();
            System.out.println("received " + fd.object() + " connections: "
                    + ccohTCP.total()+ "/"+ccohUDP.total());
            // now we don't want to keep the connection open anymore:
            futurePeerConnection.close();
        } finally {
            if (peer1 != null) {
                peer1.shutdown();
            }
            if (peer2 != null) {
                peer2.shutdown();
            }
        }
    }
    
    private static PipelineFilter createFilter() {
    	
    	PipelineFilter pf = new PipelineFilter() {
			@Override
			public Map<String, Pair<EventExecutorGroup, ChannelHandler>> filter(Map<String, Pair<EventExecutorGroup, ChannelHandler>> channelHandlers, boolean tcp,
			        boolean client) {
				Map<String, Pair<EventExecutorGroup, ChannelHandler>> retVal = new LinkedHashMap<String, Pair<EventExecutorGroup, ChannelHandler>>();
				retVal.put("counter", new Pair<EventExecutorGroup, ChannelHandler>(null, tcp? ccohTCP:ccohUDP));
				retVal.putAll(channelHandlers);
				return retVal;
			}
		};
		return pf;
    }

}
