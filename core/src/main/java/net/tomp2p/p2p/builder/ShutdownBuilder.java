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

package net.tomp2p.p2p.builder;

import java.security.KeyPair;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.DefaultConnectionConfiguration;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.FutureRouting;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.RoutingConfiguration;
import net.tomp2p.utils.Utils;

/**
 * Set the configuration options for the shutdown command. The shutdown does first a rounting, searches for its close
 * peers and then send a quit message so that the other peers knows that this peer is offline.
 * 
 * @author Thomas Bocek
 * 
 */
public class ShutdownBuilder extends DefaultConnectionConfiguration implements SignatureBuilder<ShutdownBuilder> {
	
	private static final FutureDone<Void> FUTURE_SHUTDOWN = new FutureDone<Void>().failed("shutdown");
    
	private final Peer peer;
	
	private KeyPair keyPair = null;
	
	private RoutingConfiguration routingConfiguration;
	
	private boolean forceRoutingOnlyToSelf = false;

    /**
     * Constructor.
     * 
     * @param peer
     *            The peer that runs the routing and quit messages
     */
    public ShutdownBuilder(final Peer peer) {
        this.peer = peer;
    }

    /**
     * Start the shutdown. This method returns immediately with a future object. The future object can be used to block
     * or add a listener.
     * 
     * @return The future object
     */
    public FutureDone<Void> start() {
        if (peer.isShutdown()) {
            return FUTURE_SHUTDOWN;
        }

        if (routingConfiguration == null) {
            routingConfiguration = new RoutingConfiguration(8, 10, 2);
        }
        
        int conn = routingConfiguration.parallel();
        FutureChannelCreator fcc = peer.connectionBean().reservation().create(conn);
        final FutureDone<Void> futureShutdown = new FutureDone<Void> ();
        Utils.addReleaseListener(fcc, futureShutdown);
        
        fcc.addListener(new BaseFutureAdapter<FutureChannelCreator>() {
            @Override
            public void operationComplete(final FutureChannelCreator futureChannelCreator) throws Exception {
            	if (futureChannelCreator.isSuccess()) {
            		ChannelCreator cc = futureChannelCreator.channelCreator();
            		RoutingBuilder routingBuilder = BootstrapBuilder.createBuilder(routingConfiguration, forceRoutingOnlyToSelf);
            		routingBuilder.locationKey(peer.peerID());
            		FutureRouting futureRouting = peer.distributedRouting().quit(routingBuilder, cc);
            		futureRouting.addListener(new BaseFutureAdapter<FutureRouting>() {
						@Override
                        public void operationComplete(FutureRouting future) throws Exception {
	                       if(future.isSuccess()) {
	                    	   futureShutdown.done();
	                       } else {
	                    	   futureShutdown.failed(future);
	                       }
                        }
					});
            	} else {
            		futureShutdown.failed(futureChannelCreator);
            	}
            }
        });
        return futureShutdown;
    }

    /**
     * Gets whether the message should be signed.
	 */
	public boolean isSign() {
		return keyPair != null;
	}

	/**
	 * Sets whether a message should be signed. For entry protection, set this to true.
	 * @return This class
	 */
	public ShutdownBuilder sign(final boolean signMessage) {
		if (signMessage) {
			sign();
		} else {
			this.keyPair = null;
		}
		return this;
	}

	/**
	 * Sets a message to be signed.
	 */
	public ShutdownBuilder sign() {
		this.keyPair = peer.peerBean().keyPair();
		return this;
	}

	/**
	 * @param keyPair
	 *            The keyPair to sing the complete message. The key will be
	 *            attached to the message and stored potentially with a data
	 *            object (if there is such an object in the message).
	 * @return This class
	 */
	public ShutdownBuilder keyPair(KeyPair keyPair) {
		this.keyPair = keyPair;
		return this;
	}

	/**
	 * @return The current keypair to sign the message. If null, no signature is
	 *         applied.
	 */
	public KeyPair keyPair() {
		return keyPair;
	}
	
	public RoutingConfiguration routingConfiguration() {
        return routingConfiguration;
    }

    public ShutdownBuilder routingConfiguration(RoutingConfiguration routingConfiguration) {
        this.routingConfiguration = routingConfiguration;
        return this;
    }
    
    public boolean isForceRoutingOnlyToSelf() {
        return forceRoutingOnlyToSelf;
    }

    public ShutdownBuilder forceRoutingOnlyToSelf() {
        this.forceRoutingOnlyToSelf = true;
        return this;
    }

    public ShutdownBuilder forceRoutingOnlyToSelf(boolean forceRoutingOnlyToSelf) {
        this.forceRoutingOnlyToSelf = forceRoutingOnlyToSelf;
        return this;
    }
    
}
