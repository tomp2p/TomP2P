/*
 * Copyright 2012 Thomas Bocek
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
package net.tomp2p.replication;

import java.util.Map;

import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.p2p.Peer;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.PeerExchangeRPC;
import net.tomp2p.storage.TrackerStorage;
import net.tomp2p.utils.Timings;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrackerStorageReplication
    implements ResponsibilityListener
{
    final private static Logger logger = LoggerFactory.getLogger( TrackerStorageReplication.class );

    final private PeerExchangeRPC peerExchangeRPC;

    final private Map<BaseFuture, Long> pendingFutures;

    final private TrackerStorage trackerStorage;

    final private Peer peer;

    final private boolean forceTCP;

    public TrackerStorageReplication( Peer peer, PeerExchangeRPC peerExchangeRPC, Map<BaseFuture, Long> pendingFutures,
                                      TrackerStorage trackerStorage, boolean forceTCP )
    {
        this.peer = peer;
        this.peerExchangeRPC = peerExchangeRPC;
        this.pendingFutures = pendingFutures;
        this.trackerStorage = trackerStorage;
        this.forceTCP = forceTCP;
    }

    @Override
    public void meResponsible( Number160 locationKey )
    {
        // the other has to send us the data, so do nothing
    }

    @Override
    public void otherResponsible( final Number160 locationKey, final PeerAddress other )
    {
        // do pex here, but with mesh peers!
        if ( logger.isDebugEnabled() )
        {
            logger.debug( "other peer became responsibel and we thought we were responsible, so move the data to this peer" );
        }
        for ( final Number160 domainKey : trackerStorage.responsibleDomains( locationKey ) )
        {
            peer.getConnectionBean().getConnectionReservation().reserve( 1 ).addListener( new BaseFutureAdapter<FutureChannelCreator>()
                                                                                          {
                                                                                              @Override
                                                                                              public void operationComplete( FutureChannelCreator future )
                                                                                                  throws Exception
                                                                                              {
                                                                                                  if ( future.isSuccess() )
                                                                                                  {
                                                                                                      FutureResponse futureResponse =
                                                                                                          peerExchangeRPC.peerExchange( other,
                                                                                                                                        locationKey,
                                                                                                                                        domainKey,
                                                                                                                                        true,
                                                                                                                                        future.getChannelCreator(),
                                                                                                                                        forceTCP );
                                                                                                      Utils.addReleaseListener( futureResponse,
                                                                                                                                peer.getConnectionBean().getConnectionReservation(),
                                                                                                                                future.getChannelCreator(),
                                                                                                                                1 );
                                                                                                      pendingFutures.put( futureResponse,
                                                                                                                          Timings.currentTimeMillis() );
                                                                                                  }
                                                                                                  else
                                                                                                  {
                                                                                                      if ( logger.isErrorEnabled() )
                                                                                                      {
                                                                                                          logger.error( "otherResponsible failed "
                                                                                                              + future.getFailedReason() );
                                                                                                      }
                                                                                                  }
                                                                                              }
                                                                                          } );
        }
    }
}
