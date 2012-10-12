/*
 * Copyright 2009 Thomas Bocek
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
package net.tomp2p.p2p;

import java.io.File;
import java.io.IOException;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import net.tomp2p.connection.Bindings;
import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.ConnectionHandler;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.connection.PeerConnection;
import net.tomp2p.futures.BaseFuture;
import net.tomp2p.futures.FutureChannelCreator;
import net.tomp2p.natpmp.NatPmpException;
import net.tomp2p.p2p.builder.AddBuilder;
import net.tomp2p.p2p.builder.AddTrackerBuilder;
import net.tomp2p.p2p.builder.BootstrapBuilder;
import net.tomp2p.p2p.builder.BroadcastBuilder;
import net.tomp2p.p2p.builder.DiscoverBuilder;
import net.tomp2p.p2p.builder.GetBuilder;
import net.tomp2p.p2p.builder.GetTrackerBuilder;
import net.tomp2p.p2p.builder.ParallelRequestBuilder;
import net.tomp2p.p2p.builder.PingBuilder;
import net.tomp2p.p2p.builder.PutBuilder;
import net.tomp2p.p2p.builder.RemoveBuilder;
import net.tomp2p.p2p.builder.SendBuilder;
import net.tomp2p.p2p.builder.SendDirectBuilder;
import net.tomp2p.p2p.builder.SubmitBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.rpc.BroadcastRPC;
import net.tomp2p.rpc.DirectDataRPC;
import net.tomp2p.rpc.HandshakeRPC;
import net.tomp2p.rpc.NeighborRPC;
import net.tomp2p.rpc.ObjectDataReply;
import net.tomp2p.rpc.PeerExchangeRPC;
import net.tomp2p.rpc.QuitRPC;
import net.tomp2p.rpc.RawDataReply;
import net.tomp2p.rpc.StorageRPC;
import net.tomp2p.rpc.TaskRPC;
import net.tomp2p.rpc.TrackerRPC;
import net.tomp2p.task.AsyncTask;
import net.tomp2p.task.Worker;
import net.tomp2p.utils.CacheMap;

import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TomP2P implements besides the following distributed hash table (DHT) operations:
 * <ul>
 * <li>value=get(locationKey)</li>
 * <li>put(locationKey,value)</li>
 * <li>remove(locationKey)</li>
 * </ul>
 * also the following operations:
 * <ul>
 * <li>value=get(locationKey,contentKey)</li>
 * <li>put(locationKey,contentKey,value)</li>
 * <li>remove(locationKey,contentKey)</li>
 * </ul>
 * The advantage of TomP2P is that multiple values can be stored in one location. Furthermore, TomP2P also provides to
 * store keys in different domains to avoid key collisions.
 * 
 * @author Thomas Bocek
 */
public class Peer
{
    // domain used if no domain provided
    final private static Logger logger = LoggerFactory.getLogger( Peer.class );

    // As soon as the user calls listen, this connection handler is set
    private ConnectionHandler connectionHandler;

    // the id of this node
    final private Number160 peerId;

    // the p2p network identifier, two different networks can have the same
    // ports
    private final int p2pID;

    private final KeyPair keyPair;

    // Distributed
    private DistributedHashTable distributedHashMap;

    private DistributedTracker distributedTracker;

    private DistributedRouting distributedRouting;

    private DistributedTask distributedTask;

    private AsyncTask asyncTask;

    // RPC
    private HandshakeRPC handshakeRCP;

    private StorageRPC storageRPC;

    private NeighborRPC neighborRPC;

    private QuitRPC quitRCP;

    private PeerExchangeRPC peerExchangeRPC;

    private DirectDataRPC directDataRPC;

    private TrackerRPC trackerRPC;

    private TaskRPC taskRPC;

    private BroadcastRPC broadcastRPC;

    //
    private Bindings bindings;

    //
    final private ConnectionConfiguration configuration;

    final private Map<BaseFuture, Long> pendingFutures =
        Collections.synchronizedMap( new CacheMap<BaseFuture, Long>( 1000, true ) );

    private boolean masterFlag = true;

    private List<ScheduledFuture<?>> scheduledFutures =
        Collections.synchronizedList( new ArrayList<ScheduledFuture<?>>() );

    final private List<PeerListener> listeners = new ArrayList<PeerListener>();

    private Timer timer;

    final public static int BLOOMFILTER_SIZE = 1024;

    //
    final private int maintenanceThreads;

    final private int replicationThreads;

    // final private int replicationRefreshMillis;

    final private PeerMap peerMap;

    final private int maxMessageSize;

    private volatile boolean shutdown = false;

    Peer( final int p2pID, final Number160 nodeId, final KeyPair keyPair, int maintenanceThreads,
          int replicationThreads, ConnectionConfiguration configuration, PeerMap peerMap, int maxMessageSize )
    {
        this.p2pID = p2pID;
        this.peerId = nodeId;
        this.configuration = configuration;
        this.keyPair = keyPair;
        this.maintenanceThreads = maintenanceThreads;
        this.replicationThreads = replicationThreads;
        this.peerMap = peerMap;
        this.maxMessageSize = maxMessageSize;
    }

    /**
     * Adds a listener to peer events. The events being triggered are: startup, shutdown, change of peer address. The
     * change of the peer address is due to the discovery process. Since this process runs in an other thread, this
     * method is thread safe.
     * 
     * @param listener The listener
     */
    public void addPeerListener( PeerListener listener )
    {
        if ( isRunning() )
        {
            listener.notifyOnStart();
        }
        synchronized ( listeners )
        {
            listeners.add( listener );
        }
    }

    /**
     * Removes a peer listener. This method is thread safe.
     * 
     * @param listener The listener
     */
    public void removePeerListener( PeerListener listener )
    {
        synchronized ( listeners )
        {
            listeners.remove( listener );
        }
    }

    public List<PeerListener> getListeners()
    {
        return listeners;
    }

    /**
     * Closes all connections of this node
     * 
     * @throws InterruptedException
     */
    public void shutdown()
    {
        shutdown = true;
        logger.info( "begin shutdown in progres at " + System.nanoTime() );
        synchronized ( scheduledFutures )
        {
            for ( ScheduledFuture<?> scheduledFuture : scheduledFutures )
                scheduledFuture.cancel( true );
        }
        // don't send any new requests
        if ( masterFlag )
        {
            getConnectionBean().getSender().shutdown();
            getPeerBean().getTaskManager().shutdown();
        }
        
        if ( masterFlag && timer != null )
        {
            Set<Timeout> timeouts = timer.stop();
            for ( Timeout timeout : timeouts )
            {
                try
                {
                    timeout.getTask().run( null );
                }
                catch ( Exception e )
                {
                    logger.error( "unable to stop timertask" );
                    e.printStackTrace( );
                }
            }
        }
        //timer must be stopped before, or there will be still connections that are running
        getConnectionHandler().shutdown();

        // listeners may be called from other threads
        synchronized ( listeners )
        {
            for ( PeerListener listener : listeners )
                listener.notifyOnShutdown();
        }
        getPeerBean().getStorage().close();
        connectionHandler = null;
    }

    /**
     * Lets this node listen on a port
     * 
     * @param udpPort the UDP port to listen on
     * @param tcpPort the TCP port to listen on
     * @param bindInformation contains IP addresses to listen on
     * @param replication
     * @param statServer
     * @throws Exception
     */
    ConnectionHandler listen( final int udpPort, final int tcpPort, final Bindings bindings,
                              final File fileMessageLogger, int workerThreads )
        throws IOException
    {
        // I'm the master
        masterFlag = true;
        this.timer = new HashedWheelTimer( 10, TimeUnit.MILLISECONDS, 10 );
        this.bindings = bindings;

        ConnectionHandler connectionHandler =
            new ConnectionHandler( udpPort, tcpPort, peerId, bindings, getP2PID(), configuration, fileMessageLogger,
                                   keyPair, peerMap, timer, maxMessageSize, maintenanceThreads, replicationThreads,
                                   workerThreads );
        logger.debug( "listen done" );
        this.connectionHandler = connectionHandler;
        return connectionHandler;
    }

    ConnectionHandler listen( final Peer master )
        throws IOException
    {
        // I'm a slave
        masterFlag = false;
        this.timer = master.timer;
        this.bindings = master.bindings;
        // listen to the masters peermap
        ConnectionHandler connectionHandler =
            new ConnectionHandler( master.getConnectionHandler(), peerId, keyPair, peerMap );
        logger.debug( "listen done" );
        this.connectionHandler = connectionHandler;
        return connectionHandler;
    }

    public Map<BaseFuture, Long> getPendingFutures()
    {
        return pendingFutures;
    }

    public boolean isRunning()
    {
        return connectionHandler != null;
    }

    public boolean isListening()
    {
        if ( !isRunning() )
            return false;
        return connectionHandler.isListening();
    }

    public void customLoggerMessage( String customMessage )
    {
        getConnectionHandler().customLoggerMessage( customMessage );
    }

    public HandshakeRPC getHandshakeRPC()
    {
        if ( handshakeRCP == null )
        {
            throw new RuntimeException( "Not enabled, please enable this RPC in PeerMaker" );
        }
        return handshakeRCP;
    }

    public void setHandshakeRPC( HandshakeRPC handshakeRPC )
    {
        this.handshakeRCP = handshakeRPC;
    }

    public StorageRPC getStoreRPC()
    {
        if ( storageRPC == null )
        {
            throw new RuntimeException( "Not enabled, please enable this RPC in PeerMaker" );
        }
        return storageRPC;
    }

    public void setStorageRPC( StorageRPC storageRPC )
    {
        this.storageRPC = storageRPC;
    }

    public NeighborRPC getNeighborRPC()
    {
        if ( neighborRPC == null )
        {
            throw new RuntimeException( "Not enabled, please enable this RPC in PeerMaker" );
        }
        return neighborRPC;
    }

    public void setNeighborRPC( NeighborRPC neighborRPC )
    {
        this.neighborRPC = neighborRPC;
    }

    public QuitRPC getQuitRPC()
    {
        if ( quitRCP == null )
        {
            throw new RuntimeException( "Not enabled, please enable this RPC in PeerMaker" );
        }
        return quitRCP;
    }

    public void setQuitRPC( QuitRPC quitRCP )
    {
        this.quitRCP = quitRCP;
    }

    public PeerExchangeRPC getPeerExchangeRPC()
    {
        if ( peerExchangeRPC == null )
        {
            throw new RuntimeException( "Not enabled, please enable this RPC in PeerMaker" );
        }
        return peerExchangeRPC;
    }

    public void setPeerExchangeRPC( PeerExchangeRPC peerExchangeRPC )
    {
        this.peerExchangeRPC = peerExchangeRPC;
    }

    public DirectDataRPC getDirectDataRPC()
    {
        if ( directDataRPC == null )
        {
            throw new RuntimeException( "Not enabled, please enable this RPC in PeerMaker" );
        }
        return directDataRPC;
    }

    public void setDirectDataRPC( DirectDataRPC directDataRPC )
    {
        this.directDataRPC = directDataRPC;
    }

    public TrackerRPC getTrackerRPC()
    {
        if ( trackerRPC == null )
        {
            throw new RuntimeException( "Not enabled, please enable this RPC in PeerMaker" );
        }
        return trackerRPC;
    }

    public void setTrackerRPC( TrackerRPC trackerRPC )
    {
        this.trackerRPC = trackerRPC;
    }

    public TaskRPC getTaskRPC()
    {
        if ( taskRPC == null )
        {
            throw new RuntimeException( "Not enabled, please enable this RPC in PeerMaker" );
        }
        return taskRPC;
    }

    public void setTaskRPC( TaskRPC taskRPC )
    {
        this.taskRPC = taskRPC;
    }

    public void setBroadcastRPC( BroadcastRPC broadcastRPC )
    {
        this.broadcastRPC = broadcastRPC;

    }

    public BroadcastRPC getBroadcastRPC()
    {
        if ( broadcastRPC == null )
        {
            throw new RuntimeException( "Not enabled, please enable this RPC in PeerMaker" );
        }
        return broadcastRPC;
    }

    public DistributedRouting getDistributedRouting()
    {
        if ( distributedRouting == null )
        {
            throw new RuntimeException( "Not enabled, please enable this RPC in PeerMaker" );
        }
        return distributedRouting;
    }

    public void setDistributedRouting( DistributedRouting distributedRouting )
    {
        this.distributedRouting = distributedRouting;
    }

    public DistributedHashTable getDistributedHashMap()
    {
        if ( distributedHashMap == null )
        {
            throw new RuntimeException( "Not enabled, please enable this RPC in PeerMaker" );
        }
        return distributedHashMap;
    }

    public void setDistributedHashMap( DistributedHashTable distributedHashMap )
    {
        this.distributedHashMap = distributedHashMap;
    }

    public DistributedTracker getDistributedTracker()
    {
        if ( distributedTracker == null )
        {
            throw new RuntimeException( "Not enabled, please enable this RPC in PeerMaker" );
        }
        return distributedTracker;
    }

    public void setDistributedTracker( DistributedTracker distributedTracker )
    {
        this.distributedTracker = distributedTracker;
    }

    public AsyncTask getAsyncTask()
    {
        if ( asyncTask == null )
        {
            throw new RuntimeException( "Not enabled, please enable this RPC in PeerMaker" );
        }
        return asyncTask;
    }

    public void setAsyncTask( AsyncTask asyncTask )
    {
        this.asyncTask = asyncTask;
    }

    public DistributedTask getDistributedTask()
    {
        if ( distributedTask == null )
        {
            throw new RuntimeException( "Not enabled, please enable this RPC in PeerMaker" );
        }
        return distributedTask;
    }

    public void setDistributedTask( DistributedTask task )
    {
        this.distributedTask = task;
    }

    public List<ScheduledFuture<?>> getScheduledFutures()
    {
        return scheduledFutures;
    }

    public ConnectionHandler getConnectionHandler()
    {
        if ( connectionHandler == null )
            throw new RuntimeException( "Not listening to anything. Use the listen method first" );
        else
            return connectionHandler;
    }

    public Bindings getBindings()
    {
        if ( bindings == null )
            throw new RuntimeException( "Not listening to anything. Use the listen method first" );
        else
            return bindings;
    }

    public Timer getTimer()
    {
        if ( timer == null )
            throw new RuntimeException( "Not listening to anything. Use the listen method first" );
        else
            return timer;
    }

    public PeerBean getPeerBean()
    {
        return getConnectionHandler().getPeerBean();
    }

    public ConnectionBean getConnectionBean()
    {
        return getConnectionHandler().getConnectionBean();
    }

    public Number160 getPeerID()
    {
        return peerId;
    }

    public int getP2PID()
    {
        return p2pID;
    }

    public PeerAddress getPeerAddress()
    {
        return getPeerBean().getServerPeerAddress();
    }

    public ConnectionConfiguration getConfiguration()
    {
        return configuration;
    }

    // *********************************** DHT / Tracker operations start here

    public void setRawDataReply( final RawDataReply rawDataReply )
    {
        getDirectDataRPC().setReply( rawDataReply );
    }

    public void setObjectDataReply( final ObjectDataReply objectDataReply )
    {
        getDirectDataRPC().setReply( objectDataReply );
    }

    /**
     * Opens a TCP connection and keeps it open. The user can provide the idle timeout, which means that the connection
     * gets closed after that time of inactivity. If the other peer goes offline or closes the connection (due to
     * inactivity), further requests with this connections reopens the connection. This methods blocks until a
     * connection can be reserver.
     * 
     * @param destination The end-point to connect to
     * @param idleSeconds time in seconds after a connection gets closed if idle, -1 if it should remain always open
     *            until the user closes the connection manually.
     * @return A class that needs to be passed to those methods that should use the already open connection. If the
     *         connection could not be reserved, maybe due to a shutdown, null is returned.
     */
    public PeerConnection createPeerConnection( PeerAddress destination, int idleTCPMillis )
    {
        final FutureChannelCreator fcc =
            getConnectionBean().getConnectionReservation().reserve( 1, true, "PeerConnection" );
        fcc.awaitUninterruptibly();
        if ( fcc.isFailed() )
        {
            return null;
        }
        final ChannelCreator cc = fcc.getChannelCreator();
        final PeerConnection peerConnection =
            new PeerConnection( destination, getConnectionBean().getConnectionReservation(), cc, idleTCPMillis );
        return peerConnection;
    }

    /**
     * The Dynamic and/or Private Ports are those from 49152 through 65535
     * (http://www.iana.org/assignments/port-numbers)
     * 
     * @param internalHost
     * @param port
     * @return
     */
    public boolean setupPortForwanding( String internalHost )
    {
        int portUDP = bindings.getOutsideUDPPort();
        int portTCP = bindings.getOutsideTCPPort();
        boolean success;

        try
        {
            success =
                connectionHandler.getNATUtils().mapUPNP( internalHost, getPeerAddress().portUDP(),
                                                         getPeerAddress().portTCP(), portUDP, portTCP );
        }
        catch ( IOException e )
        {
            success = false;
        }

        if ( !success )
        {
            logger.warn( "cannot find UPNP devices" );
            try
            {
                success =
                    connectionHandler.getNATUtils().mapPMP( getPeerAddress().portUDP(), getPeerAddress().portTCP(),
                                                            portUDP, portTCP );
                if ( !success )
                {
                    logger.warn( "cannot find NAT-PMP devices" );
                }
            }
            catch ( NatPmpException e1 )
            {
                logger.warn( "cannot find NAT-PMP devices " + e1 );
            }
        }
        return success;
    }

    // New API - builder pattern --------------------------------------------------

    public SubmitBuilder submit( Number160 locationKey, Worker worker )
    {
        return new SubmitBuilder( this, locationKey, worker );
    }

    public AddBuilder add( Number160 locationKey )
    {
        return new AddBuilder( this, locationKey );
    }

    public PutBuilder put( Number160 locationKey )
    {
        return new PutBuilder( this, locationKey );
    }

    public GetBuilder get( Number160 locationKey )
    {
        return new GetBuilder( this, locationKey );
    }

    public RemoveBuilder remove( Number160 locationKey )
    {
        return new RemoveBuilder( this, locationKey );
    }

    /**
     * The send method works as follows:
     * 
     * <pre>
     * 1. routing: find close peers to the content hash. 
     *    You can control the routing behavior with 
     *    setRoutingConfiguration() 
     * 2. sending: send the data to the n closest peers. 
     *    N is set via setRequestP2PConfiguration(). 
     *    If you want to send it to the closest one, use 
     *    setRequestP2PConfiguration(1, 5, 0)
     * </pre>
     * 
     * @param locationKey The target hash to search for during the routing process
     * @return The send builder that allows to set options
     */
    public SendBuilder send( Number160 locationKey )
    {
        return new SendBuilder( this, locationKey );
    }
    
    public SendDirectBuilder sendDirect(PeerAddress recipientAddress)
    {
        return new SendDirectBuilder( this, recipientAddress );
    }
    
    public SendDirectBuilder sendDirect(PeerConnection recipientConnection)
    {
        return new SendDirectBuilder( this, recipientConnection );
    }

    @Deprecated
    public SendDirectBuilder sendDirect()
    {
        return new SendDirectBuilder( this );
    }

    public BootstrapBuilder bootstrap()
    {
        return new BootstrapBuilder( this );
    }

    public PingBuilder ping()
    {
        return new PingBuilder( this );
    }

    public DiscoverBuilder discover()
    {
        return new DiscoverBuilder( this );
    }

    public AddTrackerBuilder addTracker( Number160 locationKey )
    {
        return new AddTrackerBuilder( this, locationKey );
    }

    public GetTrackerBuilder getTracker( Number160 locationKey )
    {
        return new GetTrackerBuilder( this, locationKey );
    }

    public ParallelRequestBuilder parallelRequest( Number160 locationKey )
    {
        return new ParallelRequestBuilder( this, locationKey );
    }

    public BroadcastBuilder broadcast( Number160 messageKey )
    {
        return new BroadcastBuilder( this, messageKey );
    }

    // *************************** Connection Reservation ************************

    /**
     * Reserves a connection for a routing and DHT operation. This call does not blocks. At least one of the arguments
     * routingConfiguration or requestP2PConfiguration must not be null.
     * 
     * @param routingConfiguration The information about the routing
     * @param requestP2PConfiguration The information about the DHT operation
     * @param name The name of the ChannelCreator, used for easier debugging
     * @return A ChannelCreator that can create channel according to routingConfiguration and requestP2PConfiguration
     * @throws IllegalArgumentException If both arguments routingConfiguration and requestP2PConfiguration are null
     */
    public FutureChannelCreator reserve( final RoutingConfiguration routingConfiguration,
                                         RequestP2PConfiguration requestP2PConfiguration, String name )
    {
        if ( routingConfiguration == null && requestP2PConfiguration == null )
        {
            throw new IllegalArgumentException( "Both routingConfiguration and requestP2PConfiguration cannot be null" );
        }
        final int nrConnections;
        if ( routingConfiguration == null )
        {
            nrConnections = requestP2PConfiguration.getParallel();
        }
        else if ( requestP2PConfiguration == null )
        {
            nrConnections = routingConfiguration.getParallel();
        }
        else
        {
            nrConnections = Math.max( routingConfiguration.getParallel(), requestP2PConfiguration.getParallel() );
        }
        return getConnectionBean().getConnectionReservation().reserve( nrConnections, name );
    }

    /**
     * Release a ChannelCreator. The permits will be returned so that they can be used again. This is a wrapper for
     * ConnectionReservation.
     * 
     * @param channelCreator The ChannelCreator that is not used anymore
     */
    public void release( ChannelCreator channelCreator )
    {
        getConnectionBean().getConnectionReservation().release( channelCreator );
    }

    /**
     * Sets a timeout for this future. If the timeout passes, the future fails with the reason provided
     * 
     * @param baseFuture The future to set the timeout
     * @param millis The time in milliseconds until this future is considered a failure.
     * @param reason The reason why this future failed
     */
    public void setFutureTimeout( BaseFuture baseFuture, int millis, String reason )
    {
        getConnectionBean().getScheduler().scheduleTimeout( baseFuture, millis, reason );
    }

    /**
     * Returns true if shutdown has been initiated
     */
    public boolean isShutdown()
    {
        return shutdown;
    }

}