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
package net.tomp2p.connection;

import java.security.KeyPair;

import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.Statistics;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerMap;
import net.tomp2p.replication.Replication;
import net.tomp2p.storage.StorageGeneric;
import net.tomp2p.storage.TrackerStorage;
import net.tomp2p.task.TaskManager;

/**
 * A bean that holds non-sharable (unique for each peer) configuration settings for the peer. The sharable
 * configurations are stored in {@link ConnectionBean}.
 * 
 * @author Thomas Bocek
 */
public class PeerBean
{
    // the key pair cannot be changed anymore
    private final KeyPair keyPair;

    // we need to make all volatile, as this can be called by the user from any
    // thread.
    private PeerAddress serverPeerAddress;

    private PeerMap peerMap;

    private StorageGeneric storage;

    private TrackerStorage trackerStorage;

    private Replication replicationStorage;

    private Replication replicationTracker;

    private Statistics statistics;

    private Peer peer;

    private TaskManager taskManager;

    /**
     * Creates a bean with a key pair.
     * 
     * @param keyPair The key pair that holds private public key,
     */
    public PeerBean( KeyPair keyPair )
    {
        this.keyPair = keyPair;
    }

    public PeerAddress getServerPeerAddress()
    {
        return serverPeerAddress;
    }

    public void setServerPeerAddress( PeerAddress serverPeerAddress )
    {
        this.serverPeerAddress = serverPeerAddress;
    }

    public PeerMap getPeerMap()
    {
        return peerMap;
    }

    public void setPeerMap( PeerMap routing )
    {
        this.peerMap = routing;
    }

    public void setStorage( StorageGeneric storage )
    {
        this.storage = storage;
    }

    public StorageGeneric getStorage()
    {
        return storage;
    }

    public void setTrackerStorage( TrackerStorage trackerStorage )
    {
        this.trackerStorage = trackerStorage;
    }

    public TrackerStorage getTrackerStorage()
    {
        return trackerStorage;
    }

    public KeyPair getKeyPair()
    {
        return keyPair;
    }

    public void setReplicationStorage( Replication replicationStorage )
    {
        this.replicationStorage = replicationStorage;
    }

    public Replication getReplicationStorage()
    {
        return replicationStorage;
    }

    public void setStatistics( Statistics statistics )
    {
        this.statistics = statistics;
    }

    public Statistics getStatistics()
    {
        return statistics;
    }

    public Replication getReplicationTracker()
    {
        return replicationTracker;
    }

    public void setReplicationTracker( Replication replicationTracker )
    {
        this.replicationTracker = replicationTracker;
    }

    public Peer getPeer()
    {
        return peer;
    }

    public void setPeer( Peer peer )
    {
        this.peer = peer;
    }

    public TaskManager getTaskManager()
    {
        return taskManager;
    }

    public void setTaskManager( TaskManager taskManager )
    {
        this.taskManager = taskManager;
    }
}