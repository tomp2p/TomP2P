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
package net.tomp2p.storage;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.security.PublicKey;

import net.tomp2p.peers.Number160;
import net.tomp2p.utils.Timings;
import net.tomp2p.utils.Utils;

/**
 * This class holds the data for the transport. The data is already serialized and a hash may be created. It is
 * reasonable to create the hash on the remote peer, but not on the local peer. The remote peer uses the hash to tell
 * the other peers, which version is stored and its used quite often.
 * 
 * @author draft
 */
public class Data
    implements Serializable
{
    private static final long serialVersionUID = -5023493840082652284L;

    final private byte[] data;

    final private int offset;

    final private int length;

    // never serialied over the network
    final private long validFromMillis;

    final private Number160 peerId;

    //
    private int ttlSeconds;

    private Number160 hash;

    private boolean protectedEntry;

    private boolean directReplication;

    private boolean fileReference;

    // never serialized in this object over the network
    private PublicKey publicKey;

    public Data( Object object )
        throws IOException
    {
        this( object, null );
    }

    public Data( Object object, Number160 peerId )
        throws IOException
    {
        this( Utils.encodeJavaObject( object ), peerId );
    }

    public Data( byte[] data )
    {
        this( data, null );
    }

    public Data( byte[] data, Number160 peerId )
    {
        this( data, 0, data.length, peerId );
    }

    public Data( ByteBuffer[] buffer, int length, Number160 peerId )
    {
        this.data = new byte[length];
        int offset = 0;
        for ( int i = 0; i < buffer.length; i++ )
        {
            int rem = buffer[i].remaining();
            // TODO: do not array copy, but store as is
            buffer[i].get( this.data, offset, rem );
            offset += rem;
        }
        this.offset = 0;
        this.length = length;
        this.validFromMillis = Timings.currentTimeMillis();
        this.peerId = peerId;
    }

    public Data( byte[] data, int offset, int length, Number160 peerId )
    {
        this.data = data;
        this.offset = offset;
        this.length = length;
        this.validFromMillis = Timings.currentTimeMillis();
        this.peerId = peerId;
    }

    public byte[] getData()
    {
        return data;
    }

    public Object getObject()
        throws ClassNotFoundException, IOException
    {
        return Utils.decodeJavaObject( data, this.offset, this.length );
    }

    public long getCreated()
    {
        return validFromMillis;
    }

    public Number160 getHash()
    {
        if ( this.hash == null )
            this.hash = Utils.makeSHAHash( data, offset, length );
        return hash;
    }

    public int getOffset()
    {
        return offset;
    }

    public int getLength()
    {
        return length;
    }

    public long getExpirationMillis()
    {
        return ttlSeconds <= 0 ? Long.MAX_VALUE : validFromMillis + ( ttlSeconds * 1000L );
    }

    public Number160 getPeerId()
    {
        return peerId;
    }

    public int getTTLSeconds()
    {
        return ttlSeconds;
    }

    public Data setTTLSeconds( int ttlSeconds )
    {
        this.ttlSeconds = ttlSeconds;
        return this;
    }

    public PublicKey getPublicKey()
    {
        return publicKey;
    }

    public Data setPublicKey( PublicKey publicKey )
    {
        this.publicKey = publicKey;
        return this;
    }

    public boolean isFileReference()
    {
        return fileReference;
    }

    public Data setFileReference()
    {
        this.fileReference = true;
        return this;
    }

    public Data setFileReference( boolean fileReference )
    {
        this.fileReference = fileReference;
        return this;
    }

    public boolean isProtectedEntry()
    {
        return protectedEntry;
    }

    public Data setProtectedEntry()
    {
        this.protectedEntry = true;
        return this;
    }

    public Data setProtectedEntry( boolean protectedEntry )
    {
        this.protectedEntry = protectedEntry;
        return this;
    }

    public boolean isDirectReplication()
    {
        return directReplication;
    }

    public Data setDirectReplication()
    {
        this.directReplication = true;
        return this;
    }

    public Data setDirectReplication( boolean directReplication )
    {
        this.directReplication = directReplication;
        return this;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "Data[l:" );
        sb.append( length );
        sb.append( ",t:" );
        sb.append( getTTLSeconds() );
        sb.append( ",hasPK:" );
        sb.append( publicKey != null );
        sb.append( ",h:" );
        sb.append( getHash() );
        sb.append( "]" );
        return sb.toString();
    }
}