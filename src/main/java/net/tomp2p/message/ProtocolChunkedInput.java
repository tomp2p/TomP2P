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
package net.tomp2p.message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.DefaultFileRegion;
import org.jboss.netty.handler.stream.ChunkedInput;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;

public class ProtocolChunkedInput
    implements ChunkedInput, ProtocolChunked
{
    private final ChannelHandlerContext ctx;

    private final Queue<Object> queue = new ConcurrentLinkedQueue<Object>();

    private ChannelBuffer channelBuffer = ChannelBuffers.dynamicBuffer();

    private volatile boolean done = false;

    private final Signature signature;

    public ProtocolChunkedInput( ChannelHandlerContext ctx, PrivateKey privateKey )
        throws NoSuchAlgorithmException, InvalidKeyException
    {
        this.ctx = ctx;
        if ( privateKey != null )
        {
            signature = Signature.getInstance( "SHA1withDSA" );
            signature.initSign( privateKey );
        }
        else
        {
            signature = null;
        }
    }

    @Override
    public boolean hasNextChunk()
        throws Exception
    {
        return !queue.isEmpty();
    }

    @Override
    public Object nextChunk()
        throws Exception
    {
        Object object = queue.poll();
        if ( object == null )
        {
            return null;
        }
        if ( object instanceof ChannelBuffer )
        {
            ChannelBuffer channelBuffer = (ChannelBuffer) object;
            if ( signature != null && channelBuffer != ChannelBuffers.EMPTY_BUFFER )
            {
                ByteBuffer[] tmp = channelBuffer.duplicate().toByteBuffers();
                for ( int i = 0; i < tmp.length; i++ )
                {
                    signature.update( tmp[i] );
                }
            }
            else if ( signature != null && channelBuffer == ChannelBuffers.EMPTY_BUFFER )
            {
                byte[] signatureData = signature.sign();
                SHA1Signature decodedSignature = new SHA1Signature();
                decodedSignature.decode( signatureData );
                channelBuffer =
                    ChannelBuffers.wrappedBuffer( decodedSignature.getNumber1().toByteArray(),
                                                  decodedSignature.getNumber2().toByteArray() );
            }
            return channelBuffer;
        }
        if ( object instanceof DefaultFileRegion )
        {
            DefaultFileRegion fileRegion = (DefaultFileRegion) object;
            if ( signature != null )
            {
                // TODO: implement the update of the signature for filechannel / fileregion
            }
            // ByteBuffer chunk = ByteBuffer.wrap(2*1024*1024);
            // fileRegion.
            return fileRegion;
        }
        else
        {
            throw new RuntimeException( "unknown object" );
        }
    }

    public int size()
    {
        return queue.size();
    }

    public void addMarkerForSignature()
    {
        flush( true );
        queue.add( ChannelBuffers.EMPTY_BUFFER );
        done = true;
    }

    @Override
    public boolean isEndOfInput()
        throws Exception
    {
        return done && !hasNextChunk();
    }

    @Override
    public void close()
        throws Exception
    {
        done = true;
    }

    public void resume()
    {
        ChunkedWriteHandler chunkedWriteHandler = (ChunkedWriteHandler) ctx.getPipeline().get( "streamer" );
        chunkedWriteHandler.resumeTransfer();
    }

    /*
     * (non-Javadoc)
     * @see net.tomp2p.message.ProtocolChunked#copyToCurrent(byte[])
     */
    @Override
    public void copyToCurrent( byte[] byteArray )
    {
        if ( done )
            return;
        channelBuffer.writeBytes( byteArray );
    }

    /*
     * (non-Javadoc)
     * @see net.tomp2p.message.ProtocolChunked#copyToCurrent(int)
     */
    @Override
    public void copyToCurrent( int size )
    {
        if ( done )
            return;
        channelBuffer.writeInt( size );
    }

    /*
     * (non-Javadoc)
     * @see net.tomp2p.message.ProtocolChunked#copyToCurrent(byte)
     */
    @Override
    public void copyToCurrent( byte size )
    {
        if ( done )
            return;
        channelBuffer.writeByte( size );
    }

    /*
     * (non-Javadoc)
     * @see net.tomp2p.message.ProtocolChunked#copyToCurrent(long)
     */
    @Override
    public void copyToCurrent( long long1 )
    {
        if ( done )
            return;
        channelBuffer.writeLong( long1 );
    }

    /*
     * (non-Javadoc)
     * @see net.tomp2p.message.ProtocolChunked#copyToCurrent(short)
     */
    @Override
    public void copyToCurrent( short short1 )
    {
        if ( done )
            return;
        channelBuffer.writeShort( short1 );
    }

    /*
     * (non-Javadoc)
     * @see net.tomp2p.message.ProtocolChunked#copyToCurrent(org.jboss.netty.buffer.ChannelBuffer)
     */
    @Override
    public void copyToCurrent( ChannelBuffer slice )
    {
        if ( done )
            return;
        if ( slice.writerIndex() == 0 )
            return;
        flush( false );
        queue.add( slice );
    }

    /*
     * (non-Javadoc)
     * @see net.tomp2p.message.ProtocolChunked#copyToCurrent(byte[], int, int)
     */
    @Override
    public void copyToCurrent( byte[] array, int offset, int length )
    {
        if ( done )
            return;
        if ( length == 0 )
            return;
        flush( false );
        queue.add( ChannelBuffers.wrappedBuffer( array, offset, length ) );
    }

    public void flush( boolean last )
    {
        if ( channelBuffer.writerIndex() > 0 )
        {
            queue.add( channelBuffer );
            if ( !last )
            {
                channelBuffer = ChannelBuffers.dynamicBuffer();
            }
        }
        if ( last )
            done = true;
    }

    @Override
    public void transferToCurrent( FileChannel inChannel, long length )
        throws IOException
    {
        if ( done )
            return;
        if ( length == 0 )
            return;
        flush( false );
        DefaultFileRegion defaultFileRegion = new DefaultFileRegion( inChannel, 0, length );
        queue.add( defaultFileRegion );
    }
}