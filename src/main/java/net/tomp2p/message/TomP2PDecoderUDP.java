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
package net.tomp2p.message;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.security.Signature;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.ChannelHandler.Sharable;

@Sharable
public class TomP2PDecoderUDP
    implements ChannelUpstreamHandler
{
    @Override
    public void handleUpstream( final ChannelHandlerContext ctx, final ChannelEvent evt )
        throws Exception
    {
        if ( !( evt instanceof MessageEvent ) )
        {
            ctx.sendUpstream( evt );
            return;
        }
        final MessageEvent e = (MessageEvent) evt;
        final Object originalMessage = e.getMessage();
        if ( !( originalMessage instanceof ChannelBuffer ) )
        {
            ctx.sendUpstream( evt );
            return;
        }
        Message message = decode( ctx, e.getChannel(), (ChannelBuffer) originalMessage, e.getRemoteAddress() );
        if ( message != null )
        {
            Channels.fireMessageReceived( ctx, message, e.getRemoteAddress() );
        }
    }

    protected Message decode( final ChannelHandlerContext ctx, final Channel channel, final ChannelBuffer buffer,
                              final SocketAddress socketAddress )
        throws Exception
    {
        if ( buffer.readableBytes() < MessageCodec.HEADER_SIZE )
        {
            Channels.fireExceptionCaught( ctx, new DecoderException( "did not get all the data expected (1), got "
                + buffer.readableBytes() ) );
            return null;
        }

        int readerIndex = buffer.readerIndex();
        final InetSocketAddress localSocket = (InetSocketAddress) channel.getLocalAddress();
        final Message message = MessageCodec.decodeHeader( buffer, localSocket, ( (InetSocketAddress) socketAddress ) );
        // set finished time before, as the sender already started its timer
        message.setUDP();
        message.finished();
        if ( message.hasContent() )
        {
            if ( !MessageCodec.decodePayload( message.getContentType1(), buffer, message ) )
            {
                Channels.fireExceptionCaught( ctx, new DecoderException( "did not get all the data expected (2), got "
                    + buffer.readableBytes() ) );
                return null;
            }
            if ( !MessageCodec.decodePayload( message.getContentType2(), buffer, message ) )
            {
                Channels.fireExceptionCaught( ctx, new DecoderException( "did not get all the data expected (3), got "
                    + buffer.readableBytes() ) );
                return null;
            }
            if ( !MessageCodec.decodePayload( message.getContentType3(), buffer, message ) )
            {
                Channels.fireExceptionCaught( ctx, new DecoderException( "did not get all the data expected (4), got "
                    + buffer.readableBytes() ) );
                return null;
            }
            if ( !MessageCodec.decodePayload( message.getContentType4(), buffer, message ) )
            {
                Channels.fireExceptionCaught( ctx, new DecoderException( "did not get all the data expected (5), got "
                    + buffer.readableBytes() ) );
                return null;
            }
            if ( message.isHintSign() )
            {
                Signature signature = Signature.getInstance( "SHA1withDSA" );
                signature.initVerify( message.getPublicKey() );
                int read = buffer.readerIndex() - readerIndex;
                if ( read > 0 )
                {
                    ByteBuffer[] tmp = buffer.toByteBuffers( readerIndex, read );
                    for ( int i = 0; i < tmp.length; i++ )
                    {
                        signature.update( tmp[i] );
                    }
                }
                // dont worry about the return value
                if ( !MessageCodec.decodeSignature( signature, message, buffer ) )
                {
                    Channels.fireExceptionCaught( ctx, new DecoderException(
                                                                             "did not get all the data expected (6), got "
                                                                                 + buffer.readableBytes() ) );
                    return null;
                }
            }
        }
        return message;
    }
}
