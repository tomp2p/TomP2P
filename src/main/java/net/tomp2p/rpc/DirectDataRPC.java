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
package net.tomp2p.rpc;

import net.tomp2p.connection.ChannelCreator;
import net.tomp2p.connection.ConnectionBean;
import net.tomp2p.connection.PeerBean;
import net.tomp2p.futures.FutureResponse;
import net.tomp2p.message.Message;
import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Utils;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectDataRPC
    extends ReplyHandler
{
    final private static Logger logger = LoggerFactory.getLogger( DirectDataRPC.class );
    
    private volatile RawDataReply rawDataReply;

    private volatile ObjectDataReply objectDataReply;

    public DirectDataRPC( PeerBean peerBean, ConnectionBean connectionBean )
    {
        super( peerBean, connectionBean );
        registerIoHandler( Command.DIRECT_DATA );
    }

    public FutureResponse send( final PeerAddress remotePeer, final ChannelBuffer buffer, boolean raw,
                                ChannelCreator channelCreator, boolean forceUDP )
    {
        return send( remotePeer, buffer, raw, channelCreator,
                     getConnectionBean().getConfiguration().getIdleTCPMillis(), forceUDP );
    }

    /**
     * Send data directly to a peer. Make sure you have set up a reply handler. This is an RPC.
     * 
     * @param remotePeer The remote peer to store the data
     * @param buffer The data to send to the remote peer
     * @param raw Set to true if a the byte array is expected or if it should be converted to an object
     * @param channelCreator The channel creator
     * @param idleTCPMillis Set the timeout when a connection is considered inactive (idle)
     * @param forceUDP Set to true if the communication should be UDP, default is TCP
     * @return FutureResponse that stores which content keys have been stored.
     */
    public FutureResponse send( final PeerAddress remotePeer, final ChannelBuffer buffer, boolean raw,
                                ChannelCreator channelCreator, int idleTCPMillis, boolean forceUDP )
    {
        final Message message = createMessage( remotePeer, Command.DIRECT_DATA, raw ? Type.REQUEST_1 : Type.REQUEST_2 );
        message.setPayload( buffer );
        final FutureResponse futureResponse = new FutureResponse( message, raw );
        if ( !forceUDP )
        {
            final RequestHandlerTCP<FutureResponse> requestHandler =
                new RequestHandlerTCP<FutureResponse>( futureResponse, getPeerBean(), getConnectionBean(), message );
            return requestHandler.sendTCP( channelCreator, idleTCPMillis );
        }
        else
        {
            final RequestHandlerUDP<FutureResponse> requestHandler =
                new RequestHandlerUDP<FutureResponse>( futureResponse, getPeerBean(), getConnectionBean(), message );
            return requestHandler.sendUDP( channelCreator );
        }
    }

    /**
     * Prepares for sending to a remote peer.
     * 
     * @param remotePeer The remote peer to store the data
     * @param buffer The data to send to the remote peer
     * @param raw Set to true if a the byte array is expected or if it should be converted to an object
     * @return The request handler that sends with TCP
     */
    public RequestHandlerTCP<FutureResponse> prepareSend( final PeerAddress remotePeer, final ChannelBuffer buffer,
                                                          boolean raw )
    {
        final Message message = createMessage( remotePeer, Command.DIRECT_DATA, raw ? Type.REQUEST_1 : Type.REQUEST_2 );
        message.setPayload( buffer );
        final FutureResponse futureResponse = new FutureResponse( message, raw );
        final RequestHandlerTCP<FutureResponse> requestHandler =
            new RequestHandlerTCP<FutureResponse>( futureResponse, getPeerBean(), getConnectionBean(), message );
        return requestHandler;
    }

    public void setReply( final RawDataReply rawDataReply )
    {
        this.rawDataReply = rawDataReply;
    }

    public void setReply( ObjectDataReply objectDataReply )
    {
        this.objectDataReply = objectDataReply;
    }

    public boolean hasRawDataReply()
    {
        return rawDataReply != null;
    }

    public boolean hasObjectDataReply()
    {
        return objectDataReply != null;
    }

    @Override
    public Message handleResponse( final Message message, boolean sign )
        throws Exception
    {
        if ( !( ( message.getType() == Type.REQUEST_1 || message.getType() == Type.REQUEST_2 ) && message.getCommand() == Command.DIRECT_DATA ) )
        {
            throw new IllegalArgumentException( "Message content is wrong" );
        }
        final Message responseMessage = createResponseMessage( message, Type.OK );
        if ( sign )
        {
            responseMessage.setPublicKeyAndSign( getPeerBean().getKeyPair() );
        }
        final RawDataReply rawDataReply2 = rawDataReply;
        final ObjectDataReply objectDataReply2 = objectDataReply;
        if ( message.getType() == Type.REQUEST_1 && rawDataReply2 == null )
        {
            responseMessage.setType( Type.NOT_FOUND );
        }
        else if ( message.getType() == Type.REQUEST_2 && objectDataReply2 == null )
        {
            responseMessage.setType( Type.NOT_FOUND );
        }
        else
        {
            final ChannelBuffer requestBuffer = message.getPayload1();
            // the user can reply with null, indicating not found. Or
            // returning the request buffer, which means nothing is
            // returned. Or an exception can be thrown
            if ( message.getType() == Type.REQUEST_1 )
            {
                if(logger.isDebugEnabled()) {
                    logger.debug( "handling requet1");
                }
                final ChannelBuffer replyBuffer = rawDataReply2.reply( message.getSender(), requestBuffer );
                if ( replyBuffer == null )
                    responseMessage.setType( Type.NOT_FOUND );
                else if ( replyBuffer == requestBuffer )
                    responseMessage.setType( Type.OK );
                else
                    responseMessage.setPayload( replyBuffer );
            }
            else
            {
                Object obj = Utils.decodeJavaObject( requestBuffer );
                if(logger.isDebugEnabled()) {
                    logger.debug( "handling " + obj);
                }
                Object reply = objectDataReply2.reply( message.getSender(), obj );
                if ( reply == null )
                    responseMessage.setType( Type.NOT_FOUND );
                else if ( reply == obj )
                    responseMessage.setType( Type.OK );
                else
                {
                    byte[] me = Utils.encodeJavaObject( reply );
                    responseMessage.setPayload( ChannelBuffers.wrappedBuffer( me ) );
                }
            }
        }
        return responseMessage;
    }
}