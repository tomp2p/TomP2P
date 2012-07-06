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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Content;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.HashData;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.TrackerData;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

public class MessageCodec
{
    final public static byte[] EMPTY_BYTE_ARRAY = new byte[] {};

    final public static int MAX_BYTE = 255;

    final public static int HEADER_SIZE = 56;

    /**
     * The format looks as follows: 32bit p2p version - 32bit id - 4bit message type - 4bit message name - 160bit sender
     * id - 16bit tcp port - 16bit udp port - 160bit recipient id - 16bit (4x4)content type - 8bit network address
     * information. It total, the header is of size 56 bytes.
     * 
     * @param buffer The Netty buffer to fill
     * @param message The message with the header that will be serialized
     * @return The buffer passed as an argument
     */
    public static ChannelBuffer encodeHeader( final ChannelBuffer buffer, final Message message )
    {
        buffer.writeInt( message.getVersion() ); // 4
        buffer.writeInt( message.getMessageId() ); // 8
        buffer.writeByte( ( message.getType().ordinal() << 4 ) | message.getCommand().ordinal() ); // 9
        buffer.writeBytes( message.getSender().getID().toByteArray() ); // 29
        buffer.writeShort( (short) message.getSender().portTCP() ); // 31
        buffer.writeShort( (short) message.getSender().portUDP() ); // 33
        buffer.writeBytes( message.getRecipient().getID().toByteArray() ); // 53
        final int content =
            ( ( message.getContentType4().ordinal() << 12 ) | ( message.getContentType3().ordinal() << 8 )
                | ( message.getContentType2().ordinal() << 4 ) | message.getContentType1().ordinal() );
        buffer.writeShort( (short) content ); // 55
        // options
        int options = ( message.getSender().getOptions() << 4 ) | message.getOptions();
        buffer.writeByte( options ); // 56
        return buffer;
    }

    /**
     * Encode payload
     * 
     * @param buffer The Netty buffer to fill
     * @param message The message which contains the payload
     * @return The same buffer, passed as an argument
     * @throws NoSuchAlgorithmException
     * @throws SignatureException
     * @throws InvalidKeyException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static void encodePayload( final Message message, ProtocolChunkedInput input )
        throws InvalidKeyException, SignatureException, NoSuchAlgorithmException, ClassNotFoundException, IOException
    {
        int contentLength = 0;
        if ( message.getContentType1() != Message.Content.EMPTY )
        {
            contentLength += encodePayloadType( message.getContentType1(), input, message );
            if ( message.getContentType2() != Message.Content.EMPTY )
            {
                contentLength += encodePayloadType( message.getContentType2(), input, message );
                if ( message.getContentType3() != Message.Content.EMPTY )
                {
                    contentLength += encodePayloadType( message.getContentType3(), input, message );

                    if ( message.getContentType4() != Message.Content.EMPTY )
                    {
                        contentLength += encodePayloadType( message.getContentType4(), input, message );
                    }
                }
            }
        }
        if ( message.isHintSign() )
        {
            input.addMarkerForSignature();
        }
        else
            input.flush( true );
        message.setLength( contentLength );
    }

    /**
     * Encodes payload in a big switch statement. Types are: EMPTY, KEY_KEY, PUBLIC_KEY, KEY_KEY_PUBLIC_KEY,
     * MAP_KEY_DATA, MAP_KEY_DATA_TTL, SET_DATA_TTL, MAP_KEY_KEY, SET_KEYS, SET_NEIGHBORS, BYTE_ARRAY, INTEGER, USER1,
     * USER2, USER3
     * 
     * @param contentType The type of the content to encode
     * @param input The ProtocolChunkedInput to fill
     * @param message The message which contains the payload
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private static int encodePayloadType( final Content content, final ProtocolChunked input, final Message message )
        throws ClassNotFoundException, IOException
    {
        final int size;
        final byte[] data;
        int count;
        switch ( content )
        {
            case KEY:
                input.copyToCurrent( message.getKey().toByteArray() );
                return 20;
            case KEY_KEY:
                input.copyToCurrent( message.getKeyKey1().toByteArray() );
                input.copyToCurrent( message.getKeyKey2().toByteArray() );
                return 40;
            case MAP_KEY_DATA:
                count = 4;
                if ( message.isConvertNumber480to160() )
                {
                    Map<Number480, Data> dataMap = message.getDataMapConvert();
                    input.copyToCurrent( dataMap.size() );
                    for ( Map.Entry<Number480, Data> entry : dataMap.entrySet() )
                    {
                        input.copyToCurrent( entry.getKey().getContentKey().toByteArray() );
                        count += 20;
                        count += DataCodec.encodeData( input, entry.getValue() );
                    }
                    return count;
                }
                else
                {
                    input.copyToCurrent( message.getDataMap().size() );
                    for ( Map.Entry<Number160, Data> entry : message.getDataMap().entrySet() )
                    {
                        input.copyToCurrent( entry.getKey().toByteArray() );
                        count += 20;
                        count += DataCodec.encodeData( input, entry.getValue() );
                    }
                    return count;
                }
            case MAP_KEY_COMPARE_DATA:
                count = 4;
                input.copyToCurrent( message.getHashDataMap().size() );
                for ( Map.Entry<Number160, HashData> entry : message.getHashDataMap().entrySet() )
                {
                    input.copyToCurrent( entry.getKey().toByteArray() );
                    count += 20;
                    input.copyToCurrent( entry.getValue().getHash().toByteArray() );
                    count += 20;
                    count += DataCodec.encodeData( input, entry.getValue().getData() );
                }
                return count;
            case MAP_KEY_KEY:
                Map<Number160, Number160> keyMap = message.getKeyMap();
                size = keyMap.size();
                input.copyToCurrent( size );
                for ( final Map.Entry<Number160, Number160> entry : keyMap.entrySet() )
                {
                    input.copyToCurrent( entry.getKey().toByteArray() );
                    input.copyToCurrent( entry.getValue().toByteArray() );
                }
                return 4 + ( size * ( 20 + 20 ) );
            case SET_KEYS:
                if ( message.isConvertNumber480to160() )
                {
                    Collection<Number480> keys = message.getKeysConvert();
                    size = keys.size();
                    input.copyToCurrent( size );
                    for ( Number480 key : keys )
                        input.copyToCurrent( key.getContentKey().toByteArray() );
                    return 4 + ( size * 20 );
                }
                else
                {
                    size = message.getKeys().size();
                    input.copyToCurrent( size );
                    for ( Number160 key : message.getKeys() )
                        input.copyToCurrent( key.toByteArray() );
                    return 4 + ( size * 20 );
                }
            case SET_NEIGHBORS:
                count = 1;
                size = Math.min( message.getNeighbors().size(), Math.min( message.getUseAtMostNeighbors(), MAX_BYTE ) );
                input.copyToCurrent( (byte) size );
                final Iterator<PeerAddress> iterator = message.getNeighbors().iterator();
                for ( int i = 0; iterator.hasNext() && i < size; i++ )
                {
                    byte[] data1 = iterator.next().toByteArray();
                    input.copyToCurrent( data1 );
                    count += data1.length;
                }
                return count;
            case SET_TRACKER_DATA:
                count = 1;
                final Collection<TrackerData> trackerDatas = message.getTrackerData();
                input.copyToCurrent( (byte) trackerDatas.size() );
                for ( TrackerData trackerData : trackerDatas )
                {
                    byte[] data1 = trackerData.getPeerAddress().toByteArray();
                    input.copyToCurrent( data1 );
                    count += data1.length;
                    if ( trackerData.getAttachement() != null )
                    {
                        input.copyToCurrent( (byte) 1 );
                        input.copyToCurrent( trackerData.getLength() );
                        count += 5;
                        input.copyToCurrent( trackerData.getAttachement(), trackerData.getOffset(),
                                             trackerData.getLength() );
                        count += trackerData.getLength();
                    }
                    else
                    {
                        input.copyToCurrent( (byte) 0 );
                        count += 1;
                    }
                }
                return count;
            case CHANNEL_BUFFER:
                ChannelBuffer tmpBuffer = message.getPayload1();
                if ( tmpBuffer == null )
                    tmpBuffer = message.getPayload2();
                else
                    message.setPayload1( null );
                size = tmpBuffer.writerIndex();
                input.copyToCurrent( size );
                input.copyToCurrent( tmpBuffer.slice() );
                return 4 + size;
            case LONG:
                input.copyToCurrent( message.getLong() );
                return 8;
            case INTEGER:
                input.copyToCurrent( message.getInteger() );
                return 4;
            case PUBLIC_KEY:
                data = message.getPublicKey().getEncoded();
                size = data.length;
                input.copyToCurrent( (short) size );
                input.copyToCurrent( data );
                return 2 + size;
            case PUBLIC_KEY_SIGNATURE:
                // flag to encode public key
                data = message.getPublicKey().getEncoded();
                size = data.length;
                input.copyToCurrent( (short) size );
                input.copyToCurrent( data );
                message.setHintSign( true );
                // count 40 for the signature, which comes later in ProtocolChunkedInput
                return 40 + 2 + size;
            case TWO_BLOOM_FILTER:
                SimpleBloomFilter<Number160> bloomFilter1 = message.getBloomFilter1();
                // TODO: idea, pass the buffer to toByteArray for direct writing
                final int size1;
                if ( bloomFilter1 != null )
                {
                    byte[] raw = bloomFilter1.toByteArray();
                    size1 = raw.length;
                    input.copyToCurrent( size1 );
                    input.copyToCurrent( raw );
                }
                else
                {
                    size1 = 0;
                    input.copyToCurrent( size1 );
                }
                // second bloomfilter
                SimpleBloomFilter<Number160> bloomFilter2 = message.getBloomFilter2();
                // TODO: idea, pass the buffer to toByteArray for direct writing
                final int size2;
                if ( bloomFilter2 != null )
                {
                    byte[] raw = bloomFilter2.toByteArray();
                    size2 = raw.length;
                    input.copyToCurrent( size2 );
                    input.copyToCurrent( raw );
                }
                else
                {
                    size2 = 0;
                    input.copyToCurrent( size2 );
                }
                return 8 + size1 + size2;
            case EMPTY:
            case RESERVED1:
            default:
                return 0;
        }
    }

    /**
     * Decode a message header from a Netty buffer
     * 
     * @param buffer The buffer to decode from
     * @param sender The sender of the packet, which has been set in the socket class
     * @return The partial message, only the header fields are filled
     */
    public static Message decodeHeader( final ChannelBuffer buffer, final InetSocketAddress recipient,
                                        final InetSocketAddress sender )
        throws DecoderException
    {
        final Message message = new Message();
        message.setVersion( buffer.readInt() );
        message.setMessageId( buffer.readInt() );
        //
        final int typeCommand = buffer.readUnsignedByte();
        message.setType( Type.values()[typeCommand >>> 4] );
        message.setCommand( Command.values()[typeCommand & 0xf] );
        final Number160 senderID = readID( buffer );
        final int portTCP = buffer.readUnsignedShort();
        final int portUDP = buffer.readUnsignedShort();
        final Number160 recipientID = readID( buffer );
        message.setRecipient( new PeerAddress( recipientID, recipient ) );
        final int contentType = buffer.readUnsignedShort();
        message.setContentType( Content.values()[contentType & 0xf], Content.values()[( contentType >>> 4 ) & 0xf],
                                Content.values()[( contentType >>> 8 ) & 0xf], Content.values()[contentType >>> 12] );
        // set the address as we see it, important for port forwarding
        // identification
        final int senderMessageOptions = buffer.readUnsignedByte();
        final int senderOptions = senderMessageOptions >>> 4;
        final PeerAddress peerAddress =
            new PeerAddress( senderID, sender.getAddress(), portTCP, portUDP, senderOptions );
        message.setSender( peerAddress );
        final int options = senderMessageOptions & 0xf;
        message.setOptions( options );
        return message;
    }

    /**
     * Decodes the payload from a Netty buffer in a big switch
     * 
     * @param content The content type
     * @param buffer The buffer to read from
     * @param message The message to store the results
     * @throws IndexOutOfBoundsException If a buffer is read beyond its limits
     * @throws NoSuchAlgorithmException
     * @throws SignatureException
     * @throws InvalidKeyException
     * @throws InvalidKeySpecException
     * @throws InvalidKeySpecException
     * @throws IOException
     * @throws DecoderException
     * @throws ASN1Exception
     * @throws UnsupportedEncodingException If UTF-8 is not there
     */
    public static boolean decodePayload( final Content content, final ChannelBuffer buffer, final Message message )
        throws InvalidKeyException, SignatureException, NoSuchAlgorithmException, InvalidKeySpecException, IOException,
        DecoderException
    {
        final int len;
        byte[] me;
        switch ( content )
        {
            case KEY:
                if ( buffer.readableBytes() < 20 )
                    return false;
                message.setKey0( readID( buffer ) );
                return true;
            case KEY_KEY:
                if ( buffer.readableBytes() < 40 )
                    return false;
                message.setKeyKey0( readID( buffer ), readID( buffer ) );
                return true;
            case MAP_KEY_DATA:
                if ( buffer.readableBytes() < 4 )
                    return false;
                len = buffer.readInt();
                Map<Number160, Data> result = new HashMap<Number160, Data>( len );
                for ( int i = 0; i < len; i++ )
                {
                    if ( buffer.readableBytes() < 20 )
                        return false;
                    Number160 key = readID( buffer );
                    final Data data = DataCodec.decodeData( buffer, message.getSender() );
                    if ( data == null )
                        return false;
                    if ( message.isRequest() )
                    {
                        if ( data.isProtectedEntry() && message.getPublicKey() == null )
                            throw new DecoderException(
                                                        "You indicated that you want to protect the data, but you did not provide or provided too late a public key." );
                        data.setPublicKey( message.getPublicKey() );
                    }
                    result.put( key, data );
                }
                message.setDataMap0( result );
                return true;
            case MAP_KEY_COMPARE_DATA:
                if ( buffer.readableBytes() < 4 )
                    return false;
                len = buffer.readInt();
                Map<Number160, HashData> result2 = new HashMap<Number160, HashData>( len );
                for ( int i = 0; i < len; i++ )
                {
                    if ( buffer.readableBytes() < 20 )
                        return false;
                    Number160 key = readID( buffer );
                    if ( buffer.readableBytes() < 20 )
                        return false;
                    Number160 hash = readID( buffer );
                    final Data data = DataCodec.decodeData( buffer, message.getSender() );
                    if ( data == null )
                        return false;
                    if ( message.isRequest() )
                    {
                        if ( data.isProtectedEntry() && message.getPublicKey() == null )
                            throw new DecoderException(
                                                        "You indicated that you want to protect the data, but you did not provide or provided too late a public key." );
                        data.setPublicKey( message.getPublicKey() );
                    }
                    result2.put( key, new HashData( hash, data ) );
                }
                message.setHashDataMap0( result2 );
                return true;
            case MAP_KEY_KEY:
                if ( buffer.readableBytes() < 4 )
                    return false;
                len = buffer.readInt();
                if ( buffer.readableBytes() < ( ( 20 + 20 ) * len ) )
                    return false;
                final Map<Number160, Number160> keyMap = new HashMap<Number160, Number160>();
                for ( int i = 0; i < len; i++ )
                {
                    final Number160 key1 = readID( buffer );
                    final Number160 key2 = readID( buffer );
                    keyMap.put( key1, key2 );
                }
                message.setKeyMap0( keyMap );
                return true;
            case SET_KEYS:
                // can be 31bit long ~ 2GB
                if ( buffer.readableBytes() < 4 )
                    return false;
                len = buffer.readInt();
                if ( buffer.readableBytes() < ( 20 * len ) )
                    return false;
                final Collection<Number160> tmp = new ArrayList<Number160>( len );
                for ( int i = 0; i < len; i++ )
                {
                    Number160 key = readID( buffer );
                    tmp.add( key );
                }
                message.setKeys0( tmp );
                return true;
            case SET_NEIGHBORS:
                if ( buffer.readableBytes() < 1 )
                    return false;
                len = buffer.readUnsignedByte();
                if ( buffer.readableBytes() < ( len * PeerAddress.SIZE_IPv4 ) )
                    return false;
                final Collection<PeerAddress> neighbors = new ArrayList<PeerAddress>( len );
                for ( int i = 0; i < len; i++ )
                {
                    PeerAddress peerAddress = readPeerAddress( buffer );
                    if ( peerAddress == null )
                        return false;
                    neighbors.add( peerAddress );
                }
                message.setNeighbors0( neighbors );
                return true;
            case SET_TRACKER_DATA:
                if ( buffer.readableBytes() < 1 )
                    return false;
                len = buffer.readUnsignedByte();
                if ( buffer.readableBytes() < ( len * ( PeerAddress.SIZE_IPv4 + 1 ) ) )
                    return false;
                final Collection<TrackerData> trackerDatas = new ArrayList<TrackerData>( len );
                for ( int i = 0; i < len; i++ )
                {
                    PeerAddress peerAddress = readPeerAddress( buffer );
                    if ( peerAddress == null )
                        return false;
                    byte[] attachment = null;
                    int offset = 0;
                    int length = 0;
                    if ( buffer.readableBytes() < 1 )
                        return false;
                    byte miniHeader = buffer.readByte();
                    if ( miniHeader != 0 )
                    {
                        if ( buffer.readableBytes() < 4 )
                            return false;
                        length = buffer.readInt();
                        if ( buffer.readableBytes() < length )
                            return false;
                        attachment = new byte[length];
                        buffer.readBytes( attachment );
                    }
                    trackerDatas.add( new TrackerData( peerAddress, message.getSender(), attachment, offset, length ) );
                }
                message.setTrackerData0( trackerDatas );
                return true;
            case CHANNEL_BUFFER:
                if ( buffer.readableBytes() < 4 )
                    return false;
                len = buffer.readInt();
                if ( len == 0 )
                {
                    message.setPayload0( ChannelBuffers.EMPTY_BUFFER );
                    return true;
                }
                if ( buffer.readableBytes() < len )
                    return false;
                // you can only use slice if no execution handler is in place,
                // otherwise, you will overwrite stuff
                // TODO find out why we need to copy!
                final ChannelBuffer tmpBuffer = buffer.copy( buffer.readerIndex(), len );
                buffer.skipBytes( len );
                message.setPayload0( tmpBuffer );
                return true;
            case LONG:
                if ( buffer.readableBytes() < 8 )
                    return false;
                message.setLong0( buffer.readLong() );
                return true;
            case INTEGER:
                if ( buffer.readableBytes() < 4 )
                    return false;
                message.setInteger0( buffer.readInt() );
                return true;
            case PUBLIC_KEY:
            case PUBLIC_KEY_SIGNATURE:
                if ( buffer.readableBytes() < 2 )
                    return false;
                len = buffer.readUnsignedShort();
                me = new byte[len];
                if ( buffer.readableBytes() < len )
                    return false;
                message.setPublicKey0( decodePublicKey( buffer, me ) );
                if ( content == Content.PUBLIC_KEY_SIGNATURE )
                {
                    message.setHintSign( true );
                }
                return true;
            case TWO_BLOOM_FILTER:
                if ( buffer.readableBytes() < 4 )
                    return false;
                len = buffer.readInt();
                SimpleBloomFilter<Number160> sbf1 = null;
                if ( len > 0 )
                {
                    me = new byte[len];
                    if ( buffer.readableBytes() < len )
                        return false;
                    // TODO: copy directly from buffer
                    buffer.readBytes( me );
                    sbf1 = new SimpleBloomFilter<Number160>( me );
                }
                // second bloom filter
                if ( buffer.readableBytes() < 4 )
                    return false;
                int len2 = buffer.readInt();
                SimpleBloomFilter<Number160> sbf2 = null;
                if ( len2 > 0 )
                {
                    me = new byte[len2];
                    if ( buffer.readableBytes() < len2 )
                        return false;
                    // TODO: copy directly from buffer
                    buffer.readBytes( me );
                    sbf2 = new SimpleBloomFilter<Number160>( me );
                }
                message.setTwoBloomFilter0( sbf1, sbf2 );
                return true;
            case EMPTY:
            case RESERVED1:
            default:
                return true;
        }
    }

    /**
     * Read a 160bit number from a Netty buffer. I did not want to include ChannelBuffer in the class Number160.
     * 
     * @param buffer The Netty buffer
     * @return A 160bit number from the Netty buffer (deserialized)
     */
    private static Number160 readID( final ChannelBuffer buffer )
    {
        byte[] me = new byte[Number160.BYTE_ARRAY_SIZE];
        buffer.readBytes( me );
        return new Number160( me );
    }

    /*
     * private static ChannelBuffer writePeerAddress(PeerAddress peerAddress) { return
     * ChannelBuffers.wrappedBuffer(peerAddress.toByteArray()); }
     */

    /**
     * Read a PeerAddress from a Netty buffer. I did not want to include ChannelBuffer in the class PeerAddress
     * 
     * @param buffer The Netty buffer
     * @return A PeerAddress created from the buffer (deserialized)
     */
    private static PeerAddress readPeerAddress( final ChannelBuffer buffer )
    {
        if ( buffer.readableBytes() < 21 )
            return null;
        Number160 id = readID( buffer );
        // peek
        int type = buffer.getUnsignedByte( buffer.readerIndex() );
        // now we know the length
        int len = PeerAddress.expectedSocketLength( type );
        if ( buffer.readableBytes() < len )
            return null;
        PeerAddress peerAddress = new PeerAddress( id, buffer );
        return peerAddress;
    }

    public static PublicKey decodePublicKey( ChannelBuffer buffer, byte[] receivedRawPublicKey )
        throws InvalidKeyException, NoSuchAlgorithmException, InvalidKeySpecException
    {
        buffer.readBytes( receivedRawPublicKey );
        X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec( receivedRawPublicKey );
        KeyFactory keyFactory = KeyFactory.getInstance( "DSA" );
        final PublicKey receivedPublicKey = keyFactory.generatePublic( pubKeySpec );
        return receivedPublicKey;
    }

    public static boolean decodeSignature( Signature signature, Message message, ChannelBuffer buffer )
        throws NoSuchAlgorithmException, SignatureException, InvalidKeyException, IOException
    {
        if ( buffer.readableBytes() < 20 + 20 )
            return false;
        Number160 number1 = MessageCodec.readID( buffer );
        Number160 number2 = MessageCodec.readID( buffer );
        SHA1Signature signatureEncode = new SHA1Signature( number1, number2 );
        byte[] signatureReceived = signatureEncode.encode();
        if ( !signature.verify( signatureReceived ) )
        {
            // set public key only if signature is correct
            message.setPublicKey0( null );
        }
        // set data maps
        return true;
    }
}
