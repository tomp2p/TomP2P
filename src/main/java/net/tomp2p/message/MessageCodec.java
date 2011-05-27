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
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
import net.tomp2p.storage.Data;
import net.tomp2p.storage.TrackerData;

import org.jboss.netty.buffer.ChannelBuffer;

public class MessageCodec
{
	final public static byte[] EMPTY_BYTE_ARRAY = new byte[] {};
	final public static int MAX_BYTE = 255;
	final public static int HEADER_SIZE = 60;

	/**
	 * The format looks as follows:
	 * 
	 * 32bit p2p version - 32bit id - 4bit message type - 4bit message name -
	 * 160bit sender id - 16bit tcp port - 16bit udp port - 160bit recipient id
	 * - 16bit (4x4)content type - 8bit network address
	 * information - 32bit IPv4 address to override address as seen in case of 
	 * NAT issues. It total, the header is of size 60 bytes.
	 * 
	 * 
	 * @param buffer The Netty buffer to fill
	 * @param message The message with the header that will be serialized
	 * @return The buffer passed as an argument
	 */
	public static ChannelBuffer encodeHeader(final ChannelBuffer buffer, final Message message)
	{
		buffer.writeInt(message.getVersion()); // 4
		buffer.writeInt(message.getMessageId()); // 8
		buffer.writeByte(((message.getType().ordinal() << 4) + message.getCommand().ordinal())); // 9
		buffer.writeBytes(message.getSender().getID().toByteArray()); // 29
		buffer.writeShort((short) message.getSender().portTCP()); // 31
		buffer.writeShort((short) message.getSender().portUDP()); // 33
		buffer.writeBytes(message.getRecipient().getID().toByteArray()); // 53
		final int content = ((message.getContentType4().ordinal() << 12)
				| (message.getContentType3().ordinal() << 8)
				| (message.getContentType2().ordinal() << 4) | message.getContentType1().ordinal());
		buffer.writeShort((short) content); // 55
		// options
		buffer.writeByte(message.getSender().createType()); // 56
		if(message.getSender().isPresetIPv4() && message.getSender().isIPv4()) {
			buffer.writeBytes(message.getSender().getInetAddress().getAddress()); // 60
		}
		else {
			buffer.writeInt(0); // 60
		}
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
	 */
	public static void encodePayload(final Message message, ProtocolChunkedInput input)
			throws InvalidKeyException, SignatureException, NoSuchAlgorithmException
	{
		int contentLength = 0;
		if (message.getContentType1() != Message.Content.EMPTY)
		{
			contentLength += encodePayloadType(message.getContentType1(), input, message);
			if (message.getContentType2() != Message.Content.EMPTY)
			{
				contentLength += encodePayloadType(message.getContentType2(), input,
						message);
				if (message.getContentType3() != Message.Content.EMPTY)
				{
					contentLength += encodePayloadType(message.getContentType3(), input,
							message);
					if (message.getContentType4() != Message.Content.EMPTY)
					{
						contentLength += encodePayloadType(message.getContentType4(),
								input, message);
					}
				}
			}
		}
		if(message.isHintSign()){
			input.addMarkerForSignature();
		}
		else
			input.flush(true);
		message.setLength(contentLength);
	}

	/**
	 * Encodes payload in a big switch statement. Types are: EMPTY, KEY_KEY,
	 * PUBLIC_KEY, KEY_KEY_PUBLIC_KEY, MAP_KEY_DATA, MAP_KEY_DATA_TTL,
	 * SET_DATA_TTL, MAP_KEY_KEY, SET_KEYS, SET_NEIGHBORS, BYTE_ARRAY, INTEGER,
	 * USER1, USER2, USER3
	 * 
	 * @param contentType The type of the content to encode
	 * @param input The ProtocolChunkedInput to fill
	 * @param message The message which contains the payload
	 * @throws NoSuchAlgorithmException
	 * @throws SignatureException
	 * @throws InvalidKeyException
	 */
	private static int encodePayloadType(final Content content, final ProtocolChunkedInput input,
			final Message message)
	{
		final int size;
		final byte[] data;
		int count;
		switch (content)
		{
			case KEY:
				input.copyToCurrent(message.getKey3().toByteArray());
				return 20;
			case KEY_KEY:
				input.copyToCurrent(message.getKey1().toByteArray());
				input.copyToCurrent(message.getKey2().toByteArray());
				return 40;
			case MAP_KEY_DATA:
				count = 4;
				if (message.isConvertNumber480to160())
				{
					Map<Number480, Data> dataMap = message.getDataMapConvert();
					input.copyToCurrent(dataMap.size());
					for (Map.Entry<Number480, Data> entry : dataMap.entrySet())
					{
						input.copyToCurrent(entry.getKey().getContentKey().toByteArray());
						count += 20;
						count += encodeData(input, message, entry.getValue());
					}
					return count;
				}
				else
				{
					input.copyToCurrent(message.getDataMap().size());
					for (Map.Entry<Number160, Data> entry : message.getDataMap().entrySet())
					{
						input.copyToCurrent(entry.getKey().toByteArray());
						count += 20;
						count += encodeData(input, message, entry.getValue());
					}
					return count;
				}
			case MAP_KEY_KEY:
				Map<Number160, Number160> keyMap = message.getKeyMap();
				size = keyMap.size();
				input.copyToCurrent(size);
				for (final Map.Entry<Number160, Number160> entry : keyMap.entrySet())
				{
					input.copyToCurrent(entry.getKey().toByteArray());
					input.copyToCurrent(entry.getValue().toByteArray());
				}
				return 4 + (size * (20 + 20));
			case SET_KEYS:
				if (message.isConvertNumber480to160())
				{
					Collection<Number480> keys = message.getKeysConvert();
					size = keys.size();
					input.copyToCurrent(size);
					for (Number480 key : keys)
						input.copyToCurrent(key.getContentKey().toByteArray());
					return 4 + (size * 20);
				}
				else
				{
					size = message.getKeys().size();
					input.copyToCurrent(size);
					for (Number160 key : message.getKeys())
						input.copyToCurrent(key.toByteArray());
					return 4 + (size * 20);
				}
			case SET_NEIGHBORS:
				count = 1;
				size = Math.min(message.getNeighbors().size(),
						Math.min(message.getUseAtMostNeighbors(), MAX_BYTE));
				input.copyToCurrent((byte)size);
				final Iterator<PeerAddress> iterator = message.getNeighbors().iterator();
				for (int i = 0; iterator.hasNext() && i < size; i++)
				{
					byte[] data1=iterator.next().toByteArray();
					input.copyToCurrent(data1);
					count += data1.length;
				}
				return count;
			case SET_TRACKER_DATA:
				count = 1;
				final Collection<TrackerData> trackerDatas=message.getTrackerData();
				input.copyToCurrent((byte)trackerDatas.size());
				for(TrackerData trackerData:trackerDatas)
				{
					byte[] data1=trackerData.getPeerAddress().toByteArray();
					input.copyToCurrent(data1);
					count += data1.length;
					if(trackerData.getAttachement()!=null)
					{
						input.copyToCurrent((byte)1);
						input.copyToCurrent(trackerData.getLength());
						count += 5;
						input.copyToCurrent(trackerData.getAttachement(), trackerData.getOffset(), trackerData.getLength());
						count += trackerData.getLength();
					}
					else
					{
						input.copyToCurrent((byte)0);
						count += 1;
					}
				}
				return count;
			case CHANNEL_BUFFER:
				ChannelBuffer tmpBuffer= message.getPayload1();
				if(tmpBuffer==null)
					tmpBuffer= message.getPayload2();
				else
					message.setPayload1(null);
				size = tmpBuffer.writerIndex();
				input.copyToCurrent(size);
				input.copyToCurrent(tmpBuffer.slice());
				return 4 + size;
			case LONG:
				input.copyToCurrent(message.getLong());
				return 8;
			case INTEGER:
				input.copyToCurrent(message.getInteger());
				return 4;
			case PUBLIC_KEY:
				data = message.getPublicKey().getEncoded();
				size = data.length;
				input.copyToCurrent((short)size);
				input.copyToCurrent(data);
				return 2 + size;
			case PUBLIC_KEY_SIGNATURE:
				// flag to encode public key
				data = message.getPublicKey().getEncoded();
				size = data.length;
				input.copyToCurrent((short)size);
				input.copyToCurrent(data);
				message.setHintSign(true);
				// count 40 for the signature, which comes later in ProtocolChunkedInput
				return 40 + 2 + size;
			case EMPTY:
			case RESERVED1:
			case RESERVED2:
			case RESERVED3:
			default:
				return 0;
		}
	}

	public static int encodeData(ProtocolChunkedInput input, final Message message, Data data)
	{
		int count = 4 + 4;
		// encode entry protection in millis as the sign bit. Thus the max value
		// of millis is 2^31, which is more than enough
		int seconds = data.getTTLSeconds();
		seconds = data.isProtectedEntry() ? seconds | 0x80000000 : seconds & 0x7FFFFFFF;
		input.copyToCurrent(seconds);
		input.copyToCurrent(data.getLength());
		// the real data
		input.copyToCurrent(data.getData(), data.getOffset(), data.getLength());
		count += data.getLength();
		return count;
	}

	/**
	 * Decode a message header from a Netty buffer
	 * 
	 * @param buffer The buffer to decode from
	 * @param sender The sender of the packet, which has been set in the socket
	 *        class
	 * @return The partial message, only the header fields are filled
	 * @throws UnknownHostException 
	 */
	public static Message decodeHeader(final ChannelBuffer buffer, final InetAddress sender)
			throws DecoderException, UnknownHostException
	{
		final Message message = new Message();
		message.setVersion(buffer.readInt());
		message.setMessageId(buffer.readInt());
		//
		final int commandType = buffer.readUnsignedByte();
		message.setCommand(Command.values()[commandType & 0xf]);
		message.setType(Type.values()[commandType >>> 4]);
		final Number160 senderID = readID(buffer);
		final int portTCP = buffer.readUnsignedShort();
		final int portUDP = buffer.readUnsignedShort();
		final Number160 recipientID = readID(buffer);
		message.setRecipient(new PeerAddress(recipientID));
		final int contentType = buffer.readUnsignedShort();
		message.setContentType(Content.values()[contentType & 0xf],
				Content.values()[(contentType >>> 4) & 0xf],
				Content.values()[(contentType >>> 8) & 0xf], Content.values()[contentType >>> 12]);
		// set the address as we see it, important for port forwarding
		// identification
		message.setRealSender(new PeerAddress(senderID, sender, portTCP, portUDP));
		final byte optionType = buffer.readByte();
		if(PeerAddress.isPresetIPv4(optionType)) {
			byte[] me=new byte[4];
			buffer.readBytes(me);
			InetAddress senderAsReported=Inet4Address.getByAddress(me);
			final PeerAddress peerAddress = new PeerAddress(senderID, senderAsReported, portTCP, portUDP,
					optionType);
			message.setSender(peerAddress);
		}
		else {
			buffer.skipBytes(4);
			final PeerAddress peerAddress = new PeerAddress(senderID, sender, portTCP, portUDP,
					optionType);
			message.setSender(peerAddress);
		}
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
	public static boolean decodePayload(final Content content, final ChannelBuffer buffer,
			final Message message) throws InvalidKeyException, SignatureException,
			NoSuchAlgorithmException, InvalidKeySpecException, IOException, DecoderException
	{
		final int len;
		byte[] me;
		switch (content)
		{
			case KEY:
				if(buffer.readableBytes() < 20) return false;
				message.setKey0(readID(buffer));
				return true;
			case KEY_KEY:
				if(buffer.readableBytes() < 40) return false;
				message.setKeyKey0(readID(buffer), readID(buffer));
				return true;
			case MAP_KEY_DATA:
				if(buffer.readableBytes() < 4) return false;
				int size = buffer.readInt();
				Map<Number160, Data> result = new HashMap<Number160, Data>(size);
				for (int i = 0; i < size; i++)
				{
					if(buffer.readableBytes() < 20) return false;
					Number160 key = readID(buffer);
					final Data data = decodeData(new ChannelDecoder(buffer), message);
					if(data == null) return false;
					if(data.isProtectedEntry() && message.getPublicKey()==null)
						throw new DecoderException("You indicated that you want to protect the data, but you did not provide or provided too late a public key.");
					data.setPublicKey(message.getPublicKey());
					result.put(key, data);
				}
				message.setDataMap0(result);
				return true;
			case MAP_KEY_KEY:
				if(buffer.readableBytes() < 4) return false;
				len = buffer.readInt();
				if(buffer.readableBytes() < ((20+20)*len)) return false;
				final Map<Number160, Number160> keyMap = new HashMap<Number160, Number160>();
				for (int i = 0; i < len; i++)
				{
					final Number160 key1 = readID(buffer);
					final Number160 key2 = readID(buffer);
					keyMap.put(key1, key2);
				}
				message.setKeyMap0(keyMap);
				return true;
			case SET_KEYS:
				// can be 31bit long ~ 2GB
				if(buffer.readableBytes() < 4) return false;
				len = buffer.readInt();
				if(buffer.readableBytes() < (20*len)) return false;
				final Collection<Number160> tmp = new ArrayList<Number160>(len);
				for (int i = 0; i < len; i++)
				{
					Number160 key = readID(buffer);
					tmp.add(key);
				}
				message.setKeys0(tmp);
				return true;
			case SET_NEIGHBORS:
				if(buffer.readableBytes() < 1) return false;
				len = buffer.readUnsignedByte();
				if(buffer.readableBytes() < (len * PeerAddress.SIZE_IPv4)) return false;
				final Collection<PeerAddress> neighbors = new ArrayList<PeerAddress>(len);
				for (int i = 0; i < len; i++) {
					PeerAddress peerAddress=readPeerAddress(buffer);
					if(peerAddress == null) return false;
					neighbors.add(peerAddress);
				}
				message.setNeighbors0(neighbors);
				return true;
			case SET_TRACKER_DATA:
				if(buffer.readableBytes() < 1) return false;
				len = buffer.readUnsignedByte();
				if(buffer.readableBytes() < (len * (PeerAddress.SIZE_IPv4 + 1))) return false;
				final Collection<TrackerData> trackerDatas = new ArrayList<TrackerData>(len);
				for (int i = 0; i < len; i++)
				{
					PeerAddress peerAddress = readPeerAddress(buffer);
					if(peerAddress == null) return false;
					byte[] attachment = null;
					int offset = 0;
					int length = 0;
					if(buffer.readableBytes() < 1) return false;
					byte miniHeader = buffer.readByte();
					if(miniHeader!=0)
					{
						if(buffer.readableBytes() < 4) return false;
						length=buffer.readInt();
						if(buffer.readableBytes() < length) return false;
						attachment = new byte[length];
						buffer.readBytes(attachment);
					}
					trackerDatas.add(new TrackerData(peerAddress, message.getSender(), attachment, offset, length));
				}
				message.setTrackerData0(trackerDatas);
				return true;
			case CHANNEL_BUFFER:
				if(buffer.readableBytes() < 4) return false;
				len = buffer.readInt();
				if(buffer.readableBytes() < len) return false;
				// final ChannelBuffer tmpBuffer =
				// buffer.slice(buffer.readerIndex(), len);
				// you can only use slice if no execution handler is in place,
				// otherwise, you will overwrite stuff
				final ChannelBuffer tmpBuffer = buffer.copy(buffer.readerIndex(), len);
				buffer.skipBytes(len);
				message.setPayload0(tmpBuffer);
				return true;
			case LONG:
				if(buffer.readableBytes() < 8) return false;
				message.setLong0(buffer.readLong());
				return true;
			case INTEGER:
				if(buffer.readableBytes() < 4) return false;
				message.setInteger0(buffer.readInt());
				return true;
			case PUBLIC_KEY:
			case PUBLIC_KEY_SIGNATURE:
				if(buffer.readableBytes() < 2) return false;
				len = buffer.readUnsignedShort();
				me = new byte[len];
				if(buffer.readableBytes() < len) return false;
				message.setPublicKey0(decodePublicKey(new ChannelDecoder(buffer), me));
				if(content == Content.PUBLIC_KEY_SIGNATURE) {
					message.setHintSign(true);
				}
				return true;
			case EMPTY:
			case RESERVED1:
			case RESERVED2:
			case RESERVED3:
			default:
				return true;
		}
	}

	public static Data decodeData(final DataInput buffer, Message message)
			throws InvalidKeyException, NoSuchAlgorithmException, InvalidKeySpecException,
			UnknownHostException
	{
		//mini header for data, 8 bytes ttl and data length
		if (buffer.readableBytes() < 4 + 4) return null;
		int ttl = buffer.readInt();
		boolean protectedEntry = (ttl & 0x80000000) != 0;
		ttl &= 0x7FFFFFFF;
		int dateLength = buffer.readInt();
		//
		if (buffer.readableBytes() < dateLength) return null;
		final Data data = createData(buffer.array(), buffer.arrayOffset() + buffer.readerIndex(),
				dateLength, ttl, protectedEntry, message.getSender());
		buffer.skipBytes(dateLength);
		return data;
	}

	public static Data createData(final byte[] me, final int offset, final int length,
			final int ttl, boolean protectedEntry, PeerAddress originator)
	{
		Data data;
		// length may be 0 if data is only used for expiration
		if (length == 0)
			data = new Data(EMPTY_BYTE_ARRAY, originator);
		else
		{
			// check if its worth coping the buffer, or just take the one backed
			// by the bytebuffer. If the backing buffer is too big, then its a
			// waste of space and we should copy, otherwise, tatke the backing
			// array.
			// TODO: find good values for this. This is just a guess
			final boolean copy = true;
			// final boolean copy = me.length / length > 1;
			// we have to use copy if we use an exectution handler, otherwise
			// the buffer will have different data.
			if (copy)
			{
				final byte[] me2 = new byte[length];
				System.arraycopy(me, offset, me2, 0, length);
				data = new Data(me2, 0, length, originator);
			}
			else
				data = new Data(me, offset, length, originator);
		}
		data.setTTLSeconds(ttl);
		data.setProtectedEntry(protectedEntry);
		return data;
	}

	/**
	 * Read a 160bit number from a Netty buffer. I did not want to include
	 * ChannelBuffer in the class Number160.
	 * 
	 * @param buffer The Netty buffer
	 * @return A 160bit number from the Netty buffer (deserialized)
	 */
	private static Number160 readID(final ChannelBuffer buffer)
	{
		byte[] me = new byte[Number160.BYTE_ARRAY_SIZE];
		buffer.readBytes(me);
		return new Number160(me);
	}

	/*private static ChannelBuffer writePeerAddress(PeerAddress peerAddress)
	{
		return ChannelBuffers.wrappedBuffer(peerAddress.toByteArray());
	}*/

	/**
	 * Read a PeerAddress from a Netty buffer. I did not want to include
	 * ChannelBuffer in the class PeerAddress
	 * 
	 * @param buffer The Netty buffer
	 * @return A PeerAddress created from the buffer (deserialized)
	 * @throws UnknownHostException if the address is not valid
	 */
	private static PeerAddress readPeerAddress(final ChannelBuffer buffer)
			throws UnknownHostException
	{
		if(buffer.readableBytes() < 21) return null;
		Number160 id = readID(buffer);
		//peek
		int type = buffer.getUnsignedByte(buffer.readerIndex());
		//now we know the length
		int len=PeerAddress.expectedSocketLength(type);
		if(buffer.readableBytes() < len ) return null;
		PeerAddress peerAddress = new PeerAddress(id, buffer.array(), buffer.arrayOffset() + buffer.readerIndex());
		buffer.skipBytes(len);
		return peerAddress;
	}

	public static PublicKey decodePublicKey(DataInput buffer, byte[] receivedRawPublicKey)
			throws InvalidKeyException, NoSuchAlgorithmException, InvalidKeySpecException
	{
		buffer.readBytes(receivedRawPublicKey);
		X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(receivedRawPublicKey);
		KeyFactory keyFactory = KeyFactory.getInstance("DSA");
		final PublicKey receivedPublicKey = keyFactory.generatePublic(pubKeySpec);
		return receivedPublicKey;
	}
	
	public static boolean decodeSignature(Signature signature, Message message, ChannelBuffer buffer) throws NoSuchAlgorithmException, SignatureException, InvalidKeyException, IOException
	{
		if(buffer.readableBytes() < 20 + 20) return false;
		Number160 number1 = MessageCodec.readID(buffer);
		Number160 number2 = MessageCodec.readID(buffer);
		SHA1Signature signatureEncode = new SHA1Signature(number1, number2);
		byte[] signatureReceived = signatureEncode.encode();
		if (!signature.verify(signatureReceived))
			// set public key only if signature is correct
			message.setPublicKey0(null);
		// set data maps
		return true;
	}
	
	private static class ChannelDecoder implements DataInput
	{
		final private ChannelBuffer buffer;

		private ChannelDecoder(ChannelBuffer buffer)
		{
			this.buffer = buffer;
		}

		@Override
		public byte[] array()
		{
			return buffer.array();
		}

		@Override
		public int arrayOffset()
		{
			return buffer.arrayOffset();
		}

		@Override
		public void readBytes(byte[] buf)
		{
			buffer.readBytes(buf);
		}

		@Override
		public int readInt()
		{
			return buffer.readInt();
		}

		@Override
		public int readUnsignedByte()
		{
			return buffer.readUnsignedByte();
		}

		@Override
		public int getUnsignedByte()
		{
			return buffer.getUnsignedByte(buffer.readerIndex());
		}

		@Override
		public int readUnsignedShort()
		{
			return buffer.readUnsignedShort();
		}

		@Override
		public int readerIndex()
		{
			return buffer.readerIndex();
		}

		@Override
		public void skipBytes(int size)
		{
			buffer.skipBytes(size);
		}

		@Override
		public int readableBytes()
		{
			return buffer.readableBytes();
		}
	}
}
