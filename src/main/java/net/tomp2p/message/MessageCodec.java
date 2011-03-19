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
import java.util.List;
import java.util.Map;

import net.tomp2p.message.Message.Command;
import net.tomp2p.message.Message.Content;
import net.tomp2p.message.Message.Type;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.storage.Data;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

public class MessageCodec
{
	final public static byte[] EMPTY_BYTE_ARRAY = new byte[] {};
	final public static int MAX_BYTE = 255;
	final public static int HEADER_SIZE = 60;
	final private static ChannelFactory factory = new ChannelFactory();

	/**
	 * The format looks as follows:
	 * 
	 * 32bit p2p version - 32bit id - 4bit message type - 4bit message name -
	 * 160bit sender id - 16bit tcp port - 16bit udp port - 160bit recipient id
	 * - 32bit message length - 16bit (4x4)content type - 8bit network address
	 * information. It total, the header is of size
	 * 60 bytes.
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
		buffer.writeInt(message.getContentLength()); // 57
		final int content = ((message.getContentType4().ordinal() << 12)
				| (message.getContentType3().ordinal() << 8)
				| (message.getContentType2().ordinal() << 4) | message.getContentType1().ordinal());
		buffer.writeShort((short) content); // 59
		// options
		buffer.writeByte(message.getSender().createType()); // 60
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
	public static void encodePayload(final Message message, List<ChannelBuffer> payloadBuffers)
			throws InvalidKeyException, SignatureException, NoSuchAlgorithmException
	{
		int contentLength = 0;
		if (message.getContentType1() != Message.Content.EMPTY)
		{
			contentLength += encodePayloadType(message.getContentType1(), payloadBuffers, message);
			if (message.getContentType2() != Message.Content.EMPTY)
			{
				contentLength += encodePayloadType(message.getContentType2(), payloadBuffers,
						message);
				if (message.getContentType3() != Message.Content.EMPTY)
				{
					contentLength += encodePayloadType(message.getContentType3(), payloadBuffers,
							message);
					if (message.getContentType4() != Message.Content.EMPTY)
					{
						contentLength += encodePayloadType(message.getContentType4(),
								payloadBuffers, message);
					}
				}
			}
		}
		message.setContentLength(contentLength);
	}

	/**
	 * Encodes payload in a big switch statement. Types are: EMPTY, KEY_KEY,
	 * PUBLIC_KEY, KEY_KEY_PUBLIC_KEY, MAP_KEY_DATA, MAP_KEY_DATA_TTL,
	 * SET_DATA_TTL, MAP_KEY_KEY, SET_KEYS, SET_NEIGHBORS, BYTE_ARRAY, INTEGER,
	 * USER1, USER2, USER3
	 * 
	 * @param contentType The type of the content to encode
	 * @param buffer The Netty buffer to fill
	 * @param message The message which contains the payload
	 * @throws NoSuchAlgorithmException
	 * @throws SignatureException
	 * @throws InvalidKeyException
	 */
	private static int encodePayloadType(final Content content, final List<ChannelBuffer> buffers,
			final Message message)
	{
		if (message.isHintSign())
			throw new IllegalArgumentException(
					"can set signing only at the end, but you called after signig " + content);
		final int size;
		final ChannelBuffer buffer;
		int count;
		byte[] data;
		switch (content)
		{
			case KEY:
				buffers.add(ChannelBuffers.wrappedBuffer(message.getKey3().toByteArray()));
				return 20;
			case KEY_KEY:
				buffers.add(ChannelBuffers.wrappedBuffer(message.getKey1().toByteArray()));
				buffers.add(ChannelBuffers.wrappedBuffer(message.getKey2().toByteArray()));
				return 40;
			case MAP_KEY_DATA:
				count = 4;
				buffer = ChannelBuffers.buffer(4);
				if (message.isConvertNumber480to160())
				{
					Map<Number480, Data> dataMap = message.getDataMapConvert();
					buffer.writeInt(dataMap.size());
					buffers.add(buffer);
					for (Map.Entry<Number480, Data> entry : dataMap.entrySet())
					{
						buffers.add(ChannelBuffers.wrappedBuffer(entry.getKey().getContentKey()
								.toByteArray()));
						count += 20;
						Collection<DataOutput> tmp2 = new ArrayList<DataOutput>(4);
						count += encodeData(tmp2, factory, message, entry.getValue());
						for (DataOutput output : tmp2)
							buffers.add(((ChannelEncoder) output).getChannelBuffer());
					}
					return count;
				}
				else
				{
					buffer.writeInt(message.getDataMap().size());
					buffers.add(buffer);
					for (Map.Entry<Number160, Data> entry : message.getDataMap().entrySet())
					{
						buffers.add(ChannelBuffers.wrappedBuffer(entry.getKey().toByteArray()));
						count += 20;
						// count += encodeData(buffers, message,
						// entry.getValue());
						Collection<DataOutput> tmp2 = new ArrayList<DataOutput>(4);
						count += encodeData(tmp2, factory, message, entry.getValue());
						for (DataOutput output : tmp2)
							buffers.add(((ChannelEncoder) output).getChannelBuffer());
					}
					return count;
				}
			case MAP_KEY_KEY:
				Map<Number160, Number160> keyMap = message.getKeyMap();
				size = keyMap.size();
				buffer = ChannelBuffers.buffer(4);
				buffer.writeInt(size);
				buffers.add(buffer);
				for (final Map.Entry<Number160, Number160> entry : keyMap.entrySet())
				{
					buffers.add(ChannelBuffers.wrappedBuffer(entry.getKey().toByteArray()));
					buffers.add(ChannelBuffers.wrappedBuffer(entry.getValue().toByteArray()));
				}
				return 4 + (size * (20 + 20));
			case SET_KEYS:
				if (message.isConvertNumber480to160())
				{
					Collection<Number480> keys = message.getKeysConvert();
					size = keys.size();
					buffer = ChannelBuffers.buffer(4);
					buffer.writeInt(size);
					buffers.add(buffer);
					for (Number480 key : keys)
						buffers.add(ChannelBuffers.wrappedBuffer(key.getContentKey().toByteArray()));
					return 4 + (size * 20);
				}
				else
				{
					size = message.getKeys().size();
					buffer = ChannelBuffers.buffer(4);
					buffer.writeInt(size);
					buffers.add(buffer);
					for (Number160 key : message.getKeys())
						buffers.add(ChannelBuffers.wrappedBuffer(key.toByteArray()));
					return 4 + (size * 20);
				}
			case SET_NEIGHBORS:
				count = 1;
				size = Math.min(message.getNeighbors().size(),
						Math.min(message.getUseAtMostNeighbors(), MAX_BYTE));
				buffer = ChannelBuffers.buffer(1);
				buffer.writeByte(size);
				buffers.add(buffer);
				final Iterator<PeerAddress> iterator = message.getNeighbors().iterator();
				for (int i = 0; iterator.hasNext() && i < size; i++)
				{
					ChannelBuffer buffer2 = writePeerAddress(iterator.next());
					buffers.add(buffer2);
					count += buffer2.capacity();
				}
				return count;
			case CHANNEL_BUFFER:
				final ChannelBuffer tmpBuffer = message.getPayload();
				size = tmpBuffer.writerIndex();
				buffer = ChannelBuffers.buffer(4);
				buffer.writeInt(size);
				buffers.add(buffer);
				buffers.add(tmpBuffer.slice());
				return 4 + size;
			case LONG:
				buffer = ChannelBuffers.buffer(8);
				buffer.writeLong(message.getLong());
				buffers.add(buffer);
				return 8;
			case INTEGER:
				buffer = ChannelBuffers.buffer(4);
				buffer.writeInt(message.getInteger());
				buffers.add(buffer);
				return 4;
			case PUBLIC_KEY:
				data = message.getPublicKey().getEncoded();
				size = data.length;
				buffer = ChannelBuffers.buffer(2 + size);
				buffer.writeShort(size);
				buffer.writeBytes(data);
				buffers.add(buffer);
				return 2 + size;
			case PUBLIC_KEY_SIGNATURE:
				// flag to encode public key
				data = message.getPublicKey().getEncoded();
				size = data.length;
				buffer = ChannelBuffers.buffer(2 + size);
				buffer.writeShort(size);
				buffer.writeBytes(data);
				buffers.add(buffer);
				message.setHintSign(true);
				// count 40 for the signature, which comes later
				return 40 + 2 + size;
			case EMPTY:
			case RESERVED1:
			case RESERVED2:
			case RESERVED3:
			default:
				return 0;
		}
	}

	public static void encodeSecurity(Message message, List<ChannelBuffer> buffers)
			throws NoSuchAlgorithmException, InvalidKeyException, SignatureException, IOException
	{
		if (!message.isHintSign())
			return;
		Signature signature = Signature.getInstance("SHA1withDSA");
		signature.initSign(message.getPrivateKey());
		//System.err.println("private key for encoding is " + message.getPrivateKey());
		//System.err.println("public key for encoding is " + message.getPublicKey());
		for (ChannelBuffer buffer2 : buffers)
		{
			signature.update(buffer2.array(), buffer2.arrayOffset(), buffer2.writerIndex());
			//System.err.println("\nI do the update starting from encode " + buffer2.arrayOffset()
			//		+ " to " + buffer2.writerIndex());
			//for (int i = buffer2.arrayOffset(); i < buffer2.writerIndex(); i++)
			//{
			//	System.err.format("%1$02x,", buffer2.array()[i]);
			//	if (i % 1000 == 0 && i != 0)
			//		System.err.print("\n");
			//}
		}
		byte[] signatureData = signature.sign();
		SHA1Signature decodedSignature = new SHA1Signature();
		decodedSignature.decode(signatureData);
		// System.err.println("nn1 "+decodedSignature.getNumber1());
		// System.err.println("nn2 "+decodedSignature.getNumber2());
		buffers.add(ChannelBuffers.wrappedBuffer(decodedSignature.getNumber1().toByteArray()));
		buffers.add(ChannelBuffers.wrappedBuffer(decodedSignature.getNumber2().toByteArray()));
	}

	public static int encodeData(Collection<DataOutput> result, DataOutputFactory factory,
			final Message message, Data data)
	{
		int dataHeaderLength = 4 + 4 + 2 + 1;
		int count = dataHeaderLength;
		DataOutput output1 = factory.create(dataHeaderLength);
		// encode entry protection in millis as the sign bit. Thus the max value
		// of millis is 2^31, which is more than enough
		int seconds = data.getTTLSeconds();
		seconds = data.isProtectedEntry() ? seconds | 0x80000000 : seconds & 0x7FFFFFFF;
		output1.writeInt(seconds);
		output1.writeInt(data.getLength());
		result.add(output1);
		// here we could do the second array
		byte[] serializedPublicKey = null;
		// public key
		PublicKey publicKey = data.getDataPublicKey();
		if (publicKey == null)
			output1.writeShort(0);
		//
		else if (message != null && data.getDataPublicKey().equals(message.getPublicKey()))
			output1.writeShort(-1);
		else
		{
			serializedPublicKey = publicKey.getEncoded();
			int publicKeyLength = serializedPublicKey.length;
			output1.writeShort(publicKeyLength);
			count += publicKeyLength;
			// here we do the fourth array
		}
		// signature
		byte[] signature = data.getSignature();
		if (signature == null || signature.length == 0)
			output1.writeByte(0);
		else
		{
			int signatureLength = signature.length;
			output1.writeByte(signatureLength);
			count += signatureLength;
			// here we do the fifth array
		}
		// second array
		if (data.getPeerAddress() == null)
		{
			// writing a zero indicates that there is no peeraddress. This is
			// ok, since there cannot be a peeraddress that starts with a zeros
			DataOutput outputPeerAddress = factory.create(new byte[1]);
			result.add(outputPeerAddress);
			count++;
		}
		else
		{
			byte[] peerAddress = data.getPeerAddress().toByteArray();
			DataOutput outputPeerAddress = factory.create(peerAddress);
			result.add(outputPeerAddress);
			count += peerAddress.length;
		}
		// third array
		DataOutput output2 = factory.create(data.getData(), data.getOffset(), data.getLength());
		result.add(output2);
		count += data.getLength();
		// fourth array
		if (serializedPublicKey != null)
		{
			DataOutput output3 = factory.create(serializedPublicKey);
			result.add(output3);
			// do not add anything to the counter here, since we did it in the
			// data header
		}
		// fifth array
		if (signature != null && signature.length > 0)
		{
			DataOutput output4 = factory.create(signature);
			result.add(output4);
			// do not add anything to the counter here, since we did it in the
			// data header
		}
		return count;
	}

	/**
	 * Decode a message header from a Netty buffer
	 * 
	 * @param buffer The buffer to decode from
	 * @param sender The sender of the packet, which has been set in the socket
	 *        class
	 * @return The partial message, only the header fields are filled
	 */
	public static Message decodeHeader(final ChannelBuffer buffer, InetAddress sender)
			throws DecoderException
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
		message.setContentLength(buffer.readInt());
		final int contentType = buffer.readUnsignedShort();
		message.setContentType(Content.values()[contentType & 0xf],
				Content.values()[(contentType >>> 4) & 0xf],
				Content.values()[(contentType >>> 8) & 0xf], Content.values()[contentType >>> 12]);
		// set the address as we see it, important for port forwarding
		// identification
		message.setRealSender(new PeerAddress(senderID, sender, portTCP, portUDP));
		final byte optionType = buffer.readByte();
		final PeerAddress peerAddress = new PeerAddress(senderID, sender, portTCP, portUDP,
				optionType);
		message.setSender(peerAddress);
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
	 * @throws ASN1Exception
	 * @throws UnsupportedEncodingException If UTF-8 is not there
	 */
	public static void decodePayload(final Content content, final ChannelBuffer buffer,
			final Message message) throws InvalidKeyException, SignatureException,
			NoSuchAlgorithmException, InvalidKeySpecException, IOException
	{
		final int len;
		byte[] me;
		switch (content)
		{
			case KEY:
				message.setKey0(readID(buffer));
				break;
			case KEY_KEY:
				message.setKeyKey0(readID(buffer), readID(buffer));
				break;
			case MAP_KEY_DATA:
				int size = buffer.readInt();
				Map<Number160, Data> result = new HashMap<Number160, Data>(size);
				for (int i = 0; i < size; i++)
				{
					Number160 key = readID(buffer);
					final Data data = decodeData(new ChannelDecoder(buffer), message);
					result.put(key, data);
				}
				message.setDataMap0(result);
				break;
			case MAP_KEY_KEY:
				len = buffer.readInt();
				final Map<Number160, Number160> keyMap = new HashMap<Number160, Number160>();
				for (int i = 0; i < len; i++)
				{
					final Number160 key1 = readID(buffer);
					final Number160 key2 = readID(buffer);
					keyMap.put(key1, key2);
				}
				message.setKeyMap0(keyMap);
				break;
			case SET_KEYS:
				// can be 31bit long ~ 2GB
				len = buffer.readInt();
				final Collection<Number160> tmp = new ArrayList<Number160>(len);
				for (int i = 0; i < len; i++)
				{
					Number160 key = readID(buffer);
					tmp.add(key);
				}
				message.setKeys0(tmp);
				break;
			case SET_NEIGHBORS:
				len = buffer.readUnsignedByte();
				final Collection<PeerAddress> neighbors = new ArrayList<PeerAddress>(len);
				for (int i = 0; i < len; i++)
					neighbors.add(readPeerAddress(buffer));
				message.setNeighbors0(neighbors);
				break;
			case CHANNEL_BUFFER:
				len = buffer.readInt();
				// final ChannelBuffer tmpBuffer =
				// buffer.slice(buffer.readerIndex(), len);
				// you can only use slice if no execution handler is in place,
				// otherwise, you will overwrite stuff
				final ChannelBuffer tmpBuffer = buffer.copy(buffer.readerIndex(), len);
				buffer.skipBytes(len);
				message.setPayload0(tmpBuffer);
				break;
			case LONG:
				message.setLong0(buffer.readLong());
				break;
			case INTEGER:
				message.setInteger0(buffer.readInt());
				break;
			case PUBLIC_KEY:
				len = buffer.readUnsignedShort();
				me = new byte[len];
				message.setPublicKey0(decodePublicKey(new ChannelDecoder(buffer), me));
				break;
			case PUBLIC_KEY_SIGNATURE:
				len = buffer.readUnsignedShort();
				me = new byte[len];
				PublicKey receivedPublicKey = decodePublicKey(new ChannelDecoder(buffer), me);
				// get signature
				final Signature signatureAlgorithm = Signature.getInstance("SHA1withDSA");
				signatureAlgorithm.initVerify(receivedPublicKey);
				signatureAlgorithm.update(buffer.array(), buffer.arrayOffset(),
						buffer.readerIndex());
				//System.err.println("\nI do the update starting from docede " + buffer.arrayOffset()
				//		+ " to " + buffer.readerIndex());
				//System.err.println("public key is"+receivedPublicKey);
				//for (int i = buffer.arrayOffset(); i < buffer.readerIndex(); i++)
				//{
				//	System.err.format("%1$02x,", buffer.array()[i]);
				//	if (i % 1000 == 0 && i != 0)
				//		System.err.print("\n");
				//}
				Number160 number1 = readID(buffer);
				Number160 number2 = readID(buffer);
				// System.err.println("n1 "+number1);
				// System.err.println("n2 "+number2);
				SHA1Signature signatureEncode = new SHA1Signature(number1, number2);
				byte[] signatureReceived = signatureEncode.encode();
				if (signatureAlgorithm.verify(signatureReceived))
					// set public key only if signature is correct
					message.setPublicKey0(receivedPublicKey);
				// set data maps
				if (message.isHintDataPublickKey())
				{
					for (Data data : message.getDataMap().values())
						if (Data.FROM_MESSAGE.equals(data.getDataPublicKey()))
							data.setDataPublicKey(receivedPublicKey);
				}
				break;
			case EMPTY:
			case RESERVED1:
			case RESERVED2:
			case RESERVED3:
			default:
				break;
		}
	}

	public static Data decodeData(final DataInput buffer, Message message)
			throws InvalidKeyException, NoSuchAlgorithmException, InvalidKeySpecException,
			UnknownHostException
	{
		int ttl = buffer.readInt();
		boolean protectedEntry = (ttl & 0x80000000) != 0;
		ttl &= 0x7FFFFFFF;
		int valueSize = buffer.readInt();
		int publicKeyLength = buffer.readUnsignedShort();
		int sigLength = buffer.readUnsignedByte();
		//
		int type = buffer.getUnsignedByte();
		PeerAddress originator = null;
		if (type == 0)
		{
			buffer.skipBytes(1);
			if (message != null)
				message.getSender();
		}
		else
		{
			int len = PeerAddress.expectedLength(type);
			byte[] me = new byte[len];
			buffer.readBytes(me);
			originator = new PeerAddress(me, 0);
		}
		//
		final Data data = createData(buffer.array(), buffer.arrayOffset() + buffer.readerIndex(),
				valueSize, ttl, protectedEntry, originator);
		buffer.skipBytes(valueSize);
		// sig and pubkey
		if (message != null && publicKeyLength == -1)
		{
			message.setHintDataPublickKey(true);
			data.setDataPublicKey(Data.FROM_MESSAGE);
		}
		else if (publicKeyLength > 0)
		{
			byte[] receivedRawPublicKey = new byte[publicKeyLength];
			PublicKey receivedPublicKey = decodePublicKey(buffer, receivedRawPublicKey);
			data.setDataPublicKey(receivedPublicKey);
		}
		if (sigLength > 0)
		{
			byte[] signature = new byte[sigLength];
			buffer.readBytes(signature);
			data.setSignature(signature);
		}
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

	private static ChannelBuffer writePeerAddress(PeerAddress peerAddress)
	{
		int size = peerAddress.isIPv6() ? PeerAddress.SIZE_IPv6 : PeerAddress.SIZE_IPv4;
		ChannelBuffer result = ChannelBuffers.buffer(size);
		result.writeBytes(peerAddress.getID().toByteArray());
		result.writeShort(peerAddress.portTCP());
		result.writeShort(peerAddress.portUDP());
		byte type = peerAddress.createType();
		result.writeByte(type);
		result.writeBytes(peerAddress.getInetAddress().getAddress());
		return result;
	}

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
		final Number160 id = readID(buffer);
		// TODO: check why bytes are sent in reversed order
		final int tcpPort = buffer.readUnsignedShort();
		final int udpPort = buffer.readUnsignedShort();
		final byte type = buffer.readByte();
		byte[] tmp;
		if (!PeerAddress.isNet6(type))
		{
			// IPv4
			tmp = new byte[4];
			buffer.readBytes(tmp);
			return new PeerAddress(id, InetAddress.getByAddress(tmp), tcpPort, udpPort);
		}
		else
		{
			// IPv6
			tmp = new byte[16];
			buffer.readBytes(tmp);
			return new PeerAddress(id, InetAddress.getByAddress(tmp), tcpPort, udpPort);
		}
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
	private static class ChannelFactory implements DataOutputFactory
	{
		@Override
		public DataOutput create(int count)
		{
			return new ChannelEncoder(ChannelBuffers.buffer(count));
		}

		@Override
		public DataOutput create(byte[] data, int offset, int length)
		{
			return new ChannelEncoder(ChannelBuffers.wrappedBuffer(data, offset, length));
		}

		@Override
		public DataOutput create(byte[] data)
		{
			return new ChannelEncoder(ChannelBuffers.wrappedBuffer(data));
		}
	}
	private static class ChannelEncoder implements DataOutput
	{
		private final ChannelBuffer buffer;

		private ChannelEncoder(ChannelBuffer buffer)
		{
			this.buffer = buffer;
		}

		public ChannelBuffer getChannelBuffer()
		{
			return buffer;
		}

		@Override
		public void writeByte(int intVal)
		{
			buffer.writeByte(intVal);
		}

		@Override
		public void writeInt(int intVal)
		{
			buffer.writeInt(intVal);
		}

		@Override
		public void writeShort(int intVal)
		{
			buffer.writeShort(intVal);
		}
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
	}
}
