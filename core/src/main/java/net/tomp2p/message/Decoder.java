package net.tomp2p.message;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramChannel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;

import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.connection.TimeoutFactory;
import net.tomp2p.message.Message.Content;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;
import net.tomp2p.storage.DataBuffer;
import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Decoder {

	public static final AttributeKey<InetSocketAddress> INET_ADDRESS_KEY = AttributeKey.valueOf("inet-addr");
	public static final AttributeKey<PeerAddress> PEER_ADDRESS_KEY = AttributeKey.valueOf("peer-addr");

	private static final Logger LOG = LoggerFactory.getLogger(Decoder.class);

	private final Queue<Content> contentTypes = new LinkedList<Message.Content>();

	// private Message2 result = null;

	// current state - needs to be deleted if we want to reuse
	private Message message = null;
	private boolean headerDone = false;
	private Signature signature = null;

	private int neighborSize = -1;
	private NeighborSet neighborSet = null;
	
	private int peerSocketAddressSize = -1;
	private List<PeerSocketAddress> peerSocketAddresses = null;

	private int keyCollectionSize = -1;
	private KeyCollection keyCollection = null;

	private int mapSize = -1;
	private DataMap dataMap = null;
	private Data data = null;
	private Number640 key = null;

	private int keyMap640KeysSize = -1;
	private KeyMap640Keys keyMap640Keys = null;

	private int keyMapByteSize = -1;
	private KeyMapByte keyMapByte = null;

	private int bufferSize = -1;
	private int bufferTransferred = 0;
	private DataBuffer buffer = null;

	private int trackerDataSize = -1;
	private TrackerData trackerData = null;
	private Data currentTrackerData = null;

	private Content lastContent = null;

	private final SignatureFactory signatureFactory;

	public Decoder(SignatureFactory signatureFactory) {
		this.signatureFactory = signatureFactory;
	}

	public boolean decode(ChannelHandlerContext ctx, final ByteBuf buf, InetSocketAddress recipient,
			final InetSocketAddress sender) {

		LOG.debug("Decoding of TomP2P starts now. Readable: {}.", buf.readableBytes());

		try {
			final int readerBefore = buf.readerIndex();
			// set the sender of this message for handling timeout
			final Attribute<InetSocketAddress> attributeInet = ctx.attr(INET_ADDRESS_KEY);
			attributeInet.set(sender);

			if (message == null && !headerDone) {
				headerDone = decodeHeader(buf, recipient, sender);
				if (headerDone) {
					// store the sender as an attribute
					final Attribute<PeerAddress> attributePeerAddress = ctx.attr(PEER_ADDRESS_KEY);
					attributePeerAddress.set(message.sender());
					message.udp(ctx.channel() instanceof DatagramChannel);
					if (message.isFireAndForget() && message.isUdp()) {
						TimeoutFactory.removeTimeout(ctx);
					}
				} else {
					return false;
				}
			}
			
			final boolean donePayload = decodePayload(buf);
			decodeSignature(buf, readerBefore, donePayload);
			
			if(donePayload) {
				boolean isRelay = message.sender().isRelayed();
				if(isRelay && !message.peerSocketAddresses().isEmpty()) {
					PeerAddress tmpSender = message.sender().changePeerSocketAddresses(message.peerSocketAddresses());
					message.sender(tmpSender);
	        	}
			}
			
			// see https://github.com/netty/netty/issues/1976
			buf.discardSomeReadBytes();
			return donePayload;

		} catch (Exception e) {
			ctx.fireExceptionCaught(e);
			e.printStackTrace();
			return true;
		}
	}
	
	public void decodeSignature(final ByteBuf buf, final int readerBefore, final boolean donePayload) throws InvalidKeyException, SignatureException {
		final int readerAfter = buf.readerIndex();
		final int len = readerAfter - readerBefore;
		if(len > 0) {
			verifySignature(buf, readerBefore, len, donePayload);
		}
	}

	private void verifySignature(final ByteBuf buf, final int readerBefore, final int len, final boolean donePayload)
	        throws SignatureException, InvalidKeyException {

		if (!message.isSign()) {
			return;
		}
		// if we read the complete data, we also read the signature
		// for the verification, we should not use this for the signature
		final int length = donePayload ? len - signatureFactory.signatureSize() : len; 
		ByteBuffer[] byteBuffers = buf.nioBuffers(readerBefore, length);
		if(signature == null) {
			signature = signatureFactory.update(message.publicKey(0), byteBuffers);
		} else {
			for (int i = 0; i < byteBuffers.length; i++) {
				signature.update(byteBuffers[i]);
			}
		}

		if (donePayload) {
			byte[] signatureReceived = message.receivedSignature().encode();
			LOG.debug("Verifying received signature: {}", Arrays.toString(signatureReceived));
			if (signature.verify(signatureReceived)) {
				// set public key only if signature is correct
				message.setVerified();
				LOG.debug("Signature check OK.");
			} else {
				LOG.warn("Signature check NOT OK. Message: {}.", message);
			}
		}
	}

	public boolean decodeHeader(final ByteBuf buf, InetSocketAddress recipient, final InetSocketAddress sender) {
		if (message == null) {
			if (buf.readableBytes() < MessageHeaderCodec.HEADER_SIZE) {
				// we don't have the header yet, we need the full header first
				// wait for more data
				return false;
			}
			message = MessageHeaderCodec.decodeHeader(buf, recipient, sender);
		}
		
		if (message.sender().isNet4Private() && buf.readableBytes() < MessageHeaderCodec.HEADER_PRIVATE_ADDRESS_SIZE) {
			return false;
		} else if (message.sender().isNet4Private() && buf.readableBytes() >= MessageHeaderCodec.HEADER_PRIVATE_ADDRESS_SIZE){
			PeerSocketAddress internalPeerSocketAddress = PeerSocketAddress.create(buf, true);
			message.sender(message.sender().changeInternalPeerSocketAddress(internalPeerSocketAddress));
		}
		// we have set the content types already
		message.presetContentTypes(true);
			for (Content content : message.contentTypes()) {
			if (content == Content.EMPTY) {
				break;
			}
			if (content == Content.PUBLIC_KEY_SIGNATURE) {
				message.setHintSign();
			}
			contentTypes.offer(content);
		}
		LOG.debug("Parsed message {}.", message);
		return true;
	}

	public boolean decodePayload(final ByteBuf buf) throws NoSuchAlgorithmException, InvalidKeySpecException,
			InvalidKeyException {
		LOG.debug("About to pass message {} to {}. Buffer to read: {}.", message, message.senderSocket(), buf.readableBytes());
		if (!message.hasContent()) {
			return true;
		}

		// payload comes here
		int size;
		PublicKey receivedPublicKey;
		while (contentTypes.size() > 0) {
			Content content = contentTypes.peek();
			LOG.debug("Parse content: {} in message {}", content, message);
			switch (content) {
			case INTEGER:
				if (buf.readableBytes() < Utils.INTEGER_BYTE_SIZE) {
					return false;
				}
				message.intValue(buf.readInt());
				lastContent = contentTypes.poll();
				break;
			case LONG:
				if (buf.readableBytes() < Utils.LONG_BYTE_SIZE) {
					return false;
				}
				message.longValue(buf.readLong());
				lastContent = contentTypes.poll();
				break;
			case KEY:
				if (buf.readableBytes() < Number160.BYTE_ARRAY_SIZE) {
					return false;
				}
				byte[] me = new byte[Number160.BYTE_ARRAY_SIZE];
				buf.readBytes(me);
				message.key(new Number160(me));
				lastContent = contentTypes.poll();
				break;
			case BLOOM_FILTER:
				if (buf.readableBytes() < Utils.SHORT_BYTE_SIZE) {
					return false;
				}
				size = buf.getUnsignedShort(buf.readerIndex());
				if (buf.readableBytes() < size) {
					return false;
				}
				message.bloomFilter(new SimpleBloomFilter<Number160>(buf));
				lastContent = contentTypes.poll();
				break;
			case SET_NEIGHBORS:
				if (neighborSize == -1 && buf.readableBytes() < Utils.BYTE_BYTE_SIZE) {
					return false;
				}
				if (neighborSize == -1) {
					neighborSize = buf.readUnsignedByte();
				}
				if (neighborSet == null) {
					neighborSet = new NeighborSet(-1, new ArrayList<PeerAddress>(neighborSize));
				}
				for (int i = neighborSet.size(); i < neighborSize; i++) {
					if (buf.readableBytes() < Utils.SHORT_BYTE_SIZE) {
						return false;
					}
					int header = buf.getUnsignedShort(buf.readerIndex());
					size = PeerAddress.size(header);
					if (buf.readableBytes() < size) {
						return false;
					}
					PeerAddress pa = new PeerAddress(buf);
					neighborSet.add(pa);
				}
				message.neighborsSet(neighborSet);
				lastContent = contentTypes.poll();
				neighborSize = -1;
				neighborSet = null;
				break;
			case SET_PEER_SOCKET:
				if (peerSocketAddressSize == -1 && buf.readableBytes() < Utils.BYTE_BYTE_SIZE) {
					return false;
				}
				if (peerSocketAddressSize == -1) {
					peerSocketAddressSize = buf.readUnsignedByte();
				}
				if (peerSocketAddresses == null) {
					peerSocketAddresses = new ArrayList<PeerSocketAddress>(peerSocketAddressSize);
				}
				for (int i = peerSocketAddresses.size(); i < peerSocketAddressSize; i++) {
					if (buf.readableBytes() < Utils.BYTE_BYTE_SIZE) {
						return false;
					}
					int header = buf.getUnsignedByte(buf.readerIndex());
					boolean isIPv4 = header == 0;
					size = PeerSocketAddress.size(isIPv4);
					if (buf.readableBytes() < size + Utils.BYTE_BYTE_SIZE) {
						return false;
					}
					//skip the ipv4/ipv6 header
					buf.skipBytes(1);
					peerSocketAddresses.add(PeerSocketAddress.create(buf, isIPv4));
				}
				message.peerSocketAddresses(peerSocketAddresses);
				lastContent = contentTypes.poll();
				peerSocketAddressSize = -1;
				peerSocketAddresses = null;
				break;
			case SET_KEY640:
				if (keyCollectionSize == -1 && buf.readableBytes() < Utils.INTEGER_BYTE_SIZE) {
					return false;
				}
				if (keyCollectionSize == -1) {
					keyCollectionSize = buf.readInt();
				}
				if (keyCollection == null) {
					keyCollection = new KeyCollection(new ArrayList<Number640>(keyCollectionSize));
				}
				for (int i = keyCollection.size(); i < keyCollectionSize; i++) {
					if (buf.readableBytes() < Number160.BYTE_ARRAY_SIZE + Number160.BYTE_ARRAY_SIZE
							+ Number160.BYTE_ARRAY_SIZE + Number160.BYTE_ARRAY_SIZE) {
						return false;
					}
					byte[] me2 = new byte[Number160.BYTE_ARRAY_SIZE];
					buf.readBytes(me2);
					Number160 locationKey = new Number160(me2);
					buf.readBytes(me2);
					Number160 domainKey = new Number160(me2);
					buf.readBytes(me2);
					Number160 contentKey = new Number160(me2);
					buf.readBytes(me2);
					Number160 versionKey = new Number160(me2);
					keyCollection.add(new Number640(locationKey, domainKey, contentKey, versionKey));
				}
				message.keyCollection(keyCollection);
				lastContent = contentTypes.poll();
				keyCollectionSize = -1;
				keyCollection = null;
				break;
			case MAP_KEY640_DATA:
				if (mapSize == -1 && buf.readableBytes() < Utils.INTEGER_BYTE_SIZE) {
					return false;
				}
				if (mapSize == -1) {
					mapSize = buf.readInt();
				}
				if (dataMap == null) {
					dataMap = new DataMap(new TreeMap<Number640, Data>());
				}
				if (data != null) {
					if (!data.decodeBuffer(buf)) {
						return false;
					}
					if (!data.decodeDone(buf, message.publicKey(0), signatureFactory)) {
						return false;
					}
					data = null;
					key = null;
				}
				for (int i = dataMap.size(); i < mapSize; i++) {
					if (key == null) {
						if (buf.readableBytes() < Number160.BYTE_ARRAY_SIZE + Number160.BYTE_ARRAY_SIZE
								+ Number160.BYTE_ARRAY_SIZE + Number160.BYTE_ARRAY_SIZE) {
							return false;
						}
						byte[] me3 = new byte[Number160.BYTE_ARRAY_SIZE];
						buf.readBytes(me3);
						Number160 locationKey = new Number160(me3);
						buf.readBytes(me3);
						Number160 domainKey = new Number160(me3);
						buf.readBytes(me3);
						Number160 contentKey = new Number160(me3);
						buf.readBytes(me3);
						Number160 versionKey = new Number160(me3);
						key = new Number640(locationKey, domainKey, contentKey, versionKey);
					}
					LOG.debug("Key decoded in message {}, remaining {}", message, buf.readableBytes());
					data = Data.decodeHeader(buf, signatureFactory);
					if (data == null) {
						return false;
					}
					LOG.debug("Header decoded in message {}, remaining {}", message, buf.readableBytes());
					dataMap.dataMap().put(key, data);

					if (!data.decodeBuffer(buf)) {
						return false;
					}
					LOG.debug("Buffer decoded in message {}", message);
					if (!data.decodeDone(buf, message.publicKey(0), signatureFactory)) {
						return false;
					}
					LOG.debug("Done decoded in message {}", message);
					// if we have signed the message, set the public key anyway, but only if we indicated so
					inheritPublicKey(message, data);
					data = null;
					key = null;
				}

				message.setDataMap(dataMap);
				lastContent = contentTypes.poll();
				mapSize = -1;
				dataMap = null;
				break;
			case MAP_KEY640_KEYS:
				if (keyMap640KeysSize == -1 && buf.readableBytes() < Utils.INTEGER_BYTE_SIZE) {
					return false;
				}
				if (keyMap640KeysSize == -1) {
					keyMap640KeysSize = buf.readInt();
				}
				if (keyMap640Keys == null) {
					keyMap640Keys = new KeyMap640Keys(new TreeMap<Number640, Collection<Number160>>());
				}

				final int meta = Number160.BYTE_ARRAY_SIZE + Number160.BYTE_ARRAY_SIZE
						+ Number160.BYTE_ARRAY_SIZE + Number160.BYTE_ARRAY_SIZE;

				for (int i = keyMap640Keys.size(); i < keyMap640KeysSize; i++) {
					if (buf.readableBytes() < meta + Utils.BYTE_BYTE_SIZE) {
						return false;
					}
					size = buf.getUnsignedByte(buf.readerIndex() + meta);
					
					if (buf.readableBytes() < meta + Utils.BYTE_BYTE_SIZE + (size * Number160.BYTE_ARRAY_SIZE )) {
						return false;
					}
					byte[] me3 = new byte[Number160.BYTE_ARRAY_SIZE];
					buf.readBytes(me3);
					Number160 locationKey = new Number160(me3);
					buf.readBytes(me3);
					Number160 domainKey = new Number160(me3);
					buf.readBytes(me3);
					Number160 contentKey = new Number160(me3);
					buf.readBytes(me3);
					Number160 versionKey = new Number160(me3);

					int numBasedOn = buf.readByte();
					Set<Number160> value = new HashSet<Number160>(numBasedOn);
					for (int j = 0; j < numBasedOn; j++) {
						buf.readBytes(me3);
						Number160 basedOnKey = new Number160(me3);
						value.add(basedOnKey);
					}

					keyMap640Keys.put(new Number640(locationKey, domainKey, contentKey, versionKey), value);
				}

				message.keyMap640Keys(keyMap640Keys);
				lastContent = contentTypes.poll();
				keyMap640KeysSize = -1;
				keyMap640Keys = null;
				break;
			case MAP_KEY640_BYTE:
				if (keyMapByteSize == -1 && buf.readableBytes() < Utils.INTEGER_BYTE_SIZE) {
					return false;
				}
				if (keyMapByteSize == -1) {
					keyMapByteSize = buf.readInt();
				}
				if (keyMapByte == null) {
					keyMapByte = new KeyMapByte(new HashMap<Number640, Byte>(2 * keyMapByteSize));
				}

				for (int i = keyMapByte.size(); i < keyMapByteSize; i++) {
					if (buf.readableBytes() < Number160.BYTE_ARRAY_SIZE + Number160.BYTE_ARRAY_SIZE
							+ Number160.BYTE_ARRAY_SIZE + Number160.BYTE_ARRAY_SIZE + 1) {
						return false;
					}
					byte[] me3 = new byte[Number160.BYTE_ARRAY_SIZE];
					buf.readBytes(me3);
					Number160 locationKey = new Number160(me3);
					buf.readBytes(me3);
					Number160 domainKey = new Number160(me3);
					buf.readBytes(me3);
					Number160 contentKey = new Number160(me3);
					buf.readBytes(me3);
					Number160 versionKey = new Number160(me3);
					byte value = buf.readByte();
					keyMapByte.put(new Number640(locationKey, domainKey, contentKey, versionKey), value);
				}

				message.keyMapByte(keyMapByte);
				lastContent = contentTypes.poll();
				keyMapByteSize = -1;
				keyMapByte = null;
				break;
			case BYTE_BUFFER:
				if (bufferSize == -1 && buf.readableBytes() < Utils.INTEGER_BYTE_SIZE) {
					return false;
				}
				if (bufferSize == -1) {
					bufferSize = buf.readInt();
				}
				if (buffer == null) {
					buffer = new DataBuffer();
				}
				
				final int remaining = bufferSize - bufferTransferred;
				bufferTransferred += buffer.transferFrom(buf, remaining);
				
				if(bufferTransferred < bufferSize) {
					LOG.debug("Still looking for data. Indicating that its not finished yet. Already Transferred = {}, Size = {}.", bufferTransferred, bufferSize);
					return false;
				}
				
				message.buffer(new Buffer(buffer.toByteBuf(), bufferSize));
				lastContent = contentTypes.poll();
				bufferSize = -1;
				bufferTransferred = 0;
				buffer = null;
				break;
			case SET_TRACKER_DATA:
				if (trackerDataSize == -1 && buf.readableBytes() < Utils.BYTE_BYTE_SIZE) {
					return false;
				}
				if (trackerDataSize == -1) {
					trackerDataSize = buf.readUnsignedByte();
				}
				if (trackerData == null) {
					trackerData = new TrackerData(new HashMap<PeerAddress, Data>(2 * trackerDataSize));
				}
				if (currentTrackerData != null) {
					if (!currentTrackerData.decodeBuffer(buf)) {
						return false;
					}
					if (!currentTrackerData.decodeDone(buf, message.publicKey(0), signatureFactory)) {
						return false;
					}
					currentTrackerData = null;
				}
				for (int i = trackerData.size(); i < trackerDataSize; i++) {

					if (buf.readableBytes() < Utils.SHORT_BYTE_SIZE) {
						return false;
					}

					int header = buf.getUnsignedShort(buf.readerIndex());

					size = PeerAddress.size(header);

					if (buf.readableBytes() < size) {
						return false;
					}
					PeerAddress pa = new PeerAddress(buf);

					currentTrackerData = Data.decodeHeader(buf, signatureFactory);
					if (currentTrackerData == null) {
						return false;
					}
					trackerData.peerAddresses().put(pa, currentTrackerData);
					if (message.isSign()) {
						currentTrackerData.publicKey(message.publicKey(0));
					}

					if (!currentTrackerData.decodeBuffer(buf)) {
						return false;
					}
					if (!currentTrackerData.decodeDone(buf, message.publicKey(0), signatureFactory)) {
						return false;
					}
					currentTrackerData = null;
				}

				message.trackerData(trackerData);
				lastContent = contentTypes.poll();
				trackerDataSize = -1;
				trackerData = null;
				break;
			case PUBLIC_KEY: // fall-through
			case PUBLIC_KEY_SIGNATURE:
				receivedPublicKey = signatureFactory.decodePublicKey(buf);
				if(content == Content.PUBLIC_KEY_SIGNATURE) {
					if (receivedPublicKey == PeerBuilder.EMPTY_PUBLIC_KEY) {
						throw new InvalidKeyException("The public key cannot be empty.");
					}
				}
				if (receivedPublicKey == null) {
					return false;
				}
				
				message.publicKey(receivedPublicKey);
				lastContent = contentTypes.poll();
				break;
			default:
				break;
			}
		}
		LOG.debug("Parsed content in message {}", message);
		if (message.isSign()) {
			size = signatureFactory.signatureSize();
			if(buf.readableBytes() < size) {
				return false;
			}
			
			SignatureCodec signatureEncode = signatureFactory.signatureCodec(buf);
			message.receivedSignature(signatureEncode);
		}
		return true;
	}

	public Message prepareFinish() {
		Message ret = message;
		message.setDone();
		contentTypes.clear();
		message = null;
		headerDone = false;
		neighborSize = -1;
		neighborSet = null;
		keyCollectionSize = -1;
		keyCollection = null;
		mapSize = -1;
		dataMap = null;
		data = null;
		keyMap640KeysSize = -1;
		keyMap640Keys = null;
		bufferSize = -1;
		bufferTransferred = 0;
		buffer = null;
		signature = null;
		return ret;
	}

	public Message message() {
		return message;
	}

	public Content lastContent() {
		return lastContent;
	}
	
	public static void inheritPublicKey(Message message, Data data) {
	    if (message.isSign() && message.publicKey(0) != null && data.hasPublicKey() 
	    		&& (data.publicKey() == null || data.publicKey() == PeerBuilder.EMPTY_PUBLIC_KEY)) {
	    	data.publicKey(message.publicKey(0));
	    }
    }

	public void release() {
		if(message!=null) {
			message.release();
		}
		//release partial data
		if(data != null) {
			data.release();
		}
		if(dataMap != null) {
			for(Data data: dataMap.dataMap().values()) {
				data.release();
			}
		}
		if(buffer != null) {
			buffer.release();
		}
		
		if(currentTrackerData != null) {
			currentTrackerData.release();
		}
		if(trackerData != null) {
			for(Data data: trackerData.peerAddresses().values()) {
				data.release();
			}
		}
		
	}
}
