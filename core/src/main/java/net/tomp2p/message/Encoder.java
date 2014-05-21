package net.tomp2p.message;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.AlternativeCompositeByteBuf;
import net.tomp2p.storage.Data;
import net.tomp2p.utils.Timings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Encoder {

	private static final Logger LOG = LoggerFactory.getLogger(Encoder.class);

	private boolean header = false;
	private boolean resume = false;
	private Message message;

	private SignatureFactory signatureFactory;

	public Encoder(SignatureFactory signatureFactory) {
		this.signatureFactory = signatureFactory;
	}

	public boolean write(final AlternativeCompositeByteBuf buf,
			final Message message) throws InvalidKeyException,
			SignatureException, IOException {

		this.message = message;
		LOG.debug("message for outbound {}", message);

		if (!header) {
			MessageHeaderCodec.encodeHeader(buf, message);
			header = true;
		} else {
			LOG.debug("send a follow up message {}", message);
			resume = true;
		}

		LOG.debug("entering loop");
		boolean done = loop(buf);
		LOG.debug("exiting loop");

		// write out what we have
		if (buf.isReadable() && done) {

			// check if we need to sign the message
			if (message.isSign()) {
				SignatureCodec decodedSignature = signatureFactory.sign(
						message.getPrivateKey(), buf);
				decodedSignature.write(buf);
			}
		}
		return done;
	}

	private boolean loop(AlternativeCompositeByteBuf buf)
			throws InvalidKeyException, SignatureException, IOException {
		NumberType next;
		while ((next = message.contentRefencencs().peek()) != null) {
			switch (next.content()) {
			case KEY:
				buf.writeBytes(message.getKey(next.number()).toByteArray());
				message.contentRefencencs().poll();
				break;
			case INTEGER:
				buf.writeInt(message.getInteger(next.number()));
				message.contentRefencencs().poll();
				break;
			case LONG:
				buf.writeLong(message.getLong(next.number()));
				message.contentRefencencs().poll();
				break;
			case SET_NEIGHBORS:
				NeighborSet neighborSet = message
						.getNeighborsSet(next.number());
				// length
				buf.writeByte(neighborSet.size());
				for (PeerAddress neighbor : neighborSet.neighbors()) {
					buf.writeBytes(neighbor.toByteArray());
				}
				message.contentRefencencs().poll();
				break;
			case SET_PEER_SOCKET:
				List<PeerSocketAddress> list = message.getPeerSocketAddresses();
				// length
				buf.writeByte(list.size());
				for (PeerSocketAddress addr : list) {
					buf.writeByte(addr.isIPv4() ? 0 : 1);
					buf.writeBytes(addr.toByteArray());
				}
				message.contentRefencencs().poll();
				break;
			case BLOOM_FILTER:
				SimpleBloomFilter<Number160> simpleBloomFilter = message
						.getBloomFilter(next.number());
				simpleBloomFilter.toByteBuf(buf);
				message.contentRefencencs().poll();
				break;
			case SET_KEY640:
				// length
				KeyCollection keys = message.getKeyCollection(next.number());
				buf.writeInt(keys.size());
				if (keys.isConvert()) {
					for (Number160 key : keys.keysConvert()) {
						buf.writeBytes(keys.locationKey().toByteArray());
						buf.writeBytes(keys.domainKey().toByteArray());
						buf.writeBytes(key.toByteArray());
						buf.writeBytes(keys.versionKey().toByteArray());
					}
				} else {
					for (Number640 key : keys.keys()) {
						buf.writeBytes(key.getLocationKey().toByteArray());
						buf.writeBytes(key.getDomainKey().toByteArray());
						buf.writeBytes(key.getContentKey().toByteArray());
						buf.writeBytes(key.getVersionKey().toByteArray());
					}
				}
				message.contentRefencencs().poll();
				break;
			case MAP_KEY640_DATA:
				DataMap dataMap = message.getDataMap(next.number());

				buf.writeInt(dataMap.size());
				if (dataMap.isConvert()) {
					for (Entry<Number160, Data> entry : dataMap
							.dataMapConvert().entrySet()) {
						buf.writeBytes(dataMap.locationKey().toByteArray());
						buf.writeBytes(dataMap.domainKey().toByteArray());
						buf.writeBytes(entry.getKey().toByteArray());
						buf.writeBytes(dataMap.versionKey().toByteArray());
						encodeData(buf, entry.getValue(),
								dataMap.isConvertMeta(), !message.isRequest());
					}
				} else {
					for (Entry<Number640, Data> entry : dataMap.dataMap()
							.entrySet()) {
						buf.writeBytes(entry.getKey().getLocationKey()
								.toByteArray());
						buf.writeBytes(entry.getKey().getDomainKey()
								.toByteArray());
						buf.writeBytes(entry.getKey().getContentKey()
								.toByteArray());
						buf.writeBytes(entry.getKey().getVersionKey()
								.toByteArray());
						encodeData(buf, entry.getValue(),
								dataMap.isConvertMeta(), !message.isRequest());
					}
				}
				message.contentRefencencs().poll();
				break;
			case MAP_KEY640_KEYS:
				KeyMap640Keys keyMap640Keys = message.getKeyMap640Keys(next
						.number());
				buf.writeInt(keyMap640Keys.size());
				for (Entry<Number640, Set<Number160>> entry : keyMap640Keys
						.keysMap().entrySet()) {
					buf.writeBytes(entry.getKey().getLocationKey()
							.toByteArray());
					buf.writeBytes(entry.getKey().getDomainKey().toByteArray());
					buf.writeBytes(entry.getKey().getContentKey().toByteArray());
					buf.writeBytes(entry.getKey().getVersionKey().toByteArray());
					// write # of based on keys
					buf.writeByte(entry.getValue().size());
					// write based on keys
					for (Number160 basedOnKey : entry.getValue())
						buf.writeBytes(basedOnKey.toByteArray());
				}
				message.contentRefencencs().poll();
				break;
			case MAP_KEY640_BYTE:
				KeyMapByte keysMap = message.getKeyMapByte(next.number());
				buf.writeInt(keysMap.size());
				for (Entry<Number640, Byte> entry : keysMap.keysMap()
						.entrySet()) {
					buf.writeBytes(entry.getKey().getLocationKey()
							.toByteArray());
					buf.writeBytes(entry.getKey().getDomainKey().toByteArray());
					buf.writeBytes(entry.getKey().getContentKey().toByteArray());
					buf.writeBytes(entry.getKey().getVersionKey().toByteArray());
					buf.writeByte(entry.getValue());
				}
				message.contentRefencencs().poll();
				break;
			case BYTE_BUFFER:
				Buffer buffer = message.getBuffer(next.number());
				if (!resume) {
					buf.writeInt(buffer.length());
				}
				int readable = buffer.readable();
				buf.writeBytes(buffer.buffer(), readable);
				if (buffer.incRead(readable) == buffer.length()) {
					message.contentRefencencs().poll();
				} else if (message.isStreaming()) {
					LOG.debug("we sent a partial message of length {}",
							readable);
					return false;
				} else {
					LOG.debug("Announced a larger buffer, but not in streaming mode. This is wrong.");
					throw new RuntimeException(
							"Announced a larger buffer, but not in streaming mode. This is wrong.");
				}
				break;
			case SET_TRACKER_DATA:
				TrackerData trackerData = message.getTrackerData(next.number());
				buf.writeByte(trackerData.getPeerAddresses().size()); // 1 bytes
																		// -
																		// length,
																		// max.
																		// 255
				for (Map.Entry<PeerAddress, Data> entry : trackerData
						.getPeerAddresses().entrySet()) {
					buf.writeBytes(entry.getKey().toByteArray());
					Data data = entry.getValue().duplicate();
					encodeData(buf, data, false, !message.isRequest());
				}
				message.contentRefencencs().poll();
				break;
			case PUBLIC_KEY_SIGNATURE:
				// flag to encode public key
				message.setHintSign();
				// then do the regular public key stuff
			case PUBLIC_KEY:
				PublicKey publicKey = message.getPublicKey(next.number());
				signatureFactory.encodePublicKey(publicKey, buf);
				message.contentRefencencs().poll();
				break;
			default:
			case USER1:
				throw new RuntimeException("Unknown type: " + next.content());
			}

		}
		return true;
	}

	private void encodeData(AlternativeCompositeByteBuf buf, Data data,
			boolean isConvertMeta, boolean isReply) throws InvalidKeyException,
			SignatureException, IOException {
		if (isConvertMeta) {
			data = data.duplicateMeta();
		} else {
			data = data.duplicate();
		}
		if (isReply) {
			int ttl = (int) ((data.expirationMillis() - Timings
					.currentTimeMillis()) / 1000);
			data.ttlSeconds(ttl < 0 ? 0 : ttl);
		}
		data.encodeHeader(buf, signatureFactory);
		data.encodeDone(buf, signatureFactory);
	}

	public Message message() {
		return message;
	}

	public void reset() {
		header = false;
		resume = false;
	}
}
