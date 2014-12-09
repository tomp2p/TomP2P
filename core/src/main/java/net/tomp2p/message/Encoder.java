package net.tomp2p.message;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.message.Message.Content;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.AlternativeCompositeByteBuf;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Encoder {

    private static final Logger LOG = LoggerFactory.getLogger(Encoder.class);

    private boolean header = false;
    private boolean resume = false;
    private Message message;

    private final SignatureFactory signatureFactory;

    public Encoder(SignatureFactory signatureFactory) {
        this.signatureFactory = signatureFactory;
    }

    public boolean write(final AlternativeCompositeByteBuf buf, final Message message, SignatureCodec signatureCodec) throws InvalidKeyException,
            SignatureException, IOException {

        this.message = message;
        LOG.debug("message for outbound {}", message);

        if (!header) {
            MessageHeaderCodec.encodeHeader(buf, message);
            header = true;
        } else {
            LOG.debug("send a follow-up message {}", message);
            resume = true;
        }

        boolean done = loop(buf);
        LOG.debug("message encoded {}", message);

        // write out what we have
        if (buf.isReadable() && done) {

            // check if we need to sign the message
            if (message.isSign()) {
            	//we sign if we did not provide a signature already
            	if(signatureCodec == null) {
            		signatureCodec = signatureFactory.sign(message.privateKey(), buf);
            	}
            	//in case of relay, we have a signature, so we need to reuse this
            	signatureCodec.write(buf);
            }
        }
        return done;
    }

    private boolean loop(AlternativeCompositeByteBuf buf) throws InvalidKeyException, SignatureException, IOException {
        MessageContentIndex next;
        while ((next = message.contentReferences().peek()) != null) {
        	final int start = buf.writerIndex();
        	final Content content = next.content(); 
            switch (content) {
            case KEY:
                buf.writeBytes(message.key(next.index()).toByteArray());
                message.contentReferences().poll();
                break;
            case INTEGER:
                buf.writeInt(message.intAt(next.index()));
                message.contentReferences().poll();
                break;
            case LONG:
                buf.writeLong(message.longAt(next.index()));
                message.contentReferences().poll();
                break;
            case SET_NEIGHBORS:
                NeighborSet neighborSet = message.neighborsSet(next.index());
                // length
                buf.writeByte(neighborSet.size());
                for (PeerAddress neighbor : neighborSet.neighbors()) {
                    buf.writeBytes(neighbor.toByteArray());
                }
                message.contentReferences().poll();
                break;
            case SET_PEER_SOCKET:
                List<PeerSocketAddress> list = message.peerSocketAddresses();
                // length
                buf.writeByte(list.size());
                for (PeerSocketAddress psa : list) {
                	// IP version flag
                	buf.writeByte(psa.isIPv4() ? 0:1);
                    buf.writeBytes(psa.toByteArray());
                }
                message.contentReferences().poll();
                break;
            case BLOOM_FILTER:
                SimpleBloomFilter<Number160> simpleBloomFilter = message.bloomFilter(next.index());
                simpleBloomFilter.toByteBuf(buf);
                message.contentReferences().poll();
                break;
            case SET_KEY640:
                KeyCollection keys = message.keyCollection(next.index());
                // length
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
                        buf.writeBytes(key.locationKey().toByteArray());
                        buf.writeBytes(key.domainKey().toByteArray());
                        buf.writeBytes(key.contentKey().toByteArray());
                        buf.writeBytes(key.versionKey().toByteArray());
                    }
                }
                message.contentReferences().poll();
                break;
            case MAP_KEY640_DATA:
                DataMap dataMap = message.dataMap(next.index());
                // legnth
                buf.writeInt(dataMap.size());
                if (dataMap.isConvert()) {
                    for (Entry<Number160, Data> entry : dataMap.dataMapConvert().entrySet()) {
                    	buf.writeBytes(dataMap.locationKey().toByteArray());
                        buf.writeBytes(dataMap.domainKey().toByteArray());
                        buf.writeBytes(entry.getKey().toByteArray());
                        buf.writeBytes(dataMap.versionKey().toByteArray());
                        encodeData(buf, entry.getValue(), dataMap.isConvertMeta(), !message.isRequest());
                    }
                } else {
                    for (Entry<Number640, Data> entry : dataMap.dataMap().entrySet()) {
                        buf.writeBytes(entry.getKey().locationKey().toByteArray());
                        buf.writeBytes(entry.getKey().domainKey().toByteArray());
                        buf.writeBytes(entry.getKey().contentKey().toByteArray());
                        buf.writeBytes(entry.getKey().versionKey().toByteArray());
                        encodeData(buf, entry.getValue(), dataMap.isConvertMeta(), !message.isRequest());
                    }
                }
                message.contentReferences().poll();
                break;
            case MAP_KEY640_KEYS:
                KeyMap640Keys keyMap640Keys = message.keyMap640Keys(next.index());
                // length
                buf.writeInt(keyMap640Keys.size());
                for (Entry<Number640, Collection<Number160>> entry : keyMap640Keys.keysMap().entrySet()) {
                    buf.writeBytes(entry.getKey().locationKey().toByteArray());
                    buf.writeBytes(entry.getKey().domainKey().toByteArray());
                    buf.writeBytes(entry.getKey().contentKey().toByteArray());
                    buf.writeBytes(entry.getKey().versionKey().toByteArray());
                    // write number of based-on keys
                    buf.writeByte(entry.getValue().size());
                    // write based-on keys
                    for (Number160 basedOnKey : entry.getValue()) {
                        buf.writeBytes(basedOnKey.toByteArray());
                    }
                }
                message.contentReferences().poll();
                break;
            case MAP_KEY640_BYTE:
                KeyMapByte keysMap = message.keyMapByte(next.index());
                // length
                buf.writeInt(keysMap.size());
                for (Entry<Number640, Byte> entry : keysMap.keysMap().entrySet()) {
                    buf.writeBytes(entry.getKey().locationKey().toByteArray());
                    buf.writeBytes(entry.getKey().domainKey().toByteArray());
                    buf.writeBytes(entry.getKey().contentKey().toByteArray());
                    buf.writeBytes(entry.getKey().versionKey().toByteArray());
                    buf.writeByte(entry.getValue());
                }
                message.contentReferences().poll();
                break;
            case BYTE_BUFFER:
                Buffer buffer = message.buffer(next.index());
                if (!resume) {
                    buf.writeInt(buffer.length());
                }
                // length
                int readable = buffer.readable();
                buf.writeBytes(buffer.buffer(), readable);
                if (buffer.incRead(readable) == buffer.length()) {
                    message.contentReferences().poll();
                } else if (message.isStreaming()) {
                    LOG.debug("Partial message of lengt {} sent.", readable);
                    return false;
                } else {
                	final String description = "Larger buffer has been announced, but not in message streaming mode. This is wrong.";
                    LOG.error(description);
                    throw new RuntimeException(description);
                }
                break;
            case SET_TRACKER_DATA:
                TrackerData trackerData = message.trackerData(next.index());
                buf.writeByte(trackerData.peerAddresses().size()); // 1 bytes - length, max. 255
                for (Map.Entry<PeerAddress, Data> entry : trackerData.peerAddresses().entrySet()) {
                	byte[] me = entry.getKey().toByteArray();
                    buf.writeBytes(me);
                    Data data = entry.getValue().duplicate();
                    encodeData(buf, data, false, !message.isRequest());
                }
                message.contentReferences().poll();
                break;
            case PUBLIC_KEY_SIGNATURE:
                // flag to encode public key
                message.setHintSign();
                // then do the regular public key stuff -> no break
            case PUBLIC_KEY:
            	PublicKey publicKey = message.publicKey(next.index());
            	signatureFactory.encodePublicKey(publicKey, buf);
            	message.contentReferences().poll();
            	break;
            default:
                throw new RuntimeException("Unknown type: " + next.content());
            }
            LOG.debug("wrote in encoder for {} {}", content, buf.writerIndex() - start);

        }
        return true;
    }

	private void encodeData(AlternativeCompositeByteBuf buf, Data data, boolean isConvertMeta, boolean isReply) throws InvalidKeyException, SignatureException, IOException {
		if(isConvertMeta) {
			data = data.duplicateMeta();
		} else {
			data = data.duplicate();
		}
		if(isReply) {
			int ttl = (int) ((data.expirationMillis() - System.currentTimeMillis()) / 1000);
			data.ttlSeconds(ttl < 0 ? 0 : ttl);
		}
	    data.encodeHeader(buf, signatureFactory);
	    data.encodeBuffer(buf);
	    data.encodeDone(buf, signatureFactory, message.privateKey());
    }

    public Message message() {
        return message;
    }

    public void reset() {
        header = false;
        resume = false;
    }
}
