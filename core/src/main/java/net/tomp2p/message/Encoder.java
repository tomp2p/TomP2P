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
import net.tomp2p.peers.PeerStatatistic;
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

    private SignatureFactory signatureFactory;

    public Encoder(SignatureFactory signatureFactory) {
        this.signatureFactory = signatureFactory;
    }

    public boolean write(final AlternativeCompositeByteBuf buf, final Message message) throws InvalidKeyException,
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
            	SignatureCodec decodedSignature = signatureFactory.sign(message.privateKey(), buf);
            	decodedSignature.write(buf);
            }
        }
        return done;
    }

    private boolean loop(AlternativeCompositeByteBuf buf) throws InvalidKeyException, SignatureException, IOException {
        NumberType next;
        while ((next = message.contentRefencencs().peek()) != null) {
        	final int start = buf.writerIndex();
        	final Content content = next.content(); 
            switch (content) {
            case KEY:
                buf.writeBytes(message.key(next.number()).toByteArray());
                message.contentRefencencs().poll();
                break;
            case INTEGER:
                buf.writeInt(message.intAt(next.number()));
                message.contentRefencencs().poll();
                break;
            case LONG:
                buf.writeLong(message.longAt(next.number()));
                message.contentRefencencs().poll();
                break;
            case SET_NEIGHBORS:
                NeighborSet neighborSet = message.neighborsSet(next.number());
                // length
                buf.writeByte(neighborSet.size());
                for (PeerAddress neighbor : neighborSet.neighbors()) {
                    buf.writeBytes(neighbor.toByteArray());
                }
                message.contentRefencencs().poll();
                break;
            case SET_PEER_SOCKET:
                List<PeerSocketAddress> list = message.peerSocketAddresses();
                // length
                buf.writeByte(list.size());
                for (PeerSocketAddress addr : list) {
                	buf.writeByte(addr.isIPv4() ? 0:1);
                    buf.writeBytes(addr.toByteArray());
                }
                message.contentRefencencs().poll();
                break;
            case BLOOM_FILTER:
                SimpleBloomFilter<Number160> simpleBloomFilter = message.bloomFilter(next.number());
                simpleBloomFilter.toByteBuf(buf);
                message.contentRefencencs().poll();
                break;
            case SET_KEY640:
                // length
                KeyCollection keys = message.keyCollection(next.number());
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
                message.contentRefencencs().poll();
                break;
            case MAP_KEY640_DATA:
                DataMap dataMap = message.dataMap(next.number());
                
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
                message.contentRefencencs().poll();
                break;
            case MAP_KEY640_KEYS:
                KeyMap640Keys keyMap640Keys = message.keyMap640Keys(next.number());
                buf.writeInt(keyMap640Keys.size());
                for (Entry<Number640, Collection<Number160>> entry : keyMap640Keys.keysMap().entrySet()) {
                    buf.writeBytes(entry.getKey().locationKey().toByteArray());
                    buf.writeBytes(entry.getKey().domainKey().toByteArray());
                    buf.writeBytes(entry.getKey().contentKey().toByteArray());
                    buf.writeBytes(entry.getKey().versionKey().toByteArray());
                    // write # of based on keys
                    buf.writeByte(entry.getValue().size());
                    // write based on keys
                    for (Number160 basedOnKey : entry.getValue()) {
                        buf.writeBytes(basedOnKey.toByteArray());
                    }
                }
                message.contentRefencencs().poll();
                break;
            case MAP_KEY640_BYTE:
                KeyMapByte keysMap = message.keyMapByte(next.number());
                buf.writeInt(keysMap.size());
                for (Entry<Number640, Byte> entry : keysMap.keysMap().entrySet()) {
                    buf.writeBytes(entry.getKey().locationKey().toByteArray());
                    buf.writeBytes(entry.getKey().domainKey().toByteArray());
                    buf.writeBytes(entry.getKey().contentKey().toByteArray());
                    buf.writeBytes(entry.getKey().versionKey().toByteArray());
                    buf.writeByte(entry.getValue());
                }
                message.contentRefencencs().poll();
                break;
            case BYTE_BUFFER:
                Buffer buffer = message.buffer(next.number());
                if (!resume) {
                    buf.writeInt(buffer.length());
                }
                int readable = buffer.readable();
                buf.writeBytes(buffer.buffer(), readable);
                if (buffer.incRead(readable) == buffer.length()) {
                    message.contentRefencencs().poll();
                } else if (message.isStreaming()) {
                    LOG.debug("we sent a partial message of length {}", readable);
                    return false;
                } else {
                    LOG.debug("Announced a larger buffer, but not in streaming mode. This is wrong.");
                    throw new RuntimeException(
                            "Announced a larger buffer, but not in streaming mode. This is wrong.");
                }
                break;
            case SET_TRACKER_DATA:
                TrackerData trackerData = message.trackerData(next.number());
                buf.writeByte(trackerData.peerAddresses().size()); // 1 bytes - length, max. 255
                for (Map.Entry<PeerStatatistic, Data> entry : trackerData.peerAddresses().entrySet()) {
                	byte[] me = entry.getKey().peerAddress().toByteArray();
                    buf.writeBytes(me);
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
            	PublicKey publicKey = message.publicKey(next.number());
            	signatureFactory.encodePublicKey(publicKey, buf);
            	message.contentRefencencs().poll();
            	break;
            default:
            case USER1:
                throw new RuntimeException("Unknown type: " + next.content());
            }
            LOG.debug("wrote in encoder for {} {}", content, buf.writerIndex() - start);

        }
        return true;
    }

	private int encodeData(AlternativeCompositeByteBuf buf, Data data, boolean isConvertMeta, boolean isReply) throws InvalidKeyException, SignatureException, IOException {
		if(isConvertMeta) {
			data = data.duplicateMeta();
		} else {
			data = data.duplicate();
		}
		if(isReply) {
			int ttl = (int) ((data.expirationMillis() - System.currentTimeMillis()) / 1000);
			data.ttlSeconds(ttl < 0 ? 0:ttl);
		}
		final int startWriter = buf.writerIndex();
	    data.encodeHeader(buf, signatureFactory);
	    data.encodeBuffer(buf);
	    data.encodeDone(buf, signatureFactory, message.privateKey());
	    return buf.writerIndex() - startWriter;
    }

    public Message message() {
        return message;
    }

    public void reset() {
        header = false;
        resume = false;
    }
}
