package net.tomp2p.message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Map;
import java.util.Map.Entry;

import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
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
                Signature signature = signatureFactory.signatureInstance();

                signature.initSign(message.getPrivateKey());
                // debug2 = message.getPublicKey();
                ByteBuffer[] byteBuffers = buf.nioBuffers();
                int len = byteBuffers.length;
                for (int i = 0; i < len; i++) {
                    ByteBuffer buffer = byteBuffers[i];
                    signature.update(buffer);
                }
                byte[] signatureData = signature.sign();

                SHA1Signature decodedSignature = new SHA1Signature();
                decodedSignature.decode(signatureData);
                buf.writeBytes(decodedSignature.getNumber1().toByteArray());
                buf.writeBytes(decodedSignature.getNumber2().toByteArray());
            }
        }
        return done;
    }

    private boolean loop(AlternativeCompositeByteBuf buf) {
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
                NeighborSet neighborSet = message.getNeighborsSet(next.number());
                // length
                buf.writeByte(neighborSet.size());
                for (PeerAddress neighbor : neighborSet.neighbors()) {
                    buf.writeBytes(neighbor.toByteArray());
                }
                message.contentRefencencs().poll();
                break;
            case BLOOM_FILTER:
                SimpleBloomFilter<Number160> simpleBloomFilter = message.getBloomFilter(next.number());
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
                    for (Entry<Number160, Data> entry : dataMap.dataMapConvert().entrySet()) {
                        buf.writeBytes(dataMap.locationKey().toByteArray());
                        buf.writeBytes(dataMap.domainKey().toByteArray());
                        buf.writeBytes(entry.getKey().toByteArray());
                        buf.writeBytes(dataMap.versionKey().toByteArray());
                        Data data = entry.getValue().duplicate();
                        data.encodeHeader(buf);
                        data.encodeDone(buf);
                    }
                } else {
                    for (Entry<Number640, Data> entry : dataMap.dataMap().entrySet()) {
                        buf.writeBytes(entry.getKey().getLocationKey().toByteArray());
                        buf.writeBytes(entry.getKey().getDomainKey().toByteArray());
                        buf.writeBytes(entry.getKey().getContentKey().toByteArray());
                        buf.writeBytes(entry.getKey().getVersionKey().toByteArray());
                        Data data = entry.getValue().duplicate();
                        data.encodeHeader(buf);
                        data.encodeDone(buf);
                    }
                }
                message.contentRefencencs().poll();
                break;
            case MAP_KEY640_KEY:
                KeyMap640 keyMap640 = message.getKeyMap640(next.number());
                buf.writeInt(keyMap640.size());
                for (Entry<Number640, Number160> entry : keyMap640.keysMap().entrySet()) {
                    buf.writeBytes(entry.getKey().getLocationKey().toByteArray());
                    buf.writeBytes(entry.getKey().getDomainKey().toByteArray());
                    buf.writeBytes(entry.getKey().getContentKey().toByteArray());
                    buf.writeBytes(entry.getKey().getVersionKey().toByteArray());
                    buf.writeBytes(entry.getValue().toByteArray());
                }
                message.contentRefencencs().poll();
                break;
            case MAP_KEY640_BYTE:
                KeyMapByte keysMap = message.getKeyMapByte(next.number());
                buf.writeInt(keysMap.size());
                for (Entry<Number640, Byte> entry : keysMap.keysMap().entrySet()) {
                    buf.writeBytes(entry.getKey().getLocationKey().toByteArray());
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
                    LOG.debug("we sent a partial message of length {}", readable);
                    return false;
                } else {
                    LOG.debug("Announced a larger buffer, but not in streaming mode. This is wrong.");
                    throw new RuntimeException(
                            "Announced a larger buffer, but not in streaming mode. This is wrong.");
                }
                break;
            case SET_TRACKER_DATA:
                TrackerData trackerData = message.getTrackerData(next.number());
                buf.writeByte(trackerData.getPeerAddresses().size()); // 1 bytes - length, max. 255
                for (Map.Entry<PeerAddress, Data> entry : trackerData.getPeerAddresses().entrySet()) {
                    buf.writeBytes(entry.getKey().toByteArray());
                    Data data = entry.getValue().duplicate();
                    data.encodeHeader(buf);
                    data.encodeDone(buf);
                }
                message.contentRefencencs().poll();
                break;
            case PUBLIC_KEY_SIGNATURE:
                // flag to encode public key
                message.setHintSign();
                signatureFactory.encodePublicKey(message.getPublicKey(), buf);
                message.contentRefencencs().poll();
                break;
            default:
            case USER1:
            case USER2:
            case USER3:
                throw new RuntimeException("Unknown type: " + next.content());
            }

        }
        return true;
    }

    public Message message() {
        return message;
    }

    public void reset() {
        header = false;
        resume = false;
    }
}
