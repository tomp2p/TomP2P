package net.tomp2p.message;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.message.Message.Content;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket4Address;
import net.tomp2p.peers.PeerSocketAddress.PeerSocket6Address;
import net.tomp2p.rpc.RPC.Commands;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.AlternativeCompositeByteBuf;
import net.tomp2p.storage.Data;

public class Encoder {

    private static final Logger LOG = LoggerFactory.getLogger(Encoder.class);

    private final DataFilter dataFilterTTL = new DataFilterTTL();
    
    private Message message;

    private final SignatureFactory signatureFactory;

    public Encoder(SignatureFactory signatureFactory) {
        this.signatureFactory = signatureFactory;
    }

    public boolean write(final AlternativeCompositeByteBuf buf, final Message message, SignatureCodec signatureCodec) throws InvalidKeyException,
            SignatureException, IOException {

        this.message = message;
        LOG.debug("message for outbound {}", message);
      
        MessageHeaderCodec.encodeHeader(buf, message);

        boolean done = loop(buf);
        LOG.debug("message encoded {}", message);

        // write out what we have
        if (buf.isReadable() && done) {

            // check if we need to sign the message
            if (message.isSign()) {
            	//we sign if we did not provide a signature already
            	if(signatureCodec == null) {
            		signatureCodec = signatureFactory.sign(message.privateKey(), buf.nioBuffers());
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
            	message.key(next.index()).encode(buf);
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
            	final NeighborSet neighborSet = message.neighborsSet(next.index());
                // length
                buf.writeByte(neighborSet.size());
                for (PeerAddress neighbor : neighborSet.neighbors()) {
                	neighbor.encode(buf);
                }
                message.contentReferences().poll();
                break;
            case PEER_SOCKET4:
            	final PeerSocket4Address psa4 = message.peerSocket4Address(next.index());
            	psa4.encode(buf);
                message.contentReferences().poll();
                break;
            case PEER_SOCKET6:
            	final PeerSocket6Address psa6 = message.peerSocket6Address(next.index());
            	psa6.encode(buf);
                message.contentReferences().poll();
                break;
            case BLOOM_FILTER:
            	final SimpleBloomFilter<Number160> simpleBloomFilter = message.bloomFilter(next.index());
                simpleBloomFilter.encode(buf);
                message.contentReferences().poll();
                break;
            case SET_KEY640:
                final KeyCollection keys = message.keyCollection(next.index());
                // length
                buf.writeInt(keys.size());
                if (keys.isConvert()) {
                    for (final Number160 key : keys.keysConvert()) {
                    	keys.locationKey().encode(buf);
                    	keys.domainKey().encode(buf);
                    	key.encode(buf);
                    	keys.versionKey().encode(buf);
                    }
                } else {
                    for (final Number640 key : keys.keys()) {
                    	key.locationKey().encode(buf);
                    	key.domainKey().encode(buf);
                    	key.contentKey().encode(buf);
                    	key.versionKey().encode(buf);
                    }
                }
                message.contentReferences().poll();
                break;
            case MAP_KEY640_DATA:
                final DataMap dataMap = message.dataMap(next.index());
                // length
                buf.writeInt(dataMap.size());
                if (dataMap.isConvert()) {
                    for (final Entry<Number160, Data> entry : dataMap.dataMapConvert().entrySet()) {
                    	dataMap.locationKey().encode(buf);
                    	dataMap.domainKey().encode(buf);
                    	entry.getKey().encode(buf);
                    	dataMap.versionKey().encode(buf);
                    	encodeData(buf, entry.getValue(), dataMap.isConvertMeta(), !message.isRequest(), message.command() == Commands.REPLICA_PUT.getNr());
                    }
                } else {
                    for (final Entry<Number640, Data> entry : dataMap.dataMap().entrySet()) {
                    	entry.getKey().locationKey().encode(buf);
                    	entry.getKey().domainKey().encode(buf);
                    	entry.getKey().contentKey().encode(buf);
                    	entry.getKey().versionKey().encode(buf);
                        encodeData(buf, entry.getValue(), dataMap.isConvertMeta(), !message.isRequest(), message.command() == Commands.REPLICA_PUT.getNr());
                    }
                }
                message.contentReferences().poll();
                break;
            case MAP_KEY640_KEYS:
                final KeyMap640Keys keyMap640Keys = message.keyMap640Keys(next.index());
                // length
                buf.writeInt(keyMap640Keys.size());
                for (final Entry<Number640, Collection<Number160>> entry : keyMap640Keys.keysMap().entrySet()) {
                	entry.getKey().locationKey().encode(buf);
                	entry.getKey().domainKey().encode(buf);
                	entry.getKey().contentKey().encode(buf);
                	entry.getKey().versionKey().encode(buf);
                    // write number of based-on keys
                    buf.writeByte(entry.getValue().size());
                    // write based-on keys
                    for (final Number160 basedOnKey : entry.getValue()) {
                    	basedOnKey.encode(buf);
                    }
                }
                message.contentReferences().poll();
                break;
            case MAP_KEY640_BYTE:
                final KeyMapByte keysMap = message.keyMapByte(next.index());
                // length
                buf.writeInt(keysMap.size());
                for (final Entry<Number640, Byte> entry : keysMap.keysMap().entrySet()) {
                	entry.getKey().locationKey().encode(buf);
                	entry.getKey().domainKey().encode(buf);
                	entry.getKey().contentKey().encode(buf);
                	entry.getKey().versionKey().encode(buf);
                    buf.writeByte(entry.getValue());
                }
                message.contentReferences().poll();
                break;
            case BYTE_BUFFER:
            	final Buffer buffer = message.buffer(next.index());
            	// length
                buf.writeInt(buffer.length());
                buf.addComponent(buffer.buffer());
                message.contentReferences().poll();
                break;
            case SET_TRACKER_DATA:
            	final TrackerData trackerData = message.trackerData(next.index());
                buf.writeByte(trackerData.peerAddresses().size()); // 1 bytes - length, max. 255
                for (final Map.Entry<PeerAddress, Data> entry : trackerData.peerAddresses().entrySet()) {
                	entry.getKey().encode(buf);
                	final Data data = entry.getValue().duplicate();
                    encodeData(buf, data, false, !message.isRequest(), message.command() == Commands.REPLICA_PUT.getNr());
                }
                message.contentReferences().poll();
                break;
            case PUBLIC_KEY_SIGNATURE:
                // flag to encode public key
                message.setHintSign();
                // then do the regular public key stuff -> no break
            case PUBLIC_KEY:
            	final PublicKey publicKey = message.publicKey(next.index());
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

	private void encodeData(AlternativeCompositeByteBuf buf, Data data, boolean isConvertMeta, boolean isReply, boolean isReplicaSend) throws InvalidKeyException, SignatureException, IOException {
		Data filteredData = dataFilterTTL.filter(data, isConvertMeta, isReply);
		filteredData.encodeHeader(buf, signatureFactory);
		filteredData.encodeBuffer(buf);
		filteredData.encodeDone(buf, signatureFactory, message.privateKey());
		if(isReply || isReplicaSend) {
			filteredData.release();
		}
    }

    public Message message() {
        return message;
    }
}
