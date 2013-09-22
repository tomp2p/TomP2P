package net.tomp2p.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;

import java.nio.ByteBuffer;
import java.security.Signature;
import java.util.Map;
import java.util.Map.Entry;

import net.tomp2p.connection2.SignatureFactory;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number480;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.SimpleBloomFilter;
import net.tomp2p.storage.Data;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TomP2POutbound extends ChannelOutboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(TomP2POutbound.class);
    private final boolean preferDirect;

    private boolean header = false;
    private boolean resume = false;
    private Message2 message;

    private SignatureFactory signatureFactory;

    public TomP2POutbound(boolean preferDirect, SignatureFactory signatureFactory) {
        this.preferDirect = preferDirect;
        this.signatureFactory = signatureFactory;
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise)
            throws Exception {
        ByteBuf buf = null;
        try {
            if (msg instanceof Message2) {
                message = (Message2) msg;
                LOG.debug("message for outbound {}", message);
            }

            if (preferDirect) {
                buf = ctx.alloc().ioBuffer();
            } else {
                buf = ctx.alloc().heapBuffer();
            }

            if (!header) {
                MessageHeaderCodec.encodeHeader(buf, (Message2) msg);
                header = true;
            } else {
                LOG.debug("send a follow up message {}", message);
                resume = true;
            }

            LOG.debug("entering loop");
            boolean done = loop(buf);
            
            LOG.debug("exiting loop");

            // write out what we have
            if (buf.isReadable()) {

                // check if we need to sign the message
                if (message.isSign()) {
                    Signature signature = signatureFactory.signatureInstance();

                    signature.initSign(message.getPrivateKey());
                    // debug2 = message.getPublicKey();
                    for (int i = 0; i < buf.nioBufferCount(); i++) {
                        ByteBuffer buffer = buf.nioBuffers()[i];
                        signature.update(buffer);
                    }
                    byte[] signatureData = signature.sign();

                    SHA1Signature decodedSignature = new SHA1Signature();
                    decodedSignature.decode(signatureData);
                    buf.writeBytes(decodedSignature.getNumber1().toByteArray());
                    buf.writeBytes(decodedSignature.getNumber2().toByteArray());
                }
                // this will release the buffer
                if (ctx.channel() instanceof DatagramChannel) {
                    if (message.senderSocket() == null) {
                        message.senderSocket(message.getRecipient().createSocketUDP());
                    }
                    DatagramPacket d = new DatagramPacket(buf, message.senderSocket(), message.recipientSocket());
                    LOG.debug("Send UPD message {}, datagram: {}", message, d);
                    ctx.writeAndFlush(d, promise);
                    if (done) {
                        message.done(true);
                        // we wrote the complete message, reset state
                        header = false;
                    }
                } else {
                    LOG.debug("Send TCP message {} to {}", message, message.senderSocket());
                    ctx.writeAndFlush(buf, promise);
                    if (done) {
                        message.done(true);
                        header = false;
                    }
                }
            } else {
                buf.release();
                ctx.write(Unpooled.EMPTY_BUFFER, promise);
            }
            buf = null;

        } finally {
            if (buf != null) {
                buf.release();
            }
        }
    }

    private boolean loop(ByteBuf buf) {
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
            case SET_KEY480:
                // length
                Keys keys = message.getKeys(next.number());
                buf.writeInt(keys.size());
                if (keys.isConvert()) {
                    for (Number160 key : keys.keysConvert()) {
                        buf.writeBytes(keys.locationKey().toByteArray());
                        buf.writeBytes(keys.domainKey().toByteArray());
                        buf.writeBytes(key.toByteArray());
                    }
                } else {
                    for (Number480 key : keys.keys()) {
                        buf.writeBytes(key.getLocationKey().toByteArray());
                        buf.writeBytes(key.getDomainKey().toByteArray());
                        buf.writeBytes(key.getContentKey().toByteArray());
                    }
                }
                message.contentRefencencs().poll();
                break;
            case MAP_KEY480_DATA:
                DataMap dataMap = message.getDataMap(next.number());
                buf.writeInt(dataMap.size());
                if (dataMap.isConvert()) {
                    for (Entry<Number160, Data> entry : dataMap.dataMapConvert().entrySet()) {
                        buf.writeBytes(dataMap.locationKey().toByteArray());
                        buf.writeBytes(dataMap.domainKey().toByteArray());
                        buf.writeBytes(entry.getKey().toByteArray());
                        Data data = entry.getValue().duplicate();
                        data.encode(buf);
                        data.encodeDone(buf);
                    }
                } else {
                    for (Entry<Number480, Data> entry : dataMap.dataMap().entrySet()) {
                        buf.writeBytes(entry.getKey().getLocationKey().toByteArray());
                        buf.writeBytes(entry.getKey().getDomainKey().toByteArray());
                        buf.writeBytes(entry.getKey().getContentKey().toByteArray());
                        Data data = entry.getValue().duplicate();
                        data.encode(buf);
                        data.encodeDone(buf);
                    }
                }
                message.contentRefencencs().poll();
                break;
            case MAP_KEY480_KEY:
                KeysMap keysMap = message.getKeysMap(next.number());
                buf.writeInt(keysMap.size());
                for (Entry<Number480, Number160> entry : keysMap.keysMap().entrySet()) {
                    buf.writeBytes(entry.getKey().getLocationKey().toByteArray());
                    buf.writeBytes(entry.getKey().getDomainKey().toByteArray());
                    buf.writeBytes(entry.getKey().getContentKey().toByteArray());
                    buf.writeBytes(entry.getValue().toByteArray());
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
                    data.encode(buf);
                    data.encodeDone(buf);
                }
                message.contentRefencencs().poll();
                break;
            case PUBLIC_KEY_SIGNATURE:
                // flag to encode public key
                message.setHintSign();
                byte[] data = message.getPublicKey().getEncoded();
                buf.writeShort(data.length);
                buf.writeBytes(data);
                message.contentRefencencs().poll();
                break;
            default:
                throw new RuntimeException("Unknown type: " + next.content());
            }

        }
        return true;
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
        if (message == null) {
            LOG.error("exception in encoding, starting", cause);
            cause.printStackTrace();
        } else if (message != null && !message.isDone()) {
            LOG.error("exception in encoding, started", cause);
            cause.printStackTrace();
        }
    }
}
