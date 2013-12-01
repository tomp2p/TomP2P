package net.tomp2p.message;

import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import net.tomp2p.connection.SignatureFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TomP2POutbound extends ChannelOutboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(TomP2POutbound.class);
    private final boolean preferDirect;
    private final Encoder encoder;

    public TomP2POutbound(boolean preferDirect, SignatureFactory signatureFactory) {
        this.preferDirect = preferDirect;
        this.encoder = new Encoder(signatureFactory);
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise)
            throws Exception {
        CompositeByteBuf buf = null;
        try {
            boolean done = false;
            if (msg instanceof Message) {
                Message message = (Message) msg;
                
                if (preferDirect) {
                    buf = ctx.alloc().compositeDirectBuffer(0);
                } else {
                    buf = ctx.alloc().compositeHeapBuffer(0);
                }
                done = encoder.write(buf, message);
            } else {
                ctx.write(msg, promise);
            }
            
            Message message = encoder.message();

            if (buf.isReadable()) {
                // this will release the buffer
                if (ctx.channel() instanceof DatagramChannel) {
                    if (message.senderSocket() == null) {
                        message.senderSocket(message.getRecipient().createSocketUDP());
                    }
                    DatagramPacket d = new DatagramPacket(buf, message.senderSocket(), message.recipientSocket());
                    LOG.debug("Send UPD message {}, datagram: {}", message, d);
                    ctx.writeAndFlush(d, promise);
                    
                } else {
                    LOG.debug("Send TCP message {} to {}", message, message.senderSocket());
                    ctx.writeAndFlush(buf, promise);
                }
                if (done) {
                    message.done(true);
                    // we wrote the complete message, reset state
                    encoder.reset();
                }
            } else {
                buf.release();
                ctx.write(Unpooled.EMPTY_BUFFER, promise);
            }
            buf = null;

        } catch (Throwable t) {
            ctx.fireExceptionCaught(t);
        }
        finally {
            if (buf != null) {
                buf.release();
            }
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
        if (encoder.message() == null) {
            LOG.error("exception in encoding, starting", cause);
            cause.printStackTrace();
        } else if (encoder.message() != null && !encoder.message().isDone()) {
            LOG.error("exception in encoding, started", cause);
            cause.printStackTrace();
        }
    }
}
