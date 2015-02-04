package net.tomp2p.message;

import java.net.InetSocketAddress;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.storage.AlternativeCompositeByteBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TomP2POutbound extends ChannelOutboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(TomP2POutbound.class);
    private final boolean preferDirect;
    private final Encoder encoder;
    private final CompByteBufAllocator alloc;
    
    public TomP2POutbound(boolean preferDirect, SignatureFactory signatureFactory) {
    	this(preferDirect, signatureFactory, new CompByteBufAllocator());
    }

    public TomP2POutbound(boolean preferDirect, SignatureFactory signatureFactory, CompByteBufAllocator alloc) {
        this.preferDirect = preferDirect;
        this.encoder = new Encoder(signatureFactory);
        this.alloc = alloc;
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise)
            throws Exception {
        AlternativeCompositeByteBuf buf = null;
        try {
            boolean done = false;
            if (msg instanceof Message) {
                Message message = (Message) msg;
                
                if (preferDirect) {
                    buf = alloc.compDirectBuffer(); 
                } else {
                    buf = alloc.compBuffer(); 
                }
                //null means create signature
                done = encoder.write(buf, message, null);
            } else {
                ctx.write(msg, promise);
                return;
            }
            
            Message message = encoder.message();

            if (buf.isReadable()) {
                // this will release the buffer
                if (ctx.channel() instanceof DatagramChannel) {
                	
                	final InetSocketAddress recipient;
                	final InetSocketAddress sender;
                    if (message.senderSocket() == null) {
                    	//in case of a request
                    	if(message.recipientRelay()!=null) {
                    		//in case of sending to a relay (the relayed flag is already set)
                    		recipient = message.recipientRelay().createSocketUDP();
                    	} else {
                    		recipient = message.recipient().createSocketUDP();
                    	}
                    	sender = message.sender().createSocketUDP();
                    } else {
                    	//in case of a reply
                    	recipient = message.senderSocket();
                    	sender = message.recipientSocket();
                    }
                    DatagramPacket d = new DatagramPacket(buf, recipient, sender);
                    LOG.debug("Send UDP message {}, datagram: {}.", message, d);
                    ctx.writeAndFlush(d, promise);
                    
                } else {
                    LOG.debug("Send TCP message {} to {}.", message, message.senderSocket());
                    ctx.writeAndFlush(buf, promise);
                }
                if (done) {
                    message.setDone(true);
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
            LOG.error("Exception in encoding when starting.", cause);
            cause.printStackTrace();
        } else if (encoder.message() != null && !encoder.message().isDone()) {
            LOG.error("Exception in encoding when started.", cause);
            cause.printStackTrace();
        }
    }
}
