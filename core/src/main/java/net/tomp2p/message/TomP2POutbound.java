package net.tomp2p.message;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import net.tomp2p.connection.SignatureFactory;
import net.tomp2p.storage.AlternativeCompositeByteBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TomP2POutbound extends ChannelOutboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(TomP2POutbound.class);
    private final Encoder encoder;
    private final ByteBufAllocator byteBufAllocator;

    public TomP2POutbound(SignatureFactory signatureFactory, ByteBufAllocator byteBufAllocator) {
        this.encoder = new Encoder(signatureFactory);
        this.byteBufAllocator = byteBufAllocator;
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise)
            throws Exception {
        AlternativeCompositeByteBuf buf = null;
        if (!(msg instanceof Message)) {
    		ctx.write(msg, promise);
            return;
    	}
        boolean ipv6 = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress() instanceof Inet6Address;
			
		try {
        	boolean done = false;
            buf = AlternativeCompositeByteBuf.compBuffer(byteBufAllocator, buf);
            
            //null, means create signature, as we did not have created one already
            done = encoder.write(buf, (Message) msg, null, ipv6);
            
            final Message message = encoder.message();

            if (buf.isReadable()) {
                // this will release the buffer
                if (ctx.channel() instanceof DatagramChannel) {
                	
                	InetSocketAddress recipient;
                	InetSocketAddress sender;
                    if (message.senderSocket() == null) {
                    	//in case of a request
                    	if(message.recipientRelay()!=null) {
                    		//in case of sending to a relay (the relayed flag is already set)
                    		recipient = ipv6 ? message.recipientRelay().ipv6Socket().createUDPSocket(): message.recipientRelay().ipv4Socket().createUDPSocket();
                    	} else if(message.recipientReflected()!=null) {
                    		//in case we use nat reflection
                    		recipient = ipv6 ? message.recipientReflected().ipv6Socket().createUDPSocket() : message.recipientReflected().ipv4Socket().createUDPSocket();
                    	} else {
                    		recipient = ipv6 ? message.recipient().ipv6Socket().createUDPSocket() : message.recipient().ipv4Socket().createUDPSocket();
                    	}
                    	sender = ipv6 ? message.sender().ipv6Socket().createSocket(0) : message.sender().ipv4Socket().createSocket(0);
                    } else {
                    	//in case of a reply
                    	recipient = message.senderSocket();
                    	sender = message.recipientSocket();
                    }
                    
                    // FIXME quickfix for Android (by Nico)
                    recipient = new InetSocketAddress(InetAddress.getByAddress(recipient.getAddress().getAddress()), recipient.getPort());
                    sender =  new InetSocketAddress(InetAddress.getByAddress(sender.getAddress().getAddress()), sender.getPort());
                    
                    DatagramPacket d = new DatagramPacket(buf, recipient, sender);
                    LOG.debug("Send UDP message {}, datagram: {}.", message, d);
                    ctx.writeAndFlush(d, promise);
                    
                } else {
                    LOG.debug("Send TCP message {} to {}.", message, message.senderSocket());
                    ctx.writeAndFlush(buf, promise);
                }
                if (done) {
                    message.setDone(true);
                }
            } else {
                buf.release();
                ctx.write(Unpooled.EMPTY_BUFFER, promise);
            }
            buf = null;

        } catch (Throwable t) {
            exceptionCaught(ctx, t);
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
    	//if exception is not propagated, we may need to wait for the timeout - check testChangeEntryProtectionKey
    	super.exceptionCaught(ctx, cause);
    }
}
