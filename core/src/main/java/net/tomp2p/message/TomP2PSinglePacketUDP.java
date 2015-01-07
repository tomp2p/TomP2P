package net.tomp2p.message;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.socket.DatagramPacket;

import java.net.InetSocketAddress;

import net.tomp2p.connection.SignatureFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Sharable
public class TomP2PSinglePacketUDP extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(TomP2PSinglePacketUDP.class);

    private final SignatureFactory signatureFactory;
    
    public TomP2PSinglePacketUDP(final SignatureFactory signatureFactory) {
        this.signatureFactory = signatureFactory;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        
        if (!(msg instanceof DatagramPacket)) {
            ctx.fireChannelRead(msg);
            return;
        }

        final DatagramPacket d = (DatagramPacket) msg;
        final ByteBuf buf = d.content();
        final InetSocketAddress sender = d.sender();
        final InetSocketAddress recipient = d.recipient();

        try {
            Decoder decoder = new Decoder(signatureFactory);
            boolean finished = decoder.decode(ctx, buf, recipient, sender);
            if (finished) {
                ctx.fireChannelRead(decoder.prepareFinish());
            } else {
                LOG.warn("Did not get the complete packet!");
            }
        } catch (Throwable t) {
        	LOG.error("Error in UDP decoding", t);
            throw t;
        } finally {
            buf.release();
        }
    }

    //@Override
    //public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
        /*Message2 msg = decoder.message();
        if (msg == null && decoder.lastContent() == null) {
            LOG.error("exception in decoding UDP, not started decoding", cause);
            cause.printStackTrace();
        } else if (msg != null && !msg.isDone()) {
            LOG.error("exception in decoding UDP, decoding started", cause);
            cause.printStackTrace();
        }*/
        //cause.printStackTrace();
    //}
}
