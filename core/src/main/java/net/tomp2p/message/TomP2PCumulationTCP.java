package net.tomp2p.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderException;

import java.net.InetSocketAddress;

import net.tomp2p.connection.SignatureFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TomP2PCumulationTCP extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(TomP2PCumulationTCP.class);

    private final Decoder decoder;
    private ByteBuf cumulation = null;

    private int lastId = 0;

    public TomP2PCumulationTCP(final SignatureFactory signatureFactory) {
        decoder = new Decoder(signatureFactory);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {

        if (!(msg instanceof ByteBuf)) {
            ctx.fireChannelRead(msg);
            return;
        }

        final ByteBuf buf = (ByteBuf) msg;
        final InetSocketAddress sender = (InetSocketAddress) ctx.channel().remoteAddress();

        try {
            if (cumulation == null) {
                //cumulation = buf;
                //TODO: changing this to directBuffer or uncommenting the above line, causes a StreamCorruption. I'm not sure where the direct buffer gets overwritten
                cumulation = Unpooled.buffer(buf.readableBytes());
                cumulation.writeBytes(buf);
                buf.release();
                //end of TODO
                try {
                    decoding(ctx, sender);
                } catch (Throwable t) {
                    t.printStackTrace();
                } finally {
                    if (!cumulation.isReadable()) {
                        cumulation.release();
                        cumulation = null;
                    }
                }
            } else {
                try {
                    if (cumulation.writerIndex() > cumulation.maxCapacity() - buf.readableBytes()) {
                        ByteBuf oldCumulation = cumulation;
                        //TODO: direct buffer get somewhere overwritten
                        //cumulation = ctx.alloc().buffer(oldCumulation.readableBytes() + buf.readableBytes());
                        cumulation = Unpooled.buffer(oldCumulation.readableBytes() + buf.readableBytes());
                        //end of TODO
                        cumulation.writeBytes(oldCumulation);
                        oldCumulation.release();
                    }
                    cumulation.writeBytes(buf);
                    decoding(ctx, sender);
                } finally {
                    if (!cumulation.isReadable()) {
                        cumulation.release();
                        cumulation = null;
                    } else {
                        // TODO: not sure if we can use discard here if we want to keep data in the Data object
                        //cumulation.discardSomeReadBytes();
                    }
                    buf.release();
                }
            }
        } catch (Throwable t) {
            throw new DecoderException(t);
        }
    }

    private void decoding(final ChannelHandlerContext ctx, final InetSocketAddress sender) {
        boolean finished = true;
        boolean moreData = true;
        while (finished && moreData) {
            finished = decoder.decode(ctx, cumulation, (InetSocketAddress) ctx.channel().localAddress(),
                    sender);
            if (finished) {
                lastId = decoder.message().getMessageId();
                moreData = cumulation.readableBytes() > 0;
                ctx.fireChannelRead(decoder.prepareFinish());
            } else {
                // this id was the same as the last and the last message already finished the parsing. So this message
                // is finished as well although it may send only partial data.
                if (lastId == decoder.message().getMessageId()) {
                    finished = true;
                    moreData = cumulation.readableBytes() > 0;
                    ctx.fireChannelRead(decoder.prepareFinish());
                } else if (decoder.message().isStreaming()) {
                    ctx.fireChannelRead(decoder.message());
                }
            }
        }
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        final InetSocketAddress sender = (InetSocketAddress) ctx.channel().remoteAddress();
        try {
            if (cumulation != null) {
                decoding(ctx, sender);
            }
        } catch (Throwable t) {
            throw new DecoderException(t);
        } finally {
            if (cumulation != null) {
                cumulation.release();
                cumulation = null;
            }
            ctx.fireChannelInactive();
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
        Message msg = decoder.message();
        // don't use getLocalizedMessage() -
        // http://stackoverflow.com/questions/8699521/any-way-to-ignore-only-connection-reset-by-peer-ioexceptions
        if (cause.getMessage().equals("Connection reset by peer")) {
            return; // ignore
        } else if(cause.getMessage().equals("An existing connection was forcibly closed by the remote host")){
        	//with windows we see the following message
        	return; // ignore
        }
        if (msg == null && decoder.lastContent() == null) {
            LOG.error("exception in decoding TCP, not started decoding", cause);
        } else if (msg != null && !msg.isDone()) {
            LOG.error("exception in decoding TCP, decoding started", cause);
        }
    }
}
