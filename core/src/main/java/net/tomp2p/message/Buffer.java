package net.tomp2p.message;

import java.io.IOException;

import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

public class Buffer {
    
    private static final Logger LOG = LoggerFactory.getLogger(Buffer.class);
    
    //TODO: use ByteBuffer
    private final ByteBuf buffer;
    private final int length;

    private int read = 0;

    public Buffer(final ByteBuf buffer, final int length) {
        this.buffer = buffer;
        this.length = length;
    }

    public Buffer(ByteBuf buffer) {
        this.buffer = buffer;
        this.length = buffer.readableBytes();
    }

    public int length() {
        return length;
    }

    public ByteBuf buffer() {
        return buffer;
    }

    public int readable() {
        int remaining = length - read;
        int available = buffer.readableBytes();
        return Math.min(remaining, available);
    }
    
    public boolean isComplete() {
        return length == buffer.readableBytes();
    }

    public int incRead(final int read) {
        this.read += read;
        return this.read;
    }

    public boolean done() {
        return this.read == length;
    }

    public int alreadyRead() {
        return read;
    }

    public Buffer addComponent(final ByteBuf slice) {
        if (buffer instanceof CompositeByteBuf) {
            CompositeByteBuf cbb = (CompositeByteBuf) buffer;
            //make a copy for the user, otherwise we may leak a pooled byte buffer
            cbb.addComponent(Unpooled.copiedBuffer(slice));
            cbb.writerIndex(cbb.writerIndex() + slice.readableBytes());
        } else {
            buffer.writeBytes(slice);
            LOG.debug("buffer copied. You can use a CompositeByteBuf");
        }
        return this;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof Buffer)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        Buffer b = (Buffer) obj;
        return b.buffer.duplicate().readerIndex(0).equals(buffer.duplicate().readerIndex(0));
    }

    public Object object() throws ClassNotFoundException, IOException {
        return Utils.decodeJavaObject(buffer.duplicate().readerIndex(0));
    }
}
