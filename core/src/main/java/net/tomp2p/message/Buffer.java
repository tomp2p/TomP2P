package net.tomp2p.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

import java.io.IOException;

import net.tomp2p.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Buffer {
    
    private static final Logger LOG = LoggerFactory.getLogger(Buffer.class);
    
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
            slice.retain();
            cbb.addComponent(slice);
            cbb.writerIndex(cbb.writerIndex() + slice.readableBytes());
        } else {
            buffer.writeBytes(slice);
            LOG.debug("buffer copied. You can use a CompositeByteBuf");
        }
        return this;
    }
    
    public Object object() throws ClassNotFoundException, IOException {
        return Utils.decodeJavaObject(buffer.duplicate().readerIndex(0));
    }
    
    @Override
    protected void finalize() throws Throwable {
   		buffer.release();       
    }
    
    @Override
    public int hashCode() {
        return buffer.duplicate().readerIndex(0).hashCode() ^ length;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof Buffer)) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        final Buffer b = (Buffer) obj;
        if(b.length != length) {
            return false;
        }
        return b.buffer.duplicate().readerIndex(0).equals(buffer.duplicate().readerIndex(0));
    }

	public void reset() {
		read=0;
		buffer.resetReaderIndex();
	}
}
