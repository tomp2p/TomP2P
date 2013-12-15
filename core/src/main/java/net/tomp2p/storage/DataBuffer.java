package net.tomp2p.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DataBuffer {

    private final List<ByteBuffer> buffers;

    private int alreadyTransferred = 0;
    private int bufferSize = 0;

    public DataBuffer(final byte[] buffer) {
        buffers = new ArrayList<ByteBuffer>(1);
        buffers.add(ByteBuffer.wrap(buffer));
    }

    public DataBuffer(final ByteBuf buf) {
        final ByteBuffer[] byteBuffers = buf.nioBuffers();
        final int len = byteBuffers.length;
        if (len < 1) {
            throw new IllegalArgumentException("cannot convert this netty buffer");
        }
        buffers = new ArrayList<ByteBuffer>(len);
        for (int i = 0; i < len; i++) {
            buffers.add(byteBuffers[i]);
        }
    }

    public DataBuffer() {
        buffers = new ArrayList<ByteBuffer>(1);
    }

    private DataBuffer(final List<ByteBuffer> buffers) {
        final int len = buffers.size();
        this.buffers = new ArrayList<ByteBuffer>(len);
        for(ByteBuffer buffer : buffers) {
            ByteBuffer buffer2 = buffer.duplicate();
            this.buffers.add(buffer2);
        }
    }

    /**
     * Always make a copy with shallowCopy before using the buffer directly. This buffer is not thread safe!
     * 
     * @return The backing list of byte buffers
     */
    public List<ByteBuffer> bufferList() {
        return buffers;
    }

    // from here, work with shallow copies
    public DataBuffer shallowCopy() {
        synchronized (buffers) {
            DataBuffer db = new DataBuffer(buffers);
            return db;
        }
    }

    public ByteBuf toByteBuffer() {
        synchronized (buffers) {
            return Unpooled.copiedBuffer(shallowCopy().buffers.toArray(new ByteBuffer[0]));
        }
    }

    /**
     * Transfers the data from this buffer the CompositeByteBuf.
     * 
     * @param buf
     *            The CompositeByteBuf, where the data from this buffer is trasfered to
     */
    public void transferTo(final CompositeByteBuf buf) {
        
        //check if we have something stored in the last one, if not, get rid of it.
        final int lastIndex = buf.numComponents() - 1;
        ByteBuf last = null;
        if(lastIndex>=0) {
            last = buf.component(lastIndex);
            if(!last.isReadable()) {
                buf.removeComponent(lastIndex);
            } else {
            // set the capacity of the last buffer, otherwise it will be filled with 0
                last.capacity(buf.writerIndex());
                // create new last to be appended as we otherwise may overwrite the ByteBuffers
                last = Unpooled.buffer(0, Integer.MAX_VALUE);
            }
        } else {
            last = Unpooled.buffer(0, Integer.MAX_VALUE);
        }
        
        // TODO: add component only if we have enough bytes, otherwise copy.
        final DataBuffer copy = shallowCopy();
        for (ByteBuffer buffer : copy.bufferList()) {
            //buf.addComponent(Unpooled.wrappedBuffer(buffer));
            buf.addComponent(Unpooled.copiedBuffer(buffer));
            int size = buffer.remaining();
            buf.writerIndex(buf.writerIndex() + size);
            alreadyTransferred += size;
            /*buffer.rewind();
            int read  = buffer.capacity();
            //buffer.limit(read);
            byte me[] = new byte[5];
            buffer.get(me);
            System.err.println("send array ("+System.identityHashCode(buffer)+"): "+Arrays.toString(me));*/
        }
        buf.addComponent(last);
    }

    public int transferFrom(final ByteBuf buf, final int remaining) {
        final int readable = buf.readableBytes();
        final int index = buf.readerIndex();
        final int length = Math.min(remaining, readable);
        //final ByteBuffer[] byteBuffers = buf.nioBuffers(index, length);
        final ByteBuffer[] byteBuffers = buf.nioBuffers();
        final int len = byteBuffers.length;
        if (len < 1) {
            throw new IllegalArgumentException("Buffer count must >= 1.");
        }
        for (int i = 0; i < len ; i++) {
            ByteBuffer byteBuffer = byteBuffers[i];
            byteBuffer.limit(length);
            synchronized (buffers) {
                buffers.add(byteBuffer);
            }
            
            /*byte me[] = new byte[Math.min(5, length)];
            byteBuffer.get(me);
            byteBuffer.rewind();
            System.err.print("make array ("+System.identityHashCode(byteBuffer)+"): "+Arrays.toString(me));
            if(buffers.size()==1 && me[0] != -84) {
                System.err.println("WTF?");
            }*/
            
        }
        alreadyTransferred += length;
        buf.readerIndex(buf.readerIndex() + length);
        return length;
    }

    public int addBuf(final ByteBuf buf) {
        final ByteBuffer[] byteBuffers = buf.nioBuffers();
        final int len = byteBuffers.length;
        int currentSize = 0;
        for (int i = 0; i < len; i++) {
            ByteBuffer buffer = byteBuffers[i];
            int pos = buffer.position();
            int size = buffer.limit() - pos;
            currentSize += size;
            synchronized (buffers) {
                buffers.add(buffer);
            }
        }
        buf.readerIndex(buf.readerIndex() + currentSize);
        bufferSize += currentSize;
        return currentSize;
    }

    public int alreadyTransferred() {
        return alreadyTransferred;
    }

    public int bufferSize() {
        return bufferSize;
    }

    @Override
    public int hashCode() {
        //This is a slow operation, use with care!
        return toByteBuffer().hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof DataBuffer)) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        final DataBuffer m = (DataBuffer) obj;
        //This is a slow operation, use with care!
        return m.toByteBuffer().equals(toByteBuffer());
    }
}
