package net.tomp2p.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.tomp2p.utils.Utils;

public class DataBuffer {

	private final Object lock = new Object();
	private final List<ByteBuf> buffers;
	private byte[] heapBuffer = null;

	public DataBuffer() {
		this(1);
	}
	
	public DataBuffer(int nrArrays) {
		buffers = new ArrayList<ByteBuf>(nrArrays);
	}
	
	public DataBuffer(final byte[] buffer) {
		this(buffer, 0, buffer.length);
	}

	public DataBuffer(final byte[] buffer, final int offset, final int length) {
		buffers = new ArrayList<ByteBuf>(1);
		if(length > 0) {
			final ByteBuf buf = Unpooled.wrappedBuffer(buffer, offset, length);
			buffers.add(buf);
			// no need to retain, as we initialized here and ref counter is set to 1
		}
	}

	/**
	 * Creates a DataBuffer and adds the ByteBuf to this DataBuffer
	 * 
	 * @param buf
	 *            The ByteBuf to add
	 */
	public DataBuffer(final ByteBuf buf) {
		buffers = new ArrayList<ByteBuf>(1);
		if(buf.isReadable()) {
			buffers.add(buf.slice());
			buf.retain();
		}
	}

	private DataBuffer(final List<ByteBuf> buffers) {
		this.buffers = new ArrayList<ByteBuf>(buffers.size());
		for (final ByteBuf buf : buffers) {
			if(buf.isReadable()) {
				this.buffers.add(buf.duplicate());
				buf.retain();
			}
		}
	}
	
	public DataBuffer add(DataBuffer dataBuffer) {
		if(isHeapBuffer()) {
			throw new RuntimeException("Already a heap buffer, cannot transfer data to it!");
		}
		synchronized (dataBuffer.lock) {
			for (final ByteBuf buf : dataBuffer.buffers) {
				if(buf.isReadable()) {
					this.buffers.add(buf.duplicate());
					buf.retain();
				}
			}
		}
		return this;
    }

	/**
	 * From here, work with shallow copies.
	 * @return Shallow copy of this DataBuffer.
	 */
	public DataBuffer shallowCopy() {
		if(isHeapBuffer()) {
			throw new RuntimeException("Already a heap buffer, cannot transfer data to it!");
		}
		final DataBuffer db;
		synchronized (lock) {
			db = new DataBuffer(buffers);
		}
		return db;
	}

	/**
	 * Always make a copy with shallowCopy before using the buffer directly.
	 * This buffer is not thread safe!
	 * 
	 * @return The backing list of byte buffers
	 */
	public List<ByteBuffer> bufferList() {
		final List<ByteBuffer> nioBuffers;
		if(isHeapBuffer()) {
			nioBuffers = new ArrayList<ByteBuffer>(1);
			nioBuffers.add(ByteBuffer.wrap(heapBuffer));
		} else {
			final DataBuffer copy = shallowCopy();
			nioBuffers = new ArrayList<ByteBuffer>(copy.buffers.size());
			for (final ByteBuf buf : copy.buffers) {
				for (final ByteBuffer bb : buf.nioBuffers()) {
					nioBuffers.add(bb);
				}
			}
		}	
		return nioBuffers;
	}
	
	/**
	 * @return The length of the data that is backed by the data buffer
	 */
	public int length() {
		if(isHeapBuffer()) {
			return heapBuffer.length;
		} else {
			int length = 0;
			final DataBuffer copy = shallowCopy();
			for (final ByteBuf buffer : copy.buffers) {
				length += buffer.writerIndex();
			}
			return length;
		}
	}

	/**
	 * @return The wrapped ByteBuf backed by the buffers stored in here. The buffer is
	 *         not deep copied here.
	 */
	public ByteBuf toByteBuf() {
		if(isHeapBuffer()) {
			return Unpooled.wrappedBuffer(heapBuffer);
		} else {
			final DataBuffer copy = shallowCopy();
			return Unpooled.wrappedBuffer(copy.buffers.toArray(new ByteBuf[0]));
		}
	}
	
	/**
	 * @return The ByteBuf arrays backed by the buffers stored in here. The buffer is
	 *         not deep copied here.
	 */
	public ByteBuf[] toByteBufs() {
		if(isHeapBuffer()) {
			return new ByteBuf[]{toByteBuf()};
		} else {
			final DataBuffer copy = shallowCopy();
			return copy.buffers.toArray(new ByteBuf[0]);
		}
	}

	/**
	 * Transfers the data from this buffer the CompositeByteBuf.
	 * 
	 * @param buf
	 *            The AlternativeCompositeByteBuf, where the data from this buffer is
	 *            transfered to
	 */
	public int transferTo(final AlternativeCompositeByteBuf buf) {
		if(isHeapBuffer()) {
			throw new RuntimeException("Already a heap buffer, cannot transfer data to it!");
		}
		final DataBuffer copy = shallowCopy();
		int transferred = 0;
		for (final ByteBuf buffer : copy.buffers) {
			buf.addComponent(buffer);
			transferred += buffer.readableBytes();
		}
		return transferred;
	}

	public int transferFrom(final ByteBuf buf, final int max) {
		if(isHeapBuffer()) {
			throw new RuntimeException("Already a heap buffer, cannot transfer data to it!");
		}
		final int readable = buf.readableBytes();
		final int index = buf.readerIndex();
		final int length = Math.min(max, readable);

		if (length == 0) {
			return 0;
		}

		int transferred = 0;
		if (buf instanceof AlternativeCompositeByteBuf) {
			final List<ByteBuf> decoms = ((AlternativeCompositeByteBuf) buf)
					.decompose(index, length);

			for (final ByteBuf decom : decoms) {
				if(decom.isReadable()) {
					synchronized (lock) {
						// this is already a slice
						buffers.add(decom);
					}
					transferred += decom.readableBytes();
					decom.retain();
				}
			}

		} else {
			final ByteBuf slice = buf.slice(index, length);
			if(slice.isReadable()) {
				synchronized (lock) {
					buffers.add(slice);
				}
				transferred += slice.readableBytes();
				slice.retain();
			}
		}

		buf.skipBytes(length);
		return transferred;
	}

	@Override
	public int hashCode() {
		if(isHeapBuffer()) {
			return Arrays.hashCode(heapBuffer);
		} else {
			int hash = 42;
			for(ByteBuf buf:buffers) {
				hash ^= buf.hashCode();
			}
			return hash;
		}
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
		if(isHeapBuffer() && m.isHeapBuffer()) {
			return Arrays.equals(m.bytes(), bytes());
		} else if(!isHeapBuffer() && !m.isHeapBuffer()) {
			return Utils.isSameSets(buffers, m.buffers);
		} else {
			throw new RuntimeException("cannot compare head with direct");
		}
	}

	/*@Override
	protected void finalize() throws Throwable {
		// work on the original buffer, no worries about threading as we are the
		// finalizer
		try {
			for (ByteBuf buf : buffers) {
				buf.release();
			}
		} catch (Throwable t) {
			throw t;
		} finally {
			super.finalize();
		}
	}*/
	
	/**
	 * If you plan to use PooledByteBufAlloc, then its important to release the
	 * buffer once its not used anymore. Currently the ByteBuf lives with the
	 * DataBuffer object, and when the DataBuffer object is garbage collected,
	 * then the buffer is released. This does not work well with
	 * PooledByteBufAlloc, however, it works with UnpooledByteBufAlloc, which is
	 * the default. The PooledByteBufAlloc, when running low on memory cannot
	 * force the garbage collection to happen, thus, there will be frequent
	 * OutOfMemoryExceptions when using pooled buffers. Thus, the buffer needs
	 * to be released manually with the following method.
	 * 
	 * @return The data buffer
	 */
	public DataBuffer release() {
		synchronized (lock) {
			releaseIntern();
		}
		return this;
	}
	
	private DataBuffer releaseIntern() {
		boolean allReleased = true;
		for (ByteBuf buf : buffers) {
			if(!buf.release() && allReleased) {
				allReleased = false;
			}
		}
		if(allReleased) {
			buffers.clear();
		}
		return this;
	}
	
	public boolean isHeapBuffer() {
		synchronized (lock) {
			return heapBuffer != null && buffers.size() == 0;
        }
	}

	/**
	 * Converts the ByteBuf (most likely a direct buffer) to a heap buffer. Once
	 * its converted it most of the methods for manipulating the data in this
	 * class will not work
	 */
	public byte[] bytes() {
		final List<ByteBuffer> bufs = bufferList();
		int size = 0;
		for (ByteBuffer buf : bufs) {
			size += buf.remaining();
		}
		synchronized (lock) {
			if (isHeapBuffer()) {
				return heapBuffer;
			}
			
			heapBuffer = new byte[size];
			int offset = 0;
			for (ByteBuffer bb : bufs) {
				final int length = bb.remaining();
				bb.get(heapBuffer, offset, length);
				offset += length;
			}
			releaseIntern();
			return heapBuffer;
		}
	}
	
	public Object lockObject() {
		return lock;
	}

	public String refcnt() {
		ByteBuf b = buffers.get(0);
	    if(b != null) {
	    	return ""+b.refCnt();
	    			
	    }
	    return "";
    }
}
