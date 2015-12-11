package net.tomp2p.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataBuffer {

	private final Object lock = new Object();
	
	private final List<ByteBuf> buffers;
	private volatile byte[] heapBuffer;

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

	private DataBuffer(final List<ByteBuf> buffers, boolean retain) {
		this.buffers = new ArrayList<ByteBuf>(buffers.size());
		for (final ByteBuf buf : buffers) {
			if(buf.isReadable()) {
				this.buffers.add(buf.duplicate());
				if(retain) {
					buf.retain();
				}
			}
		}
	}
	
	public DataBuffer append(final List<ByteBuf> buffers) {
		synchronized (lock) {
			for (final ByteBuf buf : buffers) {
				if(buf.isReadable()) {
					this.buffers.add(buf.duplicate());
					buf.retain();
				}
			}
		}
		return this;
	}
	
	public DataBuffer append(byte[] array, int offset, int length) {
		synchronized (lock) {
			buffers.add(Unpooled.wrappedBuffer(array, offset, length));
		}
		return this;
	}
	
	/**
	 * Shallow copy, needs to be released!
	 * @return
	 */
	public DataBuffer shallowCopy() {
		if(isHeapBuffer()) {
			throw new RuntimeException("This is now a heapbuffer, cannot copy");
		}
		synchronized (lock) {
			return new DataBuffer(buffers, true);
		}
	}

	/**
	 * From here, work with shallow copies.
	 * @return Shallow copy of this DataBuffer.
	 */
	public DataBuffer shallowCopyIntern() {
		if(isHeapBuffer()) {
			throw new RuntimeException("This is now a heapbuffer, cannot copy");
		}
		synchronized (lock) {
			return new DataBuffer(buffers, false);
		}
	}
	
	public List<ByteBuf> bufListIntern() {
		return shallowCopyIntern().buffers;
	}

	/**
	 * @return The length of the data that is backed by the data buffer
	 */
	public int length() {
		if(isHeapBuffer()) {
			return heapBuffer.length;
		} else {
			int length = 0;
			final DataBuffer copy = shallowCopyIntern();
			for (final ByteBuf buffer : copy.buffers) {
				length += buffer.writerIndex();
			}
			return length;
		}
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
			final DataBuffer copy = shallowCopyIntern();
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
	 * @return The wrapped ByteBuf backed by the buffers stored in here. The buffer is
	 *         not deep copied here.
	 */
	public ByteBuf toByteBuf() {
		if(isHeapBuffer()) {
			return Unpooled.wrappedBuffer(heapBuffer);
		} else {
			final DataBuffer copy = shallowCopyIntern();
			//wrap does a slice, so a derived buffer, not increasing ref count
			return Unpooled.wrappedBuffer(copy.buffers.toArray(new ByteBuf[copy.buffers.size()]));
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
			throw new RuntimeException("This is now a heapbuffer, cannot transfer");
		}
		final DataBuffer copy = shallowCopyIntern();
		int transferred = 0;
		for (final ByteBuf buffer : copy.buffers) {
			buf.addComponent(buffer);
			transferred += buffer.readableBytes();
		}
		return transferred;
	}

	public int transferFrom(final ByteBuf buf, final int max) {
		if(isHeapBuffer()) {
			throw new RuntimeException("This is now a heapbuffer, cannot transfer");
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
			synchronized (lock) {
				for(ByteBuf buf:buffers) {
					hash ^= buf.hashCode();
				}
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
			return Arrays.equals(heapBuffer, m.heapBuffer);
		} else if(!isHeapBuffer() && !m.isHeapBuffer()) {
			return ByteBufUtil.equals(toByteBuf(), m.toByteBuf());
		} else {
			throw new RuntimeException("Cannot compare, as DataBuffer is of mixed type: head and direct buffer");
		}
	}
	
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
			for (ByteBuf buf : buffers) {
				buf.release();
			}
			buffers.clear();
		}
		return this;
	}

	/**
	 * Converts the ByteBuf (most likely a direct buffer) to a heap buffer. Once
	 * its converted it most of the methods for manipulating the data in this
	 * class will not work
	 */
	public byte[] convertToHeapBuffer() {
		if(heapBuffer != null) {
			return heapBuffer;
		}
		final byte[] heapBuffer;
		synchronized (lock) {
			int length = 0;
			for (final ByteBuf buffer : buffers) {
				length += buffer.writerIndex();
			}
			heapBuffer = new byte[length];
			int offset = 0;
			for (final ByteBuf buffer : buffers) {
				final int len = buffer.readableBytes();
				buffer.readBytes(heapBuffer, offset, len);
				offset += len;
			}
		
			//release
			for (ByteBuf buf : buffers) {
				buf.release();
			}
			buffers.clear();
			
			//create new
			this.heapBuffer = heapBuffer;
			return heapBuffer;
		}
	}

	public boolean isHeapBuffer() {
		synchronized (lock) {
			return heapBuffer != null && buffers.size() == 0;
		}
	}
	
	public byte[] heapBuffer() {
		return heapBuffer;
	}

	
}
