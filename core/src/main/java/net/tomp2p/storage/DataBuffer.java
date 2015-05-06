package net.tomp2p.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataBuffer {

	private final List<ByteBuf> buffers;

	private int alreadyTransferred = 0;

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
		synchronized (dataBuffer.buffers) {
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
		final DataBuffer db;
		synchronized (buffers) {
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
		final DataBuffer copy = shallowCopy();
		final List<ByteBuffer> nioBuffers = new ArrayList<ByteBuffer>(
				copy.buffers.size());
		for (final ByteBuf buf : copy.buffers) {
			for (final ByteBuffer bb : buf.nioBuffers()) {
				nioBuffers.add(bb);
			}
		}
		return nioBuffers;
	}
	
	/**
	 * @return The length of the data that is backed by the data buffer
	 */
	public int length() {
		int length = 0;
		final DataBuffer copy = shallowCopy();
		for (final ByteBuf buffer : copy.buffers) {
			length += buffer.writerIndex();
		}
		return length;
	}

	/**
	 * @return The wrapped ByteBuf backed by the buffers stored in here. The buffer is
	 *         not deep copied here.
	 */
	public ByteBuf toByteBuf() {
		final DataBuffer copy = shallowCopy();
		return Unpooled.wrappedBuffer(copy.buffers.toArray(new ByteBuf[0]));
	}
	
	/**
	 * @return The ByteBuf arrays backed by the buffers stored in here. The buffer is
	 *         not deep copied here.
	 */
	public ByteBuf[] toByteBufs() {
		final DataBuffer copy = shallowCopy();
		return copy.buffers.toArray(new ByteBuf[0]);
	}

	/**
	 * Transfers the data from this buffer the CompositeByteBuf.
	 * 
	 * @param buf
	 *            The AlternativeCompositeByteBuf, where the data from this buffer is
	 *            transfered to
	 */
	public void transferTo(final AlternativeCompositeByteBuf buf) {
		final DataBuffer copy = shallowCopy();
		for (final ByteBuf buffer : copy.buffers) {
			buf.addComponent(buffer);
			alreadyTransferred += buffer.readableBytes();
		}
	}

	public int transferFrom(final ByteBuf buf, final int remaining) {
		final int readable = buf.readableBytes();
		final int index = buf.readerIndex();
		final int length = Math.min(remaining, readable);

		if (length == 0) {
			return 0;
		}

		if (buf instanceof AlternativeCompositeByteBuf) {
			final List<ByteBuf> decoms = ((AlternativeCompositeByteBuf) buf)
					.decompose(index, length);

			for (final ByteBuf decom : decoms) {
				if(decom.isReadable()) {
					synchronized (buffers) {
						// this is already a slice
						buffers.add(decom);
					}
					decom.retain();
				}
			}

		} else {
			final ByteBuf slice = buf.slice(index, length);
			if(slice.isReadable()) {
				synchronized (buffers) {
					buffers.add(slice);
				}
				slice.retain();
			}
		}

		alreadyTransferred += length;
		buf.readerIndex(buf.readerIndex() + length);
		return length;
	}

	public int alreadyTransferred() {
		return alreadyTransferred;
	}

	public void resetAlreadyTransferred() {
		alreadyTransferred = 0;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(bytes());
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
		return Arrays.equals(m.bytes(), bytes());
	}

	@Override
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
		synchronized (buffers) {
			for (ByteBuf buf : buffers) {
				buf.release();
			}
			buffers.clear();
		}
		return this;
	}

	public byte[] bytes() {
		final List<ByteBuffer> bufs = bufferList();
		int size = 0;
		for (ByteBuffer buf:bufs) {
			size += buf.remaining();
		}

		final byte[] retVal = new byte[size];
		int offset = 0;
		for (ByteBuffer bb:bufs) {
			final int length = bb.remaining();
			bb.get(retVal, offset, length);
			offset += length;
		}
		return retVal;
	}
}
