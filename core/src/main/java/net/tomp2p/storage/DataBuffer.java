package net.tomp2p.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.util.ArrayList;
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
		final ByteBuf buf = Unpooled.wrappedBuffer(buffer, offset, length);
		buffers.add(buf);
		// no need to retain, as we initialized here and ref counter is set to 1
	}

	/**
	 * Creates a DataBuffer and adds the ByteBuf to this DataBuffer
	 * 
	 * @param buf
	 *            The ByteBuf is only added, but no retain() is called!
	 */
	public DataBuffer(final ByteBuf buf) {
		ByteBuf slice = Unpooled.wrappedBuffer(buf);
		buffers = new ArrayList<ByteBuf>(1);
		buffers.add(slice);
	}

	private DataBuffer(final List<ByteBuf> buffers) {
		this.buffers = new ArrayList<ByteBuf>(buffers.size());
		for (final ByteBuf buf : buffers) {
			this.buffers.add(buf.duplicate());
			buf.retain();
		}
	}
	
	public DataBuffer add(DataBuffer dataBuffer) {
		synchronized (buffers) {
			buffers.addAll(dataBuffer.buffers);
		}
		return this;
    }

	// from here, work with shallow copies
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
	 * @return The ByteBuffers backed by the buffers stored in here. The buffer
	 *         is not deep copied here.
	 */
	public ByteBuffer[] toByteBuffer() {
		return toByteBuf().nioBuffers();
	}

	/**
	 * Transfers the data from this buffer the CompositeByteBuf.
	 * 
	 * @param buf
	 *            The CompositeByteBuf, where the data from this buffer is
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
				synchronized (buffers) {
					// this is already a slice
					buffers.add(decom);
				}
				decom.retain();
			}

		} else {
			synchronized (buffers) {
				buffers.add(buf.slice(index, length));
			}
			buf.retain();
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
		return m.toByteBuf().equals(toByteBuf());
	}

	public byte[] bytes() {
		final ByteBuffer[] bufs = toByteBuffer();
		final int bufLength = bufs.length;
		int size = 0;
		for (int i = 0; i < bufLength; i++) {
			size += bufs[i].remaining();
		}

		byte[] retVal = new byte[size];
		for (int i = 0, offset = 0; i < bufLength; i++) {
			final int remaining = bufs[i].remaining();
			bufs[i].get(retVal, offset, remaining);
			offset += remaining;
		}
		return retVal;
	}
}
