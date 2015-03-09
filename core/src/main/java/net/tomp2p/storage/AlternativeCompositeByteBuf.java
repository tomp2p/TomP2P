/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 * 
 */

package net.tomp2p.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufProcessor;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.DuplicatedByteBuf;
import io.netty.buffer.SlicedByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.CharsetUtil;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ResourceLeak;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.internal.EmptyArrays;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.StringUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Heavily inspired by CompositeByteBuf, but with a slight different behavior.
 * 
 * @author Thomas Bocek
 *
 */
public class AlternativeCompositeByteBuf extends ByteBuf {

	private static final ByteBufAllocator ALLOC = UnpooledByteBufAllocator.DEFAULT;
	private static final ByteBuffer FULL_BYTEBUFFER = (ByteBuffer) ByteBuffer
			.allocate(1).position(1);

	private final List<Component> components = new ArrayList<Component>();
	private final Component EMPTY_COMPONENT = new Component(
			Unpooled.EMPTY_BUFFER);
	private final ByteBufAllocator alloc;
	private final boolean direct;

	private int readerIndex;
	private int writerIndex;
	private int markedReaderIndex;
	private int markedWriterIndex;

	// ref counting
	private volatile int refCnt = 1;
	private static final AtomicIntegerFieldUpdater<AlternativeCompositeByteBuf> refCntUpdater = AtomicIntegerFieldUpdater
			.newUpdater(AlternativeCompositeByteBuf.class, "refCnt");
	private boolean freed;
	private final ResourceLeak leak;
	private static final ResourceLeakDetector<ByteBuf> leakDetector = new ResourceLeakDetector<ByteBuf>(
			AlternativeCompositeByteBuf.class);

	private final class Component {
		final ByteBuf buf;
		int offset;

		Component(ByteBuf buf) {
			this.buf = buf;
		}

		int endOffset() {
			return offset + buf.readableBytes();
		}
	}

	public AlternativeCompositeByteBuf(ByteBufAllocator alloc, boolean direct) {
		this.alloc = alloc;
		this.direct = direct;
		leak = leakDetector.open(this);
	}

	public AlternativeCompositeByteBuf(ByteBufAllocator alloc, boolean direct,
			ByteBuf... buffers) {
		this.alloc = alloc;
		this.direct = direct;
		addComponent(buffers);
		leak = leakDetector.open(this);
	}

	@Override
	public int refCnt() {
		return refCnt;
	}

	@Override
	public boolean release() {
		for (;;) {
			int refCnt = this.refCnt;
			if (refCnt == 0) {
				throw new IllegalReferenceCountException(0, -1);
			}

			if (refCntUpdater.compareAndSet(this, refCnt, refCnt - 1)) {
				if (refCnt == 1) {
					deallocate();
					return true;
				}
				return false;
			}
		}
	}

	@Override
	public boolean release(int decrement) {
		if (decrement <= 0) {
			throw new IllegalArgumentException("decrement: " + decrement
					+ " (expected: > 0)");
		}

		for (;;) {
			int refCnt = this.refCnt;
			if (refCnt < decrement) {
				throw new IllegalReferenceCountException(refCnt, -decrement);
			}

			if (refCntUpdater.compareAndSet(this, refCnt, refCnt - decrement)) {
				if (refCnt == decrement) {
					deallocate();
					return true;
				}
				return false;
			}
		}
	}

	@Override
	public AlternativeCompositeByteBuf retain(int increment) {
		if (increment <= 0) {
			throw new IllegalArgumentException("increment: " + increment
					+ " (expected: > 0)");
		}

		for (;;) {
			int refCnt = this.refCnt;
			if (refCnt == 0) {
				throw new IllegalReferenceCountException(0, increment);
			}
			if (refCnt > Integer.MAX_VALUE - increment) {
				throw new IllegalReferenceCountException(refCnt, increment);
			}
			if (refCntUpdater.compareAndSet(this, refCnt, refCnt + increment)) {
				break;
			}
		}
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf retain() {
		for (;;) {
			int refCnt = this.refCnt;
			if (refCnt == 0) {
				throw new IllegalReferenceCountException(0, 1);
			}
			if (refCnt == Integer.MAX_VALUE) {
				throw new IllegalReferenceCountException(Integer.MAX_VALUE, 1);
			}
			if (refCntUpdater.compareAndSet(this, refCnt, refCnt + 1)) {
				break;
			}
		}
		return this;
	}

	private void deallocate() {
		if (freed) {
			return;
		}

		freed = true;
		for (Component c : components) {
			c.buf.release();
		}
		components.clear();

		if (leak != null) {
			leak.close();
		}
	}

	private Component last() {
		if (components.isEmpty()) {
			return EMPTY_COMPONENT;
		}
		return components.get(components.size() - 1);
	}

	@Override
	public int capacity() {
		Component last = last();
		return last.offset + last.buf.capacity();
	}

	@Override
	public int maxCapacity() {
		return Integer.MAX_VALUE;
	}

	@Override
	public AlternativeCompositeByteBuf capacity(int newCapacity) {
		return capacity(newCapacity, false);
	}

	public AlternativeCompositeByteBuf capacity(int newCapacity, boolean fillBuffer) {
		if (newCapacity < 0 || newCapacity > maxCapacity()) {
			throw new IllegalArgumentException("newCapacity: " + newCapacity);
		}

		int oldCapacity = capacity();
		if (newCapacity > oldCapacity) {
			// need more storage
			final int paddingLength = newCapacity - oldCapacity;
			addComponent(fillBuffer, allocBuffer(paddingLength));
		} else if (newCapacity < oldCapacity) {
			// remove storage
			int bytesToTrim = oldCapacity - newCapacity;
			for (ListIterator<Component> i = components.listIterator(components
					.size()); i.hasPrevious();) {
				Component c = i.previous();
				if (bytesToTrim >= c.buf.capacity()) {
					bytesToTrim -= c.buf.capacity();
					c.buf.release();
					i.remove();
					continue;
				}
				Component newC = new Component(c.buf.slice(0, c.buf.capacity()
						- bytesToTrim));
				newC.offset = c.offset;
				i.set(newC);
				break;
			}
		}

		if (readerIndex() > newCapacity) {
			setIndex(newCapacity, newCapacity);
		} else if (writerIndex() > newCapacity) {
			writerIndex(newCapacity);
		}

		return this;
	}

	private ByteBuf allocBuffer(int capacity) {
		if (direct) {
			return alloc().directBuffer(capacity);
		}
		return alloc().heapBuffer(capacity);
	}

	public AlternativeCompositeByteBuf addComponent(ByteBuf... buffers) {
		return addComponent(false, buffers);
	}

	public AlternativeCompositeByteBuf addComponent(boolean fillBuffer, ByteBuf... buffers) {
		if (buffers == null) {
			throw new NullPointerException("buffers");
		}

		for (ByteBuf b : buffers) {
			if (b == null) {
				break;
			}
			//We want to use this buffer, so mark is as used
			b.retain();
			Component c = new Component(b.order(ByteOrder.BIG_ENDIAN)
					.duplicate());
			final int size = components.size();
			components.add(c);
			if (size != 0) {
				Component prev = components.get(size - 1);
				if (fillBuffer) {
					// we plan to fill the buffer
					c.offset = prev.offset + prev.buf.capacity();
				} else {
					// the buffer may not get filled
					c.offset = prev.endOffset();
				}
			}
			writerIndex0(writerIndex() + c.buf.writerIndex());
		}
		return this;
	}

	@Override
	public ByteBufAllocator alloc() {
		return alloc;
	}

	@Override
	public ByteOrder order() {
		return ByteOrder.BIG_ENDIAN;
	}

	@Override
	public AlternativeCompositeByteBuf order(ByteOrder endianness) {
		if (endianness == order()) {
			return this;
		} else {
			throw new UnsupportedOperationException("not implemented");
		}
	}

	@Override
	public boolean isDirect() {
		return direct;
	}

	@Override
	public int readerIndex() {
		return readerIndex;
	}

	@Override
	public AlternativeCompositeByteBuf readerIndex(int readerIndex) {
		if (readerIndex < 0 || readerIndex > writerIndex) {
			throw new IndexOutOfBoundsException(
					String.format(
							"readerIndex: %d (expected: 0 <= readerIndex <= writerIndex(%d))",
							readerIndex, writerIndex));
		}
		this.readerIndex = readerIndex;
		return this;
	}

	@Override
	public int writerIndex() {
		return writerIndex;
	}

	@Override
	public AlternativeCompositeByteBuf writerIndex(int writerIndex) {
		if (writerIndex < readerIndex || writerIndex > capacity()) {
			throw new IndexOutOfBoundsException(
					String.format(
							"writerIndex: %d (expected: readerIndex(%d) <= writerIndex <= capacity(%d))",
							writerIndex, readerIndex, capacity()));
		}
		setComponentWriterIndex(writerIndex);
		return this;
	}

	private AlternativeCompositeByteBuf writerIndex0(int writerIndex) {
		if (writerIndex < readerIndex || writerIndex > capacity()) {
			throw new IndexOutOfBoundsException(
					String.format(
							"writerIndex: %d (expected: readerIndex(%d) <= writerIndex <= capacity(%d))",
							writerIndex, readerIndex, capacity()));
		}
		this.writerIndex = writerIndex;
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf setIndex(int readerIndex, int writerIndex) {
		if (readerIndex < 0 || readerIndex > writerIndex
				|| writerIndex > capacity()) {
			throw new IndexOutOfBoundsException(
					String.format(
							"readerIndex: %d, writerIndex: %d (expected: 0 <= readerIndex <= writerIndex <= capacity(%d))",
							readerIndex, writerIndex, capacity()));
		}
		this.readerIndex = readerIndex;
		setComponentWriterIndex(writerIndex);
		return this;
	}
	
	
	private AlternativeCompositeByteBuf setIndex0(int readerIndex, int writerIndex) {
		if (readerIndex < 0 || readerIndex > writerIndex
				|| writerIndex > capacity()) {
			throw new IndexOutOfBoundsException(
					String.format(
							"readerIndex: %d, writerIndex: %d (expected: 0 <= readerIndex <= writerIndex <= capacity(%d))",
							readerIndex, writerIndex, capacity()));
		}
		this.readerIndex = readerIndex;
		this.writerIndex = writerIndex;
		return this;
	}

	@Override
	public int readableBytes() {
		return writerIndex - readerIndex;
	}

	@Override
	public int writableBytes() {
		return capacity() - writerIndex;
	}

	@Override
	public int maxWritableBytes() {
		return maxCapacity() - writerIndex;
	}

	@Override
	public boolean isReadable() {
		return writerIndex > readerIndex;
	}

	@Override
	public boolean isReadable(int numBytes) {
		return writerIndex - readerIndex >= numBytes;
	}

	@Override
	public boolean isWritable() {
		return capacity() > writerIndex;
	}

	@Override
	public boolean isWritable(int numBytes) {
		return capacity() - writerIndex >= numBytes;
	}

	@Override
	public ByteBuf unwrap() {
		return null;
	}

	@Override
	public AlternativeCompositeByteBuf clear() {
		readerIndex = 0;
		setComponentWriterIndex(0);
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf markReaderIndex() {
		markedReaderIndex = readerIndex;
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf resetReaderIndex() {
		readerIndex(markedReaderIndex);
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf markWriterIndex() {
		markedWriterIndex = writerIndex;
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf resetWriterIndex() {
		setComponentWriterIndex(markedWriterIndex);
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf discardReadBytes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AlternativeCompositeByteBuf discardSomeReadBytes() {
		boolean isOffsetAdjustment = false;
		int offsetAdjustment = 0;
		for(Iterator<Component> iterator = components.iterator();iterator.hasNext();) {
			Component c = iterator.next();
			
			if(isOffsetAdjustment) {
				c.offset = c.offset - offsetAdjustment;
			}
			
			if(readerIndex >= c.endOffset()) {
				c.buf.release();
				iterator.remove();
				isOffsetAdjustment = true;
				int adjust = c.endOffset() - c.offset;
				setIndex0(readerIndex - adjust, writerIndex - adjust);
				offsetAdjustment += adjust;
			} else {
				if(!isOffsetAdjustment) {
					break;
				}
			}
		}
		return this;
	}

	@Override
	public ByteBuf ensureWritable(int minWritableBytes) {
		return ensureWritable0(minWritableBytes, false);
	}

	public AlternativeCompositeByteBuf ensureWritable0(int minWritableBytes, boolean fillBuffer) {
		if (minWritableBytes < 0) {
			throw new IllegalArgumentException(String.format(
					"minWritableBytes: %d (expected: >= 0)", minWritableBytes));
		}

		if (minWritableBytes <= writableBytes()) {
			return this;
		}

		if (minWritableBytes > maxCapacity() - writerIndex) {
			throw new IndexOutOfBoundsException(
					String.format(
							"writerIndex(%d) + minWritableBytes(%d) exceeds maxCapacity(%d): %s",
							writerIndex, minWritableBytes, maxCapacity(), this));
		}

		// Normalize the current capacity to the power of 2.
		int newCapacity = calculateNewCapacity(writerIndex + minWritableBytes);

		// Adjust to the new capacity.
		capacity(newCapacity, fillBuffer);
		return this;
	}

	private int calculateNewCapacity(int minNewCapacity) {
		final int maxCapacity = maxCapacity();
		final int threshold = 1048576 * 4; // 4 MiB page

		if (minNewCapacity == threshold) {
			return threshold;
		}

		// If over threshold, do not double but just increase by threshold.
		if (minNewCapacity > threshold) {
			int newCapacity = minNewCapacity / threshold * threshold;
			if (newCapacity > maxCapacity - threshold) {
				newCapacity = maxCapacity;
			} else {
				newCapacity += threshold;
			}
			return newCapacity;
		}

		// Not over threshold. Double up to 4 MiB, starting from 64.
		int newCapacity = 64;
		while (newCapacity < minNewCapacity) {
			newCapacity <<= 1;
		}

		return Math.min(newCapacity, maxCapacity);
	}

	@Override
	public int ensureWritable(int minWritableBytes, boolean force) {
		if (minWritableBytes < 0) {
			throw new IllegalArgumentException(String.format(
					"minWritableBytes: %d (expected: >= 0)", minWritableBytes));
		}

		if (minWritableBytes <= writableBytes()) {
			return 0;
		}

		if (minWritableBytes > maxCapacity() - writerIndex) {
			if (force) {
				if (capacity() == maxCapacity()) {
					return 1;
				}

				capacity(maxCapacity());
				return 3;
			}
		}

		// Normalize the current capacity to the power of 2.
		int newCapacity = calculateNewCapacity(writerIndex + minWritableBytes);

		// Adjust to the new capacity.
		capacity(newCapacity);
		return 2;
	}

	private final void checkIndex(int index) {
		if (index < 0 || index > capacity()) {
			throw new IndexOutOfBoundsException(String.format(
					"index: %d (expected: range(0, %d))", index, capacity()));
		}
	}

	private final void checkIndex(int index, int fieldLength) {
		if (fieldLength < 0) {
			throw new IllegalArgumentException("length: " + fieldLength
					+ " (expected: >= 0)");
		}
		if (index < 0 || index > capacity() - fieldLength) {
			throw new IndexOutOfBoundsException(String.format(
					"index: %d, length: %d (expected: range(0, %d))", index,
					fieldLength, capacity()));
		}
	}

	private Component findComponent(int offset) {
		checkIndex(offset);

		Component last = last();
		if (offset >= last.offset) {
			return last;
		}

		for (ListIterator<Component> i = components.listIterator(components
				.size() - 1); i.hasPrevious();) {
			Component c = i.previous();
			if (offset >= c.offset) {
				return c;
			}
		}

		throw new Error("should not happen");
	}

	private int findIndex(int offset) {
		checkIndex(offset);

		Component last = last();
		if (offset >= last.offset) {
			return components.size() - 1;
		}

		int index = components.size() - 2;
		for (ListIterator<Component> i = components.listIterator(components
				.size() - 1); i.hasPrevious(); index--) {
			Component c = i.previous();
			if (offset >= c.offset) {
				return index;
			}
		}

		throw new Error("should not happen");
	}

	@Override
	public boolean getBoolean(int index) {
		return getByte(index) != 0;
	}

	@Override
	public byte getByte(int index) {
		Component c = findComponent(index);
		return c.buf.getByte(index - c.offset);
	}

	@Override
	public short getUnsignedByte(int index) {
		return (short) (getByte(index) & 0xFF);
	}

	@Override
	public short getShort(int index) {
		Component c = findComponent(index);
		if (index + 2 <= c.endOffset()) {
			return c.buf.getShort(index - c.offset);
		} else if (order() == ByteOrder.BIG_ENDIAN) {
			return (short) ((getByte(index) & 0xff) << 8 | getByte(index + 1) & 0xff);
		} else {
			return (short) (getByte(index) & 0xff | (getByte(index + 1) & 0xff) << 8);
		}
	}

	@Override
	public int getUnsignedShort(int index) {
		return getShort(index) & 0xFFFF;
	}

	@Override
	public int getMedium(int index) {
		int value = getUnsignedMedium(index);
		if ((value & 0x800000) != 0) {
			value |= 0xff000000;
		}
		return value;
	}

	@Override
	public int getUnsignedMedium(int index) {
		Component c = findComponent(index);
		if (index + 3 <= c.endOffset()) {
			return c.buf.getUnsignedMedium(index - c.offset);
		} else if (order() == ByteOrder.BIG_ENDIAN) {
			return (getShort(index) & 0xffff) << 8 | getByte(index + 2) & 0xff;
		} else {
			return getShort(index) & 0xFFFF | (getByte(index + 2) & 0xFF) << 16;
		}
	}

	@Override
	public int getInt(int index) {
		Component c = findComponent(index);
		if (index + 4 <= c.endOffset()) {
			return c.buf.getInt(index - c.offset);
		} else if (order() == ByteOrder.BIG_ENDIAN) {
			return (getShort(index) & 0xffff) << 16 | getShort(index + 2)
					& 0xffff;
		} else {
			return getShort(index) & 0xFFFF
					| (getShort(index + 2) & 0xFFFF) << 16;
		}
	}

	@Override
	public long getUnsignedInt(int index) {
		return getInt(index) & 0xFFFFFFFFL;
	}

	@Override
	public long getLong(int index) {
		Component c = findComponent(index);
		if (index + 8 <= c.endOffset()) {
			return c.buf.getLong(index - c.offset);
		} else if (order() == ByteOrder.BIG_ENDIAN) {
			return (getInt(index) & 0xffffffffL) << 32 | getInt(index + 4)
					& 0xffffffffL;
		} else {
			return getInt(index) & 0xFFFFFFFFL
					| (getInt(index + 4) & 0xFFFFFFFFL) << 32;
		}
	}

	@Override
	public char getChar(int index) {
		return (char) getShort(index);
	}

	@Override
	public float getFloat(int index) {
		return Float.intBitsToFloat(getInt(index));
	}

	@Override
	public double getDouble(int index) {
		return Double.longBitsToDouble(getLong(index));
	}

	private final void checkDstIndex(int index, int length, int dstIndex,
			int dstCapacity) {
		checkIndex(index, length);
		if (dstIndex < 0 || dstIndex > dstCapacity - length) {
			throw new IndexOutOfBoundsException(String.format(
					"dstIndex: %d, length: %d (expected: range(0, %d))",
					dstIndex, length, dstCapacity));
		}
	}

	private final void checkSrcIndex(int index, int length, int srcIndex,
			int srcCapacity) {
		checkIndex(index, length);
		if (srcIndex < 0 || srcIndex > srcCapacity - length) {
			throw new IndexOutOfBoundsException(String.format(
					"srcIndex: %d, length: %d (expected: range(0, %d))",
					srcIndex, length, srcCapacity));
		}
	}

	@Override
	public AlternativeCompositeByteBuf getBytes(int index, ByteBuf dst) {
		getBytes(index, dst, dst.writableBytes());
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf getBytes(int index, ByteBuf dst, int length) {
		getBytes(index, dst, dst.writerIndex(), length);
		dst.writerIndex(dst.writerIndex() + length);
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
		checkDstIndex(index, length, dstIndex, dst.capacity());
		if (length == 0) {
			return this;
		}

		int i = findIndex(index);
		while (length > 0) {
			Component c = components.get(i);
			ByteBuf s = c.buf;
			int adjustment = c.offset;
			int localLength = Math.min(length, s.readableBytes()
					- (index - adjustment));
			s.getBytes(index - adjustment, dst, dstIndex, localLength);
			index += localLength;
			dstIndex += localLength;
			length -= localLength;
			i++;
		}
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf getBytes(int index, byte[] dst) {
		getBytes(index, dst, 0, dst.length);
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
		checkDstIndex(index, length, dstIndex, dst.length);
		if (length == 0) {
			return this;
		}

		int i = findIndex(index);
		while (length > 0) {
			Component c = components.get(i);
			ByteBuf s = c.buf;
			int adjustment = c.offset;
			int localLength = Math.min(length, s.readableBytes()
					- (index - adjustment));
			s.getBytes(index - adjustment, dst, dstIndex, localLength);
			index += localLength;
			dstIndex += localLength;
			length -= localLength;
			i++;
		}
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf getBytes(int index, ByteBuffer dst) {
		int limit = dst.limit();
		int length = dst.remaining();

		checkIndex(index, length);
		if (length == 0) {
			return this;
		}

		int i = findIndex(index);
		try {
			while (length > 0) {
				Component c = components.get(i);
				ByteBuf s = c.buf;
				int adjustment = c.offset;
				int localLength = Math.min(length, s.readableBytes()
						- (index - adjustment));
				dst.limit(dst.position() + localLength);
				s.getBytes(index - adjustment, dst);
				index += localLength;
				length -= localLength;
				i++;
			}
		} finally {
			dst.limit(limit);
		}
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf getBytes(int index, OutputStream out, int length)
			throws IOException {
		checkIndex(index, length);
		if (length == 0) {
			return this;
		}

		int i = findIndex(index);
		while (length > 0) {
			Component c = components.get(i);
			ByteBuf s = c.buf;
			int adjustment = c.offset;
			int localLength = Math.min(length, s.readableBytes()
					- (index - adjustment));
			s.getBytes(index - adjustment, out, localLength);
			index += localLength;
			length -= localLength;
			i++;
		}
		return this;
	}

	@Override
	public int getBytes(int index, GatheringByteChannel out, int length)
			throws IOException {
		int count = nioBufferCount();
		if (count == 1) {
			return out.write(internalNioBuffer(index, length));
		} else {
			long writtenBytes = out.write(nioBuffers(index, length));
			if (writtenBytes > Integer.MAX_VALUE) {
				return Integer.MAX_VALUE;
			} else {
				return (int) writtenBytes;
			}
		}
	}

	@Override
	public AlternativeCompositeByteBuf setBoolean(int index, boolean value) {
		setByte(index, value ? 1 : 0);
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf setByte(int index, int value) {
		Component c = findComponent(index);
		c.buf.setByte(index - c.offset, value);
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf setShort(int index, int value) {
		Component c = findComponent(index);
		if (index + 2 <= c.endOffset()) {
			c.buf.setShort(index - c.offset, value);
		} else if (order() == ByteOrder.BIG_ENDIAN) {
			setByte(index, (byte) (value >>> 8));
			setByte(index + 1, (byte) value);
		} else {
			setByte(index, (byte) value);
			setByte(index + 1, (byte) (value >>> 8));
		}
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf setMedium(int index, int value) {
		Component c = findComponent(index);
		if (index + 3 <= c.endOffset()) {
			c.buf.setMedium(index - c.offset, value);
		} else if (order() == ByteOrder.BIG_ENDIAN) {
			setShort(index, (short) (value >> 8));
			setByte(index + 2, (byte) value);
		} else {
			setShort(index, (short) value);
			setByte(index + 2, (byte) (value >>> 16));
		}
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf setInt(int index, int value) {
		Component c = findComponent(index);
		if (index + 4 <= c.endOffset()) {
			c.buf.setInt(index - c.offset, value);
		} else if (order() == ByteOrder.BIG_ENDIAN) {
			setShort(index, (short) (value >>> 16));
			setShort(index + 2, (short) value);
		} else {
			setShort(index, (short) value);
			setShort(index + 2, (short) (value >>> 16));
		}
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf setLong(int index, long value) {
		Component c = findComponent(index);
		if (index + 8 <= c.endOffset()) {
			c.buf.setLong(index - c.offset, value);
		} else if (order() == ByteOrder.BIG_ENDIAN) {
			setInt(index, (int) (value >>> 32));
			setInt(index + 4, (int) value);
		} else {
			setInt(index, (int) value);
			setInt(index + 4, (int) (value >>> 32));
		}
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf setChar(int index, int value) {
		setShort(index, value);
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf setFloat(int index, float value) {
		setInt(index, Float.floatToRawIntBits(value));
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf setDouble(int index, double value) {
		setLong(index, Double.doubleToRawLongBits(value));
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf setBytes(int index, ByteBuf src) {
		setBytes(index, src, src.readableBytes());
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf setBytes(int index, ByteBuf src, int length) {
		checkIndex(index, length);
		if (src == null) {
			throw new NullPointerException("src");
		}
		if (length > src.readableBytes()) {
			throw new IndexOutOfBoundsException(
					String.format(
							"length(%d) exceeds src.readableBytes(%d) where src is: %s",
							length, src.readableBytes(), src));
		}

		setBytes(index, src, src.readerIndex(), length);
		src.readerIndex(src.readerIndex() + length);
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
		checkSrcIndex(index, length, srcIndex, src.capacity());
		if (length == 0) {
			return this;
		}

		int i = findIndex(index);
		while (length > 0) {
			Component c = components.get(i);
			ByteBuf s = c.buf;
			int adjustment = c.offset;
			int localLength = Math.min(length, s.writableBytes());
			s.setBytes(index - adjustment, src, srcIndex, localLength);
			index += localLength;
			srcIndex += localLength;
			length -= localLength;
			i++;
		}
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf setBytes(int index, byte[] src) {
		setBytes(index, src, 0, src.length);
		return this;
	}

	@Override
	public AlternativeCompositeByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
		checkSrcIndex(index, length, srcIndex, src.length);
		if (length == 0) {
			return this;
		}

		int i = findIndex(index);
		while (length > 0) {
			Component c = components.get(i);
			ByteBuf s = c.buf;
			int adjustment = c.offset;
			int localLength = Math.min(length, s.writableBytes());
			s.setBytes(index - adjustment, src, srcIndex, localLength);
			index += localLength;
			srcIndex += localLength;
			length -= localLength;
			i++;
		}
		return this;
	}

	public boolean sync() {
		int counter = 0;

		for (Component c : components) {
			counter += c.buf.writerIndex();
		}
		return counter == writerIndex;
	}

	@Override
	public AlternativeCompositeByteBuf setBytes(int index, ByteBuffer src) {
		int limit = src.limit();
		int length = src.remaining();

		checkIndex(index, length);
		if (length == 0) {
			return this;
		}

		int i = findIndex(index);
		try {
			while (length > 0) {
				Component c = components.get(i);
				ByteBuf s = c.buf;
				int adjustment = c.offset;
				int localLength = Math.min(length, s.writableBytes());
				src.limit(src.position() + localLength);
				s.setBytes(index - adjustment, src);
				index += localLength;
				length -= localLength;
				i++;
			}
		} finally {
			src.limit(limit);
		}
		return this;
	}

	@Override
	public int setBytes(int index, InputStream in, int length)
			throws IOException {
		checkIndex(index, length);
		if (length == 0) {
			return in.read(EmptyArrays.EMPTY_BYTES);
		}

		int i = findIndex(index);
		int readBytes = 0;

		do {
			Component c = components.get(i);
			ByteBuf s = c.buf;
			int adjustment = c.offset;
			int localLength = Math.min(length, s.writableBytes());
			int localReadBytes = s
					.setBytes(index - adjustment, in, localLength);
			if (localReadBytes < 0) {
				if (readBytes == 0) {
					return -1;
				} else {
					break;
				}
			}

			if (localReadBytes == localLength) {
				index += localLength;
				length -= localLength;
				readBytes += localLength;
				i++;
			} else {
				index += localReadBytes;
				length -= localReadBytes;
				readBytes += localReadBytes;
			}
		} while (length > 0);

		return readBytes;
	}

	@Override
	public int setBytes(int index, ScatteringByteChannel in, int length)
			throws IOException {
		checkIndex(index, length);
		if (length == 0) {
			return in.read(FULL_BYTEBUFFER);
		}

		int i = findIndex(index);
		int readBytes = 0;
		do {
			Component c = components.get(i);
			ByteBuf s = c.buf;
			int adjustment = c.offset;
			int localLength = Math.min(length, s.writableBytes());
			int localReadBytes = s
					.setBytes(index - adjustment, in, localLength);

			if (localReadBytes == 0) {
				break;
			}

			if (localReadBytes < 0) {
				if (readBytes == 0) {
					return -1;
				} else {
					break;
				}
			}

			if (localReadBytes == localLength) {
				index += localLength;
				length -= localLength;
				readBytes += localLength;
				i++;
			} else {
				index += localReadBytes;
				length -= localReadBytes;
				readBytes += localReadBytes;
			}
		} while (length > 0);

		return readBytes;
	}

	@Override
	public AlternativeCompositeByteBuf setZero(int index, int length) {
		if (length == 0) {
			return this;
		}

		checkIndex(index, length);

		int nLong = length >>> 3;
		int nBytes = length & 7;
		for (int i = nLong; i > 0; i--) {
			setLong(index, 0);
			index += 8;
		}
		if (nBytes == 4) {
			setInt(index, 0);
		} else if (nBytes < 4) {
			for (int i = nBytes; i > 0; i--) {
				setByte(index, (byte) 0);
				index++;
			}
		} else {
			setInt(index, 0);
			index += 4;
			for (int i = nBytes - 4; i > 0; i--) {
				setByte(index, (byte) 0);
				index++;
			}
		}
		return this;
	}

	/**
	 * Throws an {@link IndexOutOfBoundsException} if the current
	 * {@linkplain #readableBytes() readable bytes} of this buffer is less than
	 * the specified value.
	 */
	private final void checkReadableBytes(int minimumReadableBytes) {
		if (minimumReadableBytes < 0) {
			throw new IllegalArgumentException("minimumReadableBytes: "
					+ minimumReadableBytes + " (expected: >= 0)");
		}
		if (readerIndex > writerIndex - minimumReadableBytes) {
			throw new IndexOutOfBoundsException(String.format(
					"readerIndex(%d) + length(%d) exceeds writerIndex(%d): %s",
					readerIndex, minimumReadableBytes, writerIndex, this));
		}
	}

	@Override
	public boolean readBoolean() {
		return readByte() != 0;
	}

	@Override
	public byte readByte() {
		checkReadableBytes(1);
		int i = readerIndex;
		byte b = getByte(i);
		readerIndex = i + 1;
		return b;
	}

	@Override
	public short readUnsignedByte() {
		return (short) (readByte() & 0xFF);
	}

	@Override
	public short readShort() {
		checkReadableBytes(2);
		short v = getShort(readerIndex);
		readerIndex += 2;
		return v;
	}

	@Override
	public int readUnsignedShort() {
		return readShort() & 0xFFFF;
	}

	@Override
	public int readMedium() {
		int value = readUnsignedMedium();
		if ((value & 0x800000) != 0) {
			value |= 0xff000000;
		}
		return value;
	}

	@Override
	public int readUnsignedMedium() {
		checkReadableBytes(3);
		int v = getUnsignedMedium(readerIndex);
		readerIndex += 3;
		return v;
	}

	@Override
	public int readInt() {
		checkReadableBytes(4);
		int v = getInt(readerIndex);
		readerIndex += 4;
		return v;
	}

	@Override
	public long readUnsignedInt() {
		return readInt() & 0xFFFFFFFFL;
	}

	@Override
	public long readLong() {
		checkReadableBytes(8);
		long v = getLong(readerIndex);
		readerIndex += 8;
		return v;
	}

	@Override
	public char readChar() {
		return (char) readShort();
	}

	@Override
	public float readFloat() {
		return Float.intBitsToFloat(readInt());
	}

	@Override
	public double readDouble() {
		return Double.longBitsToDouble(readLong());
	}

	@Override
	public ByteBuf readBytes(int length) {
		checkReadableBytes(length);
		if (length == 0) {
			return Unpooled.EMPTY_BUFFER;
		}

		// Use an unpooled heap buffer because there's no way to mandate a user
		// to free the returned buffer.
		ByteBuf buf = Unpooled.buffer(length, maxCapacity());
		buf.writeBytes(this, readerIndex, length);
		readerIndex += length;
		return buf;
	}

	@Override
	public ByteBuf readSlice(int length) {
		ByteBuf slice = slice(readerIndex, length);
		readerIndex += length;
		return slice;
	}

	@Override
	public ByteBuf readBytes(ByteBuf dst) {
		readBytes(dst, dst.writableBytes());
		return this;
	}

	@Override
	public ByteBuf readBytes(ByteBuf dst, int length) {
		if (length > dst.writableBytes()) {
			throw new IndexOutOfBoundsException(
					String.format(
							"length(%d) exceeds dst.writableBytes(%d) where dst is: %s",
							length, dst.writableBytes(), dst));
		}
		readBytes(dst, dst.writerIndex(), length);
		dst.writerIndex(dst.writerIndex() + length);
		return this;
	}

	@Override
	public ByteBuf readBytes(ByteBuf dst, int dstIndex, int length) {
		checkReadableBytes(length);
		getBytes(readerIndex, dst, dstIndex, length);
		readerIndex += length;
		return this;
	}

	@Override
	public ByteBuf readBytes(byte[] dst) {
		readBytes(dst, 0, dst.length);
		return this;
	}

	@Override
	public ByteBuf readBytes(byte[] dst, int dstIndex, int length) {
		checkReadableBytes(length);
		getBytes(readerIndex, dst, dstIndex, length);
		readerIndex += length;
		return this;
	}

	@Override
	public ByteBuf readBytes(ByteBuffer dst) {
		int length = dst.remaining();
		checkReadableBytes(length);
		getBytes(readerIndex, dst);
		readerIndex += length;
		return this;
	}

	@Override
	public ByteBuf readBytes(OutputStream out, int length) throws IOException {
		checkReadableBytes(length);
		getBytes(readerIndex, out, length);
		readerIndex += length;
		return this;
	}

	@Override
	public int readBytes(GatheringByteChannel out, int length)
			throws IOException {
		checkReadableBytes(length);
		int readBytes = getBytes(readerIndex, out, length);
		readerIndex += readBytes;
		return readBytes;
	}

	@Override
	public ByteBuf skipBytes(int length) {
		checkReadableBytes(length);

		int newReaderIndex = readerIndex + length;
		if (newReaderIndex > writerIndex) {
			throw new IndexOutOfBoundsException(
					String.format(
							"length: %d (expected: readerIndex(%d) + length <= writerIndex(%d))",
							length, readerIndex, writerIndex));
		}
		readerIndex = newReaderIndex;
		return this;
	}

	private void setComponentWriterIndex(int writerIndex) {
		if (this.writerIndex == writerIndex) {
			//nothing to do
			return;
		}
		int index = findIndex(writerIndex);
		if(index < 0) {
			//no component found, make sure we can write, thus adding a compontent. TODO: check fillbuffer
			ensureWritable(writerIndex);
			index = findIndex(writerIndex);
		}
		int to = findIndex(this.writerIndex);
		Component c = components.get(index);
		int relWriterIndex = writerIndex - c.offset;
		c.buf.writerIndex(relWriterIndex);
		
		if (this.writerIndex < writerIndex) {
			//new writer index is larger than the old one
			// assuming full buffers
			for (int i = index - 1; i > to; i--) {
				c = components.get(i);
				c.buf.writerIndex(c.buf.capacity());
			}
			this.writerIndex = writerIndex;
			
		} else {
			//we go back in the buffer
			for (int i = index + 1; i < to; i++) {
				components.get(i).buf.writerIndex(0);
			}
			this.writerIndex = writerIndex;
		}
	}

	private void increaseComponentWriterIndex(final int increase) {
		int maxIncrease = 0;
		int currentIncrease = increase;
		int index = findIndex(writerIndex);
		while (maxIncrease < increase) {
			Component c = components.get(index);
			int writable = c.buf.writableBytes();
			writable = Math.min(writable, currentIncrease);
			c.buf.writerIndex(c.buf.writerIndex() + writable);
			currentIncrease -= writable;
			maxIncrease += writable;
			index++;
		}
		writerIndex += increase;
	}

	@Override
	public ByteBuf writeBoolean(boolean value) {
		writeByte(value ? 1 : 0);
		return this;
	}

	@Override
	public ByteBuf writeByte(int value) {
		ensureWritable0(1, true);
		setByte(writerIndex, value);
		increaseComponentWriterIndex(1);
		return this;
	}

	@Override
	public ByteBuf writeShort(int value) {
		ensureWritable0(2, true);
		setShort(writerIndex, value);
		increaseComponentWriterIndex(2);
		return this;
	}

	@Override
	public ByteBuf writeMedium(int value) {
		ensureWritable0(3, true);
		setMedium(writerIndex, value);
		increaseComponentWriterIndex(3);
		return this;
	}

	@Override
	public ByteBuf writeInt(int value) {
		ensureWritable0(4, true);
		setInt(writerIndex, value);
		increaseComponentWriterIndex(4);
		return this;
	}

	@Override
	public ByteBuf writeLong(long value) {
		ensureWritable0(8, true);
		setLong(writerIndex, value);
		increaseComponentWriterIndex(8);
		return this;
	}

	@Override
	public ByteBuf writeChar(int value) {
		writeShort(value);
		return this;
	}

	@Override
	public ByteBuf writeFloat(float value) {
		writeInt(Float.floatToRawIntBits(value));
		return this;
	}

	@Override
	public ByteBuf writeDouble(double value) {
		writeLong(Double.doubleToRawLongBits(value));
		return this;
	}

	@Override
	public ByteBuf writeBytes(ByteBuf src) {
		writeBytes(src, src.readableBytes());
		return this;
	}

	@Override
	public ByteBuf writeBytes(ByteBuf src, int length) {
		if (length > src.readableBytes()) {
			throw new IndexOutOfBoundsException(
					String.format(
							"length(%d) exceeds src.readableBytes(%d) where src is: %s",
							length, src.readableBytes(), src));
		}
		writeBytes(src, src.readerIndex(), length);
		src.readerIndex(src.readerIndex() + length);
		return this;
	}

	@Override
	public ByteBuf writeBytes(ByteBuf src, int srcIndex, int length) {
		ensureWritable0(length, true);
		setBytes(writerIndex, src, srcIndex, length);
		increaseComponentWriterIndex(length);
		return this;
	}

	@Override
	public ByteBuf writeBytes(byte[] src) {
		writeBytes(src, 0, src.length);
		return this;
	}

	@Override
	public ByteBuf writeBytes(byte[] src, int srcIndex, int length) {
		ensureWritable0(length, true);
		setBytes(writerIndex, src, srcIndex, length);
		increaseComponentWriterIndex(length);
		return this;
	}

	@Override
	public ByteBuf writeBytes(ByteBuffer src) {
		int length = src.remaining();
		ensureWritable0(length, true);
		setBytes(writerIndex, src);
		increaseComponentWriterIndex(length);
		return this;
	}

	@Override
	public int writeBytes(InputStream in, int length) throws IOException {
		ensureWritable0(length, true);
		int writtenBytes = setBytes(writerIndex, in, length);
		if (writtenBytes > 0) {
			increaseComponentWriterIndex(writtenBytes);
		}
		return writtenBytes;
	}

	@Override
	public int writeBytes(ScatteringByteChannel in, int length)
			throws IOException {
		ensureWritable0(length, true);
		int writtenBytes = setBytes(writerIndex, in, length);
		if (writtenBytes > 0) {
			increaseComponentWriterIndex(writtenBytes);
		}
		return writtenBytes;
	}

	@Override
	public ByteBuf writeZero(int length) {
		if (length == 0) {
			return this;
		}

		ensureWritable0(length, true);
		checkIndex(writerIndex, length);

		int nLong = length >>> 3;
		int nBytes = length & 7;
		for (int i = nLong; i > 0; i--) {
			writeLong(0);
		}
		if (nBytes == 4) {
			writeInt(0);
		} else if (nBytes < 4) {
			for (int i = nBytes; i > 0; i--) {
				writeByte((byte) 0);
			}
		} else {
			writeInt(0);
			for (int i = nBytes - 4; i > 0; i--) {
				writeByte((byte) 0);
			}
		}
		return this;
	}

	@Override
	public int indexOf(int fromIndex, int toIndex, byte value) {
		return ByteBufUtil.indexOf(this, fromIndex, toIndex, value);
	}

	@Override
	public int bytesBefore(byte value) {
		return bytesBefore(readerIndex(), readableBytes(), value);
	}

	@Override
	public int bytesBefore(int length, byte value) {
		checkReadableBytes(length);
		return bytesBefore(readerIndex(), length, value);
	}

	@Override
	public int bytesBefore(int index, int length, byte value) {
		int endIndex = indexOf(index, index + length, value);
		if (endIndex < 0) {
			return -1;
		}
		return endIndex - index;
	}

	@Override
	public int forEachByte(ByteBufProcessor processor) {
		int index = readerIndex;
		int length = writerIndex - index;
		return forEachByteAsc0(index, length, processor);
	}

	@Override
	public int forEachByte(int index, int length, ByteBufProcessor processor) {
		checkIndex(index, length);
		return forEachByteAsc0(index, length, processor);
	}

	private int forEachByteAsc0(int index, int length,
			ByteBufProcessor processor) {
		if (processor == null) {
			throw new NullPointerException("processor");
		}

		if (length == 0) {
			return -1;
		}

		final int endIndex = index + length;
		int i = index;
		try {
			do {
				if (processor.process(getByte(i))) {
					i++;
				} else {
					return i;
				}
			} while (i < endIndex);
		} catch (Exception e) {
			PlatformDependent.throwException(e);
		}

		return -1;
	}

	@Override
	public int forEachByteDesc(ByteBufProcessor processor) {
		int index = readerIndex;
		int length = writerIndex - index;
		return forEachByteDesc0(index, length, processor);
	}

	@Override
	public int forEachByteDesc(int index, int length, ByteBufProcessor processor) {
		checkIndex(index, length);
		return forEachByteDesc0(index, length, processor);
	}

	private int forEachByteDesc0(int index, int length,
			ByteBufProcessor processor) {
		if (processor == null) {
			throw new NullPointerException("processor");
		}

		if (length == 0) {
			return -1;
		}

		int i = index + length - 1;
		try {
			do {
				if (processor.process(getByte(i))) {
					i--;
				} else {
					return i;
				}
			} while (i >= index);
		} catch (Exception e) {
			PlatformDependent.throwException(e);
		}

		return -1;
	}

	@Override
	public ByteBuf copy() {
		return copy(readerIndex, readableBytes());
	}

	@Override
	public ByteBuf copy(int index, int length) {
		checkIndex(index, length);
		ByteBuf dst = Unpooled.buffer(length);
		if (length != 0) {
			copyTo(index, length, findIndex(index), dst);
		}
		return dst;
	}

	private void copyTo(int index, int length, int componentId, ByteBuf dst) {
		int dstIndex = 0;
		int i = componentId;

		while (length > 0) {
			Component c = components.get(i);
			ByteBuf s = c.buf;
			int adjustment = c.offset;
			int localLength = Math.min(length, s.readableBytes()
					- (index - adjustment));
			s.getBytes(index - adjustment, dst, dstIndex, localLength);
			index += localLength;
			dstIndex += localLength;
			length -= localLength;
			i++;
		}

		dst.writerIndex(dst.capacity());
	}

	@Override
	public ByteBuf slice() {
		return slice(readerIndex, readableBytes());
	}

	@Override
	public ByteBuf slice(int index, int length) {
		if (length == 0) {
			return Unpooled.EMPTY_BUFFER;
		}

		return new SlicedByteBuf(this, index, length);
	}

	@Override
	public ByteBuf duplicate() {
		return new DuplicatedByteBuf(this);
	}

	@Override
	public int nioBufferCount() {
		if (components.size() == 1) {
			return components.get(0).buf.nioBufferCount();
		} else {
			int count = 0;
			int componentsCount = components.size();
			// noinspection ForLoopReplaceableByForEach
			for (int i = 0; i < componentsCount; i++) {
				Component c = components.get(i);
				count += c.buf.nioBufferCount();
			}
			return count;
		}
	}

	@Override
	public ByteBuffer nioBuffer() {
		return nioBuffer(readerIndex, readableBytes());
	}

	@Override
	public ByteBuffer nioBuffer(int index, int length) {
		if (components.size() == 1) {
			ByteBuf buf = components.get(0).buf;
			if (buf.nioBufferCount() == 1) {
				return components.get(0).buf.nioBuffer(index, length);
			}
		}
		ByteBuffer merged = ByteBuffer.allocate(length).order(order());
		ByteBuffer[] buffers = nioBuffers(index, length);

		// noinspection ForLoopReplaceableByForEach
		for (int i = 0; i < buffers.length; i++) {
			merged.put(buffers[i]);
		}

		merged.flip();
		return merged;
	}

	@Override
	public ByteBuffer internalNioBuffer(int index, int length) {
		if (components.size() == 1) {
			return components.get(0).buf.internalNioBuffer(index, length);
		}
		throw new UnsupportedOperationException();
	}

	@Override
	public ByteBuffer[] nioBuffers() {
		return nioBuffers(readerIndex, readableBytes());
	}

	@Override
	public ByteBuffer[] nioBuffers(int index, int length) {
		checkIndex(index, length);
		if (length == 0) {
			return EmptyArrays.EMPTY_BYTE_BUFFERS;
		}

		List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(components.size());
		int i = findIndex(index);
		while (length > 0) {
			Component c = components.get(i);
			ByteBuf s = c.buf;
			int adjustment = c.offset;
			int localLength = Math.min(length, s.readableBytes()
					- (index - adjustment));
			switch (s.nioBufferCount()) {
			case 0:
				throw new UnsupportedOperationException();
			case 1:
				buffers.add(s.nioBuffer(index - adjustment, localLength));
				break;
			default:
				Collections.addAll(buffers,
						s.nioBuffers(index - adjustment, localLength));
			}

			index += localLength;
			length -= localLength;
			i++;
		}

		return buffers.toArray(new ByteBuffer[buffers.size()]);
	}

	@Override
	public boolean hasArray() {
		if (components.size() == 1) {
			return components.get(0).buf.hasArray();
		}
		return false;
	}

	@Override
	public byte[] array() {
		if (components.size() == 1) {
			return components.get(0).buf.array();
		}
		throw new UnsupportedOperationException();
	}

	@Override
	public int arrayOffset() {
		if (components.size() == 1) {
			return components.get(0).buf.arrayOffset();
		}
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasMemoryAddress() {
		if (components.size() == 1) {
			return components.get(0).buf.hasMemoryAddress();
		}
		return false;
	}

	@Override
	public long memoryAddress() {
		if (components.size() == 1) {
			return components.get(0).buf.memoryAddress();
		}
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString(Charset charset) {
		return toString(readerIndex, readableBytes(), charset);
	}

	@Override
	public String toString(int index, int length, Charset charset) {
		if (length == 0) {
			return "";
		}

		ByteBuffer nioBuffer;
		if (nioBufferCount() == 1) {
			nioBuffer = nioBuffer(index, length);
		} else {
			nioBuffer = ByteBuffer.allocate(length);
			getBytes(index, nioBuffer);
			nioBuffer.flip();
		}

		return decodeString(nioBuffer, charset);
	}

	@Override
	public int hashCode() {
		return ByteBufUtil.hashCode(this);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o instanceof ByteBuf) {
			return ByteBufUtil.equals(this, (ByteBuf) o);
		}
		return false;
	}

	@Override
	public int compareTo(ByteBuf that) {
		return ByteBufUtil.compare(this, that);
	}

	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		buf.append(StringUtil.simpleClassName(this));
		buf.append("(ridx: ");
		buf.append(readerIndex);
		buf.append(", widx: ");
		buf.append(writerIndex);
		buf.append(", cap: ");
		buf.append(capacity());
		buf.append(", comp: ");
		buf.append(components.size());
		buf.append(')');
		return buf.toString();
	}
	
	 public List<ByteBuf> decompose(int offset, int length) {
	        checkIndex(offset, length);
	        if (length == 0) {
	            return Collections.emptyList();
	        }

	        int componentId = findIndex(offset);
	        List<ByteBuf> slice = new ArrayList<ByteBuf>(components.size());

	        // The first component
	        Component firstC = components.get(componentId);
	        ByteBuf first = firstC.buf.duplicate();
	        first.readerIndex(offset - firstC.offset);

	        ByteBuf buf = first;
	        int bytesToSlice = length;
	        do {
	            int readableBytes = buf.readableBytes();
	            if (bytesToSlice <= readableBytes) {
	                // Last component
	                buf.writerIndex(buf.readerIndex() + bytesToSlice);
	                slice.add(buf);
	                break;
	            } else {
	                // Not the last component
	                slice.add(buf);
	                bytesToSlice -= readableBytes;
	                componentId ++;

	                // Fetch the next component.
	                buf = components.get(componentId).buf.duplicate();
	            }
	        } while (bytesToSlice > 0);

	        // Slice all components because only readable bytes are interesting.
	        for (int i = 0; i < slice.size(); i ++) {
	            slice.set(i, slice.get(i).slice());
	        }

	        return slice;
	    }
	

	private static String decodeString(ByteBuffer src, Charset charset) {
		final CharsetDecoder decoder = CharsetUtil.getDecoder(charset);
		final CharBuffer dst = CharBuffer.allocate((int) ((double) src
				.remaining() * decoder.maxCharsPerByte()));
		try {
			CoderResult cr = decoder.decode(src, dst, true);
			if (!cr.isUnderflow()) {
				cr.throwException();
			}
			cr = decoder.flush(dst);
			if (!cr.isUnderflow()) {
				cr.throwException();
			}
		} catch (CharacterCodingException x) {
			throw new IllegalStateException(x);
		}
		return dst.flip().toString();
	}

	public static AlternativeCompositeByteBuf compBuffer(ByteBufAllocator alloc,
			boolean direct, ByteBuf... buffers) {
		return new AlternativeCompositeByteBuf(alloc, direct, buffers);
	}

	public static AlternativeCompositeByteBuf compBuffer(boolean direct) {
		return compBuffer(ALLOC, direct);
	}

	public static AlternativeCompositeByteBuf compBuffer() {
		return compBuffer(false);
	}

	public static AlternativeCompositeByteBuf compDirectBuffer() {
		return compBuffer(true);
	}

	public static AlternativeCompositeByteBuf compDirectBuffer(ByteBuf... buffers) {
		return compBuffer(ALLOC, true, buffers);
	}

	public static AlternativeCompositeByteBuf compBuffer(ByteBuf... buffers) {
		return compBuffer(ALLOC, false, buffers);
	}

}
