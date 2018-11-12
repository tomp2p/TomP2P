/*
 * Copyright 2012 Thomas Bocek
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package net.tomp2p.message;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import net.tomp2p.peers.Number256;
import net.tomp2p.utils.Utils;

/**
 * Bare minimum ASN.1 encoder and decoder for the signature.
 * 
 * @author Thomas Bocek
 * 
 */
public class DSASignatureCodec implements SignatureCodec {
	
	public static final int SIGNATURE_SIZE = 2 * Number256.BYTE_ARRAY_SIZE;
	private final Number256 number1;
	private final Number256 number2;

	/**
	 * Create a signature from an already encoded signature data
	 * 
	 * @param encodedData the signature (encoded)
	 * @throws IOException
	 */
	public DSASignatureCodec(byte[] encodedData) throws IOException {
		if (encodedData[0] != 0x30) {
			throw new IOException("expected sequence with value 48, but got " + encodedData[0]);
		}
		int seqLen = encodedData[1];
		if (seqLen < 0) {
			throw new IOException("cannot handle seq length > than 127, got " + seqLen);
		}
		if (encodedData[2] != 0x02) {
			throw new IOException("expected sequence with value 2, but got " + encodedData[2]);
		}
		int intLen1 = encodedData[3];
		if (intLen1 < 0) {
			throw new IOException("cannot handle int length > than 127, got " + intLen1);
		}
		number1 = encodeNumber(encodedData, 4, intLen1);
		if (encodedData[4 + intLen1] != 0x02) {
			throw new IOException("expected sequence with value 2, but got " + encodedData[4 + intLen1]);
		}
		int intLen2 = encodedData[5 + intLen1];
		if (intLen2 < 0) {
			throw new IOException("cannot handle int length > than 127, got " + intLen2);
		}
		number2 = encodeNumber(encodedData, 6 + intLen1, intLen2);
	}
	
	/**
	 * Create a codec from a buffer, where the signature data is encoded
	 * @param buf the buffer holding the signature at the next reader index
	 */
	public DSASignatureCodec(ByteBuf buf) {
		byte[] me = new byte[Number256.BYTE_ARRAY_SIZE];
		buf.readBytes(me);
		number1 = new Number256(me);
		buf.readBytes(me);
		number2 = new Number256(me);
	}

	private Number256 encodeNumber(byte[] encodedData, int offset, int len) throws IOException {
		if (len > 20) {
			int bias = len - 20;
			for (int i = 0; i < bias; i++) {
				if (encodedData[offset + i] != 0) {
					throw new IOException("we did not expect such a large number, it should be 160bit");
				}
			}
			return new Number256(encodedData, offset + bias, 20);
		} else {
			return new Number256(encodedData, offset, len);
		}
	}

	/**
	 * @return ASN1 encoded signature
	 */
	@Override
    public byte[] encode() {
		byte me[] = new byte[2 + (2 * (20 + 2))];
		me[0] = 0x30;
		me[1] = 2 * (20 + 2);
		me[2] = 0x02;
		me[3] = 20;
		number1.encode(me, 4);
		me[24] = 0x02;
		me[25] = 20;
		number2.encode(me, 26);
		return me;
	}
	
	@Override
    public SignatureCodec write(ByteBuf buf) {
		buf.writeBytes(number1.toByteArray());
		buf.writeBytes(number2.toByteArray());
		return this;
	}

	public Number256 number1() {
		return number1;
	}

	public Number256 number2() {
		return number2;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("sig[");
		sb.append(number1.toString()).append('/');
		sb.append(number2.toString()).append(']');
		return sb.toString();
	}

	@Override
	public int hashCode() {
		return number1.hashCode() ^ number2.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof DSASignatureCodec)) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		DSASignatureCodec s = (DSASignatureCodec) obj;
		return Utils.equals(number1, s.number1) && Utils.equals(number2, s.number2);
	}

	@Override
	public int signatureSize() {
		return SIGNATURE_SIZE;
	}
}
