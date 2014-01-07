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

import java.io.IOException;

import net.tomp2p.peers.Number160;
import net.tomp2p.utils.Utils;

/**
 * Bare minimun ASN.1 encoder and decoder for the signature.
 * 
 * @author Thomas Bocek
 * 
 */
public class SHA1Signature {
	private Number160 number1;

	private Number160 number2;

	public SHA1Signature() {
	}

	public SHA1Signature(Number160 number1, Number160 number2) {
		this.number1 = number1;
		this.number2 = number2;
	}

	public void decode(byte[] encodedData) throws IOException {
		if (encodedData[0] != 0x30) {
			throw new IOException("expected sequence with value 48, but got " + encodedData[0]);
		}
		int seqLen = encodedData[1];
		if (seqLen < 0) {
			throw new IOException("cannot handle seq legth > than 127, got " + seqLen);
		}
		if (encodedData[2] != 0x02) {
			throw new IOException("expected sequence with value 2, but got " + encodedData[2]);
		}
		int intLen1 = encodedData[3];
		if (intLen1 < 0) {
			throw new IOException("cannot handle int legth > than 127, got " + intLen1);
		}
		number1 = encodeNumber(encodedData, 4, intLen1);
		//
		if (encodedData[4 + intLen1] != 0x02) {
			throw new IOException("expected sequence with value 2, but got " + encodedData[4 + intLen1]);
		}
		int intLen2 = encodedData[5 + intLen1];
		if (intLen2 < 0) {
			throw new IOException("cannot handle int legth > than 127, got " + intLen2);
		}
		number2 = encodeNumber(encodedData, 6 + intLen1, intLen2);
	}

	private Number160 encodeNumber(byte[] encodedData, int offset, int len) throws IOException {
		if (len > 20) {
			int bias = len - 20;
			for (int i = 0; i < bias; i++) {
				if (encodedData[offset + i] != 0) {
					throw new IOException("we did not expect such a large number, it should be 160bit");
				}
			}
			return new Number160(encodedData, offset + bias, 20);
		} else {
			return new Number160(encodedData, offset, len);
		}
	}

	/**
	 * @return ASN1 encoded signature
	 * @throws IOException
	 */
	public byte[] encode() throws IOException {
		byte me[] = new byte[2 + (2 * (20 + 2))];
		me[0] = 0x30;
		me[1] = 2 * (20 + 2);
		me[2] = 0x02;
		me[3] = 20;
		number1.toByteArray(me, 4);
		me[24] = 0x02;
		me[25] = 20;
		number2.toByteArray(me, 26);
		return me;
	}

	public Number160 getNumber1() {
		return number1;
	}

	public Number160 getNumber2() {
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
	public boolean equals(Object obj) {
		if (!(obj instanceof SHA1Signature)) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		SHA1Signature s = (SHA1Signature) obj;
		return Utils.<Number160> equals(number1, s.number1) && Utils.<Number160> equals(number2, s.number2);
	}
}
