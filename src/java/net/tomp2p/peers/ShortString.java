/*
 * Copyright 2009 Thomas Bocek
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
package net.tomp2p.peers;
import java.io.UnsupportedEncodingException;


/**
 * A short string stores strings with length of up to 19 characters. The length
 * is content by the encoded string using UTF-8. Fill always two variables, the
 * encoded string and the string itself. Although this seems to be a waste of
 * space, the java.lang.string is backed by a char[], thus, if we use this, we
 * might only store string with length 16. These strings (at least in English)
 * will have lots of zeros.
 * 
 * @author Thomas Bocek
 * 
 */
public final class ShortString implements Comparable<ShortString>
{
	// The value is used for character storage.
	private final String s1;
	private final Number160 number160;

	// The string is encoded and stored in this byte array
	/**
	 * Creates a short string, encodes string in UTF-8 and checks for its length
	 * 
	 * @param s1 The string
	 * @throws UnsupportedEncodingException if UTF-8 is not available. If this
	 *         is the case, then we have a problem...
	 */
	public ShortString(final String s1)
	{
		byte[] me;
		try
		{
			me = s1.getBytes("UTF-8");
		}
		catch (final UnsupportedEncodingException e)
		{
			throw new RuntimeException(e);
		}
		if (me.length > 19)
			throw new IllegalArgumentException("String too large. Max size is 19 in UTF-8");
		final byte[] me2 = new byte[me.length + 1];
		me2[0] = (byte) me.length;
		System.arraycopy(me, 0, me2, 1, me.length);
		this.s1 = s1;
		this.number160 = new Number160(me2);
	}

	/**
	 * Creates a short string, decodes the byte array from UTF-8 and checks for
	 * its length
	 * 
	 * @param me The byte array
	 * @throws UnsupportedEncodingException If this is the case, then we have a
	 *         problem...
	 */
	public ShortString(final Number160 number160) throws UnsupportedEncodingException
	{
		final byte[] tmp = number160.toByteArray();
		final int len = tmp[0];
		this.s1 = new String(tmp, 1, len);
		this.number160 = number160;
	}

	public Number160 toNumber160()
	{
		return number160;
	}

	@Override
	public String toString()
	{
		return s1;
	}

	@Override
	public boolean equals(final Object obj)
	{
		if (obj instanceof ShortString)
			return s1.equals(((ShortString) obj).s1);
		return false;
	}

	@Override
	public int compareTo(final ShortString o)
	{
		return s1.compareTo(o.s1);
	}
}
