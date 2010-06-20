/*
 * Copyright (c) 2002, 2004, Regents of the University of California All rights
 * reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer. Redistributions in binary
 * form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided
 * with the distribution. Neither the name of the University of California at
 * Berkeley nor the names of its contributors may be used to endorse or promote
 * products derived from this software without specific prior written
 * permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * 18.2.2008, Thomas Bocek: Reformatted code, added final fields, added a
 * RuntimeException, and ensures that the range of data is (0,1]
 */
package net.tomp2p;
import java.util.Random;
import java.util.TreeSet;

public class Zipf
{
	private final int size;
	private final double[] prob;
	private final static Random rand = new Random();

	public Zipf(int size, double theta)
	{
		this.size = size;
		prob = new double[size];
		int i;
		double sum = 0;
		for (i = 0; i < size; i++)
		{
			prob[i] = Math.pow(1.0 / (i + 1), theta);
			sum += prob[i];
		}
		for (i = 0; i < size; i++)
		{
			prob[i] /= sum;
			if (i > 0)
			{
				// just a trick for selecting an element using a random number
				prob[i] += prob[i - 1];
			}
			// in case of rounding erros: keep it in range (0,1]
			if (prob[i] > 1)
				prob[i] = 1;
		}
	}

	public int getSize()
	{
		return size;
	}

	public double[] getProbs()
	{
		return prob;
	}

	public int probe()
	{
		double r = rand.nextDouble() * prob[size - 1];
		for (int i = 0; i < size; i++)
			if (r <= prob[i])
				return i;
		throw new RuntimeException("ZIPF: There is something wrong here.");
	}

	public static void main(String[] args)
	{
		TreeSet<SString> ts = new TreeSet<SString>();
		SString ss1 = new SString();
		ss1.setS1("hallo");
		SString ss2 = new SString();
		ss2.setS1("hallo");
		ts.add(ss1);
		ts.add(ss2);
		System.err.println(ts.size());
	}
}

class SString implements Comparable<SString>
{
	private String s1;

	public void setS1(String s1)
	{
		this.s1 = s1;
	}

	public String getS1()
	{
		return s1;
	}

	@Override
	public boolean equals(Object obj)
	{
		return false;
	}

	@Override
	public int compareTo(SString o)
	{
		return s1.compareTo(o.getS1());
	}
}