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
package net.tomp2p.storage;
import java.io.IOException;
import java.io.Serializable;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;

import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.utils.Utils;

/**
 * This class holds the data for the transport. The data is already serialized
 * and a hash may be created. It is reasonable to create the hash on the remote
 * peer, but not on the local peer. The remote peer uses the hash to tell the
 * other peers, which version is stored and its used quite often.
 * 
 * @author draft
 * 
 */
public class Data implements Serializable
{
	private static final long serialVersionUID = -5023493840082652284L;
	final public static PublicKey FROM_MESSAGE = new EmptyPublicKey();
	final private byte[] data;
	final private int offset;
	final private int length;
	// never serialied over the network
	final private long validFromMillis;
	//
	private int ttlSeconds;
	private Number160 hash;
	private boolean protectedEntry;
	private boolean directReplication;
	// to avoid redundant objects, use the same object
	private PublicKey dataPublicKey;
	private byte[] signature;
	// if null, the data has been sent directly, otherwise this field shows the
	// originator of this data
	private PeerAddress originator;
	
	public Data(Object object) throws IOException
	{
		this(object, null);
	}

	public Data(Object object, PeerAddress originator) throws IOException
	{
		this(Utils.encodeJavaObject(object), originator);
	}
	
	public Data(byte[] data)
	{
		this(data, null);
	}

	public Data(byte[] data, PeerAddress originator)
	{
		this(data, 0, data.length, originator);
	}

	public Data(byte[] data, int offset, int length, PeerAddress originator)
	{
		this.data = data;
		this.offset = offset;
		this.length = length;
		this.validFromMillis = System.currentTimeMillis();
		this.originator = originator;
	}

	public byte[] getData()
	{
		return data;
	}

	public Object getObject() throws ClassNotFoundException, IOException
	{
		return Utils.decodeJavaObject(data, this.offset, this.length);
	}

	public long getCreated()
	{
		return validFromMillis;
	}

	public int getTTLSeconds()
	{
		return ttlSeconds;
	}

	public void setTTLSeconds(int ttlSeconds)
	{
		this.ttlSeconds = ttlSeconds;
	}

	public Number160 getHash()
	{
		if (this.hash == null)
			this.hash = Utils.makeSHAHash(data, offset, length);
		return hash;
	}

	public int getOffset()
	{
		return offset;
	}

	public int getLength()
	{
		return length;
	}

	public long getExpirationMillis()
	{
		return ttlSeconds <= 0 ? Long.MAX_VALUE : validFromMillis + (ttlSeconds * 1000L);
	}

	/**
	 * This public key is set by the storage class and not by the decoding class
	 * 
	 * @return PublicKey
	 */
	public PublicKey getDataPublicKey()
	{
		return dataPublicKey;
	}

	/**
	 * This public key is set by the storage class and not by the decoding class
	 * 
	 * @param publicKey
	 */
	public void setDataPublicKey(PublicKey dataPublicKey)
	{
		this.dataPublicKey = dataPublicKey;
	}

	public void signAndSetPublicKey(KeyPair keyPair) throws NoSuchAlgorithmException,
			InvalidKeyException, SignatureException
	{
		PrivateKey privateKey = keyPair.getPrivate();
		Signature signatureAlgorithm = Signature.getInstance("SHA1withDSA");
		signatureAlgorithm.initSign(privateKey);
		signatureAlgorithm.update(data, offset, length);
		byte[] ttl = Utils.intToByteArray(ttlSeconds);
		signatureAlgorithm.update(ttl);
		setSignature(signatureAlgorithm.sign());
		setDataPublicKey(keyPair.getPublic());
	}

	public boolean verify(PublicKey public1) throws NoSuchAlgorithmException, InvalidKeyException,
			SignatureException
	{
		final Signature signatureAlgorithm = Signature.getInstance("SHA1withDSA");
		signatureAlgorithm.initVerify(public1);
		signatureAlgorithm.update(data, offset, length);
		byte[] ttl = Utils.intToByteArray(ttlSeconds);
		signatureAlgorithm.update(ttl);
		return signatureAlgorithm.verify(signature);
	}

	public void setSignature(byte[] signature)
	{
		this.signature = signature;
	}

	public byte[] getSignature()
	{
		return signature;
	}

	// TODO: get rid of this. If the data key is set, we can assume that we
	// always want to protect an entry
	public boolean isProtectedEntry()
	{
		return protectedEntry;
	}

	public void setProtectedEntry(boolean protectedEntry)
	{
		this.protectedEntry = protectedEntry;
	}

	public void setDirectReplication(boolean directReplication)
	{
		this.directReplication = directReplication;
	}

	public boolean isDirectReplication()
	{
		return directReplication;
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("Data l:");
		sb.append(length);
		sb.append(",dpk:");
		sb.append(getDataPublicKey() != null);
		sb.append(",ttl:");
		sb.append(getTTLSeconds());
		return sb.toString();
	}

	public PeerAddress getPeerAddress()
	{
		return originator;
	}

	public void setPeerAddress(PeerAddress originator)
	{
		this.originator = originator;
	}
	private static class EmptyPublicKey implements PublicKey
	{
		private static final long serialVersionUID = -9106407808391259340L;

		@Override
		public String getAlgorithm()
		{
			return "";
		}

		@Override
		public byte[] getEncoded()
		{
			return new byte[] {};
		}

		@Override
		public String getFormat()
		{
			return "";
		}
	}
}
