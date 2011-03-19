/*
 * @(#) $CVSHeader:  $
 *
 * Copyright (C) 2011 by Netcetera AG.
 * All rights reserved.
 *
 * The copyright to the computer program(s) herein is the property of
 * Netcetera AG, Switzerland.  The program(s) may be used and/or copied
 * only with the written permission of Netcetera AG or in accordance
 * with the terms and conditions stipulated in the agreement/contract
 * under which the program(s) have been supplied.
 *
 * @(#) $Id: codetemplates.xml,v 1.5 2004/06/29 12:49:49 hagger Exp $
 */
package net.tomp2p.rpc;

import net.tomp2p.peers.Number160;

import org.junit.Assert;
import org.junit.Test;

public class TestBloomFilter
{
	@Test
	public void testBloomfilter()
	{
		SimpleBloomFilter<Number160> bloomFilter=new SimpleBloomFilter<Number160>(4096,1024);
		bloomFilter.add(Number160.MAX_VALUE);
		byte[] me=bloomFilter.toByteArray();
		SimpleBloomFilter<Number160> bloomFilter2=new SimpleBloomFilter<Number160>(me);
		Assert.assertEquals(true, bloomFilter2.contains(Number160.MAX_VALUE));
		Assert.assertEquals(false, bloomFilter2.contains(Number160.ONE));
		Assert.assertEquals(false, bloomFilter2.contains(Number160.ZERO));
	}
}
