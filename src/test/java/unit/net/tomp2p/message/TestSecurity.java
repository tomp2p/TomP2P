package net.tomp2p.message;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import junit.framework.Assert;
import net.tomp2p.Utils2;
import net.tomp2p.message.Message;
import net.tomp2p.message.SHA1Signature;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

import org.junit.Test;


public class TestSecurity
{
	@Test
	public void testSecurityDataMap1() throws NoSuchAlgorithmException, SignatureException
	{
		KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
		KeyPair pair = gen.generateKeyPair();
		try
		{
			Data data = new Data(new String("test me"));
			data.signAndSetPublicKey(pair);
			Assert.fail("RSA not supporetd");
		}
		catch (InvalidKeyException e)
		{
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void testSecurityDataMap2()
	{
		try
		{
			KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
			KeyPair pair = gen.generateKeyPair();
			DummyCoder coder = new DummyCoder();
			Message m1 = Utils2.createDummyMessage();
			Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
			Data data = new Data(new String("test me"));
			data.signAndSetPublicKey(pair);
			dataMap.put(Number160.MAX_VALUE, data);
			m1.setDataMap(dataMap);
			// action
			Message m2 = coder.decode(coder.encode(m1));
			Map<Number160, Data> dataMap2 = m2.getDataMap();
			Data data2 = dataMap2.get(Number160.MAX_VALUE);
			Assert.assertEquals(true, data2.verify(pair.getPublic()));
		}
		catch (Exception e)
		{
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void testSecurityDataMap3()
	{
		try
		{
			KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
			KeyPair pair = gen.generateKeyPair();
			DummyCoder coder = new DummyCoder();
			Message m1 = Utils2.createDummyMessage();
			Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
			Data data = new Data(new String("test me"));
			data.signAndSetPublicKey(pair);
			dataMap.put(Number160.MAX_VALUE, data);
			Data data2 = new Data(new byte[100000]);
			data2.signAndSetPublicKey(pair);
			dataMap.put(Number160.ZERO, data2);
			m1.setDataMap(dataMap);
			// action
			Message m2 = coder.decode(coder.encode(m1));
			Map<Number160, Data> dataMap2 = m2.getDataMap();
			Data data3 = dataMap2.get(Number160.MAX_VALUE);
			Assert.assertEquals(true, data3.verify(pair.getPublic()));
			Data data4 = dataMap2.get(Number160.ZERO);
			Assert.assertEquals(true, data4.verify(pair.getPublic()));
		}
		catch (Exception e)
		{
			Assert.fail();
		}
	}

	@Test
	public void testSecurityDataMap4()
	{
		try
		{
			KeyPairGenerator gen = KeyPairGenerator.getInstance("DSA");
			gen.initialize(1024);
			KeyPair pair = gen.generateKeyPair();
			DummyCoder coder = new DummyCoder();
			Message m1 = Utils2.createDummyMessage();
			Map<Number160, Data> dataMap = new HashMap<Number160, Data>();
			Data data = new Data(new String("test me"));
			dataMap.put(Number160.MAX_VALUE, data);
			Data data2 = new Data(new byte[100000]);
			dataMap.put(Number160.ZERO, data2);
			m1.setDataMap(dataMap);
			m1.setPublicKeyAndSign(pair);
			// action
			Message m2 = coder.decode(coder.encode(m1));
			Map<Number160, Data> dataMap2 = m2.getDataMap();
			Assert.assertEquals(2, dataMap2.size());
			Assert.assertNotNull(m2.getPublicKey());
			Assert.assertEquals(pair.getPublic(), m2.getPublicKey());
		}
		catch (Exception e)
		{
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void testSecurityDataMap5()
	{
		for (int i = 0; i < 100; i++)
			testSecurityDataMap4();
	}

	@Test
	public void testSecurity1() throws IOException
	{
		Random r = new Random(42L);
		for (int i = 0; i < 1000; i++)
		{
			Number160 n1 = new Number160(r);
			Number160 n2 = new Number160(r);
			SHA1Signature s1 = new SHA1Signature(n1, n2);
			byte[] me = s1.encode();
			SHA1Signature s2 = new SHA1Signature();
			s2.decode(me);
			Assert.assertEquals(n1, s2.getNumber1());
			Assert.assertEquals(n2, s2.getNumber2());
		}
	}

	@Test
	public void testSecurity2() throws IOException
	{
		//Random r = new Random(42L);
		Number160 n1 = new Number160(23);
		Number160 n2 = new Number160(24);
		SHA1Signature s1 = new SHA1Signature(n1, n2);
		byte[] me = s1.encode();
		SHA1Signature s2 = new SHA1Signature();
		s2.decode(me);
		Assert.assertEquals(n1, s2.getNumber1());
		Assert.assertEquals(n2, s2.getNumber2());
	}
}
