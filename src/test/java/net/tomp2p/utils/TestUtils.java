package net.tomp2p.utils;

import java.util.ArrayList;
import java.util.Collection;

import junit.framework.Assert;

import org.junit.Test;

public class TestUtils
{
	@SuppressWarnings("unchecked")
	@Test
	public void testDifference1()
	{
		Collection<String> collection1 = new ArrayList<String>();
		Collection<String> result = new ArrayList<String>();
		Collection<String> collection2 = new ArrayList<String>();
		Collection<String> collection3 = new ArrayList<String>();
		//
		collection1.add("hallo");
		collection1.add("test");
		//
		collection2.add("test");
		collection2.add("hallo");
		Utils.difference(collection1, result, collection2, collection3);
		Assert.assertEquals(0, result.size());
	}
}
