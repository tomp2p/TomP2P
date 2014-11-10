package net.tomp2p.utils;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;

public class TestUtils {
    @SuppressWarnings("unchecked")
    @Test
    public void testDifference1() {
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
    
    @Test
    public void testIsSameCollectionsSet() {
    	
    	List<Integer> list1 = new ArrayList<Integer>();
    	list1.add(1);
    	list1.add(2);
    	list1.add(3);
    	
    	List<Integer> list2 = new ArrayList<Integer>();
    	list2.add(4);
    	list2.add(5);
    	list2.add(6);
    	
    	List<Integer> list3 = new ArrayList<Integer>();
    	list3.add(4);
    	list3.add(5);
    	list3.add(6);
    	
    	Set<Integer> set1 = new HashSet<Integer>();
    	set1.add(1);
    	set1.add(2);
    	set1.add(3);
    	
    	Set<Integer> set2 = new HashSet<Integer>();
    	set2.add(4);
    	set2.add(5);
    	set2.add(6);
    	
    	Set<Integer> set3 = new HashSet<Integer>();
    	set3.add(7);
    	set3.add(8);
    	set3.add(9);
    	
    	Collection<List<Integer>> collection1 = new ArrayList<List<Integer>>();
    	collection1.add(list1);
    	collection1.add(list2);
    	
    	Collection<Set<Integer>> collection2 = new HashSet<Set<Integer>>();
    	collection2.add(set1);
    	collection2.add(set2);
    	
    	Collection<Set<Integer>> collection3 = new HashSet<Set<Integer>>();
    	collection3.add(set2);
    	collection3.add(set3);
    	
    	// collection1 == collection2
    	assertTrue(Utils.isSameCollectionSets(collection1, collection2));
    	
    	// collection1 != collection3
    	assertFalse(Utils.isSameCollectionSets(collection1, collection3));
    }
}
