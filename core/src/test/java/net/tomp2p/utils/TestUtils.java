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

package net.tomp2p.utils;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;

public class TestUtils {
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
    public void testDifference2() {
        Collection<String> collection1 = new ArrayList<String>();
        Collection<String> result = new ArrayList<String>();
        Collection<String> collection2 = new ArrayList<String>();
        Collection<String> collection3 = new ArrayList<String>();
        //
        collection1.add("hallo");
        collection1.add("test");
        collection1.add("world");
        //
        collection2.add("test");
        collection3.add("hallo");
        
        Utils.difference(collection1, result, collection2, collection3);
        Assert.assertEquals(1, result.size());
    }
    
    @Test
    public void testDifference3() {
        Collection<String> collection1 = new ArrayList<String>();
        Collection<String> result = new ArrayList<String>();
        Collection<String> collection2 = new ArrayList<String>();
        Collection<String> collection3 = new ArrayList<String>();
        //
        collection1.add("hallo");        
        collection2.add("world");
        //
        collection2.add("test");
        collection2.add("hallo");
        collection1.add("test");
        
        Utils.difference(collection1, result, collection2, collection3);
        Assert.assertEquals(0, result.size());
    }
    
    @Test
    public void testDifference4() {
        Collection<String> collection1 = new ArrayList<String>();
        Collection<String> result = new ArrayList<String>();
        Collection<String> collection2 = new ArrayList<String>();
        Collection<String> collection3 = new ArrayList<String>();
        
        Utils.difference(collection1, result, collection2, collection3);
        Assert.assertEquals(0, result.size());
    }
    
    @Test
    public void testDifference5() {
        Collection<String> collection1 = new ArrayList<String>();
        Collection<String> result = new ArrayList<String>();
        Collection<String> collection2 = new ArrayList<String>();
        Collection<String> collection3 = new ArrayList<String>();
        //
        collection1.add("test");
        Utils.difference(collection1, result, collection2, collection3);
        Assert.assertEquals(1, result.size());
    }
}
