package net.tomp2p;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import net.tomp2p.message.KeyCollection;
import net.tomp2p.message.Message;
import net.tomp2p.message.TestMessage;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.Number640;

import org.junit.Test;

public class KeyCollectionTest {

	static byte[] sampleBytes1 = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19};
	static byte[] sampleBytes2 = new byte[] {19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
	static byte[] sampleBytes3 = new byte[Number160.BYTE_ARRAY_SIZE];
	
	static Number160 sample160_1 = Number160.ZERO;
	static Number160 sample160_2 = Number160.ONE;
	static Number160 sample160_3 = Number160.MAX_VALUE;
	static Number160 sample160_4 = new Number160(sampleBytes1);
	static Number160 sample160_5 = new Number160(sampleBytes2);
	
	static Number640 sample640_1 = Number640.ZERO;
	static Number640 sample640_2 = new Number640(new Number160(sampleBytes1), new Number160(sampleBytes2), new Number160(sampleBytes3), Number160.MAX_VALUE);
	static Number640 sample640_3 = new Number640(Number160.MAX_VALUE, new Number160(sampleBytes3), new Number160(sampleBytes2), new Number160(sampleBytes1));
	
	@Test
	public void encodeDecodeTest() throws Exception {
		
		// create a sample key collections
		Collection<Number160> sampleCollection1 = new ArrayList<Number160>();
		sampleCollection1.add(sample160_1);
		sampleCollection1.add(sample160_2);
		sampleCollection1.add(sample160_3);
		
		Collection<Number160> sampleCollection2 = new ArrayList<Number160>();
		sampleCollection2.add(sample160_2);
		sampleCollection2.add(sample160_3);
		sampleCollection2.add(sample160_4);
		
		Collection<Number160> sampleCollection3 = new ArrayList<Number160>();
		sampleCollection3.add(sample160_3);
		sampleCollection3.add(sample160_4);
		sampleCollection3.add(sample160_5);
		
		Message m1 = Utils2.createDummyMessage();
		m1.keyCollection(new KeyCollection(sample160_1, sample160_1, sample160_1, sampleCollection1));
		m1.keyCollection(new KeyCollection(sample160_2, sample160_2, sample160_2, sampleCollection2));
		m1.keyCollection(new KeyCollection(sample160_3, sample160_3, sample160_3, sampleCollection3));
		m1.keyCollection(new KeyCollection(sample160_4, sample160_4, sample160_4, sampleCollection1));
		m1.keyCollection(new KeyCollection(sample160_5, sample160_5, sample160_5, sampleCollection2));
		m1.keyCollection(new KeyCollection(sample160_1, sample160_2, sample160_3, sampleCollection3));
		m1.keyCollection(new KeyCollection(sample160_2, sample160_3, sample160_4, sampleCollection1));
		m1.keyCollection(new KeyCollection(sample160_3, sample160_4, sample160_5, sampleCollection2));
		
		Message m2 = TestMessage.encodeDecode(m1);
		
		assertTrue(CheckIsSameList(m1.keyCollectionList(), m2.keyCollectionList()));
	}
		
	private static <T> boolean CheckIsSameList(List<T> list1, List<T> list2) {
        if (list1 == null ^ list2 == null) // XOR
        {
            return false;
        }
        if (list1 != null && (list1.size() != list2.size()))
        {
            return false;
        }

        for (int i = 0; i < list1.size(); i++)
        {
            if (!list1.get(i).equals(list2.get(i)))
            {
                return false;
            }
        }
        return true;
    }
}
