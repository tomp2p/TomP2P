package net.tomp2p.message;

import net.tomp2p.peers.Number256;
import net.tomp2p.utils.Utils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.util.Arrays;

public class TestMessageHeaderCodec {
	
	@Rule
    public TestRule watcher = new TestWatcher() {
	   protected void starting(Description description) {
          System.out.println("Starting test: " + description.getMethodName());
       }
    };

    @Test
    public void testOverlap() {
        Number256 sender = new Number256(-1, -2, -3, -4);
        Number256 recipient = new Number256(-5, -6, -7, -8);
        byte[] overlap = sender.xorOverlappedBy4(recipient);

        int recipientShort = recipient.intValueMSB();
        int senderShort = sender.intValueLSB();


        Assert.assertEquals(senderShort, Utils.byteArrayToInt(overlap));
        Assert.assertEquals(recipientShort, Utils.byteArrayToInt(overlap, 32));
    }

    @Test
    public void testDeOverlap() {
        Number256 sender = new Number256(-2, -3, -4, -5);
        Number256 recipient = new Number256(-6, -7, -8, -9);
        byte[] overlap = sender.xorOverlappedBy4(recipient);

        int senderShort = sender.intValueLSB();

        Number256 sender2 = recipient.deXorOverlappedBy4(overlap, senderShort);
        Assert.assertEquals(sender, sender2);
    }
}
