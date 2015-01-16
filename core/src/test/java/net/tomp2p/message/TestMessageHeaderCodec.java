package net.tomp2p.message;

import java.util.Random;

import net.tomp2p.message.Message.Content;

import org.junit.Assert;
import org.junit.Test;

public class TestMessageHeaderCodec {
    
    @Test
    public void testContentTypeCodec() {

        Random rnd = new Random(42);
        System.err.print("Round:");
        for (int i = 0; i < 100; i++) {
        	System.err.print(i + " ");
            Content[] types1 = initContentTypes(rnd);
            int nr = MessageHeaderCodec.encodeContentTypes(types1);
            Content[] types2 = MessageHeaderCodec.decodeContentTypes(nr, new Message());
            compare(types2, types1);
        }
        System.err.println(" done.");
    }

    private void compare(Content[] types2, Content[] types1) {
        Assert.assertEquals(types1.length, types2.length);
        for(int i=0;i<types1.length;i++) {
            if(types1[i] == null) {
                types1[i] = Content.EMPTY;
            }
        }
        Assert.assertArrayEquals(types2, types1);
    }

    private Content[] initContentTypes(Random rnd) {
        Content[] contents = new Content[Message.CONTENT_TYPE_LENGTH];
        int len = rnd.nextInt(9);
        for (int i = 0; i < len; i++) {
            contents[i] = Content.values()[rnd.nextInt(8)];
        }
        return contents;
    }
}
