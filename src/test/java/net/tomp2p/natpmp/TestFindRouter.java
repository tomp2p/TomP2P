package net.tomp2p.natpmp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

public class TestFindRouter {
    public final static Map<String, String> MAP = new HashMap<String, String>();

    static {
        MAP.put("macosx-de.txt", "/130.60.156.1");
        MAP.put("macosx-en.txt", "/130.60.155.1");
        MAP.put("ubuntu-en.txt", "/89.206.80.1");
        MAP.put("ubuntu-en2.txt", "/130.60.156.1");
        MAP.put("win7-de.txt", "/10.0.2.2");
        MAP.put("win7-en.txt", "/130.60.156.1");
        MAP.put("winxp-en.txt", "/10.0.2.2");
    }

    @Test
    public void testParser() throws UnknownHostException, IOException {
        for (Map.Entry<String, String> entry : MAP.entrySet()) {
            InputStream is = TestFindRouter.class.getResourceAsStream("/" + entry.getKey());
            if (is == null) {
                is = TestFindRouter.class.getResourceAsStream(entry.getKey());
            }
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            InetAddress inet = Gateway.parse(br);
            Assert.assertEquals(entry.getValue(), inet.toString());
        }
    }
}
