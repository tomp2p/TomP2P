package net.tomp2p.examples.ws;

import javax.xml.ws.Endpoint;

import net.tomp2p.examples.ExampleUtils;
import net.tomp2p.p2p.Peer;

public class ExampleWSJSON {
    static Peer master = null;

    public static void main(String[] args) throws Exception {
        try {
            Peer[] peers = ExampleUtils.createAndAttachNodes(100, 4001);
            master = peers[0];
            ExampleUtils.bootstrap(peers);
            exampleJSON(peers);
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            master.shutdown();
        }
    }

    private static void exampleJSON(Peer[] peers) throws InterruptedException {
        Endpoint.publish("http://localhost:1234/tomp2p", new ServiceImpl());
        Thread.sleep(Integer.MAX_VALUE);
    }
}