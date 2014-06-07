package net.tomp2p.examples.ws;

import javax.xml.ws.Endpoint;

import net.tomp2p.dht.PeerDHT;
import net.tomp2p.examples.ExampleUtils;

public class ExampleWSJSON {
    static PeerDHT master = null;

    public static void main(String[] args) throws Exception {
        try {
            PeerDHT[] peers = ExampleUtils.createAndAttachPeersDHT(100, 4001);
            master = peers[0];
            ExampleUtils.bootstrap(peers);
            exampleJSON(peers);
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            master.shutdown();
        }
    }

    private static void exampleJSON(PeerDHT[] peers) throws InterruptedException {
        Endpoint.publish("http://localhost:1234/tomp2p", new ServiceImpl());
        Thread.sleep(Integer.MAX_VALUE);
    }
}