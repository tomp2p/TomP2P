package net.tomp2p.examples;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.Random;

import net.tomp2p.connection.Bindings;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.nat.FutureRelayNAT;
import net.tomp2p.nat.PeerNAT;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.storage.Data;

public class ExampleRelay {

    private final static Random rnd = new Random();
    private final static int PORT = 7777;

    public static void main(String[] args) throws Exception {

        /*
         * 1. bootstrap: no arguments 
         * 2. put: <bootstrap address> <key> <value>
         * 3. get: <bootstrap address> <key>
         */

        if (args.length == 0) {
            //bootstrap node
            Bindings b = new Bindings();
            Peer peer = new PeerBuilder(Number160.createHash("boot")).setEnableMaintenance(false).ports(PORT).bindings(b).start();
            System.err.println("bootstrap peer id: " + peer.peerAddress().peerId());
            new PeerNAT(peer);
            System.err.println("bootstrap peer is running");
            while(true) {
                Thread.sleep(10000);
                System.err.println(peer.peerBean().peerMap().peerMapVerified());
            }
        } else if (args.length == 3) {
            //put
            int port = (rnd.nextInt() % 10000) + 10000;
            Peer peer = new PeerBuilder(Number160.createHash(args[1])).ports(port).setEnableMaintenance(false).start();
            System.err.println("put peer id: " + peer.peerAddress().peerId());
            PeerNAT pnat = new PeerNAT(peer);

            InetAddress address = Inet4Address.getByName(args[0]);

            FutureRelayNAT fbn = pnat.bootstrapBuilder(peer.bootstrap().inetAddress(address).ports(PORT)).startRelay();
            		
            fbn.awaitUninterruptibly();

            if (fbn.isSuccess()) {
                System.err.println("Successfuly set up relays");
                Thread.sleep(1000);
            }

            FuturePut fp = peer.put(Number160.createHash(args[1])).data(new Data(args[2].toUpperCase())).start();
            System.err.println("hash:" + Number160.createHash(args[1]));
            fp.awaitUninterruptibly();
            if (fp.isSuccess()) {
                System.err.println("Successfully stored " + args[1] + ":" + args[2]);
            }
        } else if (args.length == 2) {
            //get
            int port = (rnd.nextInt() % 10000) + 10000;
            Peer peer = new PeerBuilder(Number160.createHash("bla")).setEnableMaintenance(false).ports(port).start();
            System.err.println("get peer id: " + peer.peerAddress().peerId());
            System.err.println("hash:" + Number160.createHash(args[1]));
            PeerNAT pnat = new PeerNAT(peer);
            
            InetAddress address = Inet4Address.getByName(args[0]);
            FutureRelayNAT fbn = pnat.bootstrapBuilder(peer.bootstrap().inetAddress(address).ports(PORT)).startRelay();
            fbn.awaitUninterruptibly();

            if (fbn.isSuccess()) {
                System.err.println("Successfuly set up relays");
            }

            FutureGet fg = peer.get(Number160.createHash(args[1])).start();
            fg.awaitUninterruptibly();
            if (fg.isSuccess()) {
                System.err.println("Received: " + fg.data().object());
            } else {
                System.err.println(fg.failedReason());
            }
        }
    }

}
