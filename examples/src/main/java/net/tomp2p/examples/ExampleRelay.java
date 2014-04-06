package net.tomp2p.examples;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.Random;

import net.tomp2p.connection.Bindings;
import net.tomp2p.futures.FutureGet;
import net.tomp2p.futures.FuturePut;
import net.tomp2p.nat.RelayConf;
import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerMaker;
import net.tomp2p.peers.Number160;
import net.tomp2p.relay.FutureRelay;
import net.tomp2p.relay.RelayRPC;
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

        /*if (args.length == 0) {
            //bootstrap node
            Bindings b = new Bindings();
            Peer peer = new PeerMaker(Number160.createHash("boot")).setEnableMaintenance(false).ports(PORT).bindings(b).makeAndListen();
            System.err.println("bootstrap peer id: " + peer.getPeerAddress().getPeerId());
            RelayRPC.setup(peer);
            System.err.println("bootstrap peer is running");
            while(true) {
                Thread.sleep(10000);
                System.err.println(peer.getPeerBean().peerMap().peerMapVerified());
            }
        } else if (args.length == 3) {
            //put
            int port = (rnd.nextInt() % 10000) + 10000;
            Peer peer = new PeerMaker(Number160.createHash(args[1])).ports(port).setEnableMaintenance(false).makeAndListen();
            System.err.println("put peer id: " + peer.getPeerAddress().getPeerId());
            RelayRPC.setup(peer);

            InetAddress address = Inet4Address.getByName(args[0]);

            //RelayManager manager = new RelayManager(peer, new PeerAddress(bootstrapId, InetSocketAddress.createUnresolved(args[0], PORT)));
            FutureRelay rf = new RelayConf(peer).bootstrapInetAddress(address).ports(PORT).start();
            rf.awaitUninterruptibly();

            if (rf.isSuccess()) {
                System.err.println("Successfuly set up relays");
                Thread.sleep(1000);
            }

            FuturePut fp = peer.put(Number160.createHash(args[1])).setData(new Data(args[2].toUpperCase())).start();
            System.err.println("hash:" + Number160.createHash(args[1]));
            fp.awaitUninterruptibly();
            if (fp.isSuccess()) {
                System.err.println("Successfully stored " + args[1] + ":" + args[2]);
            }
        } else if (args.length == 2) {
            //get
            int port = (rnd.nextInt() % 10000) + 10000;
            Peer peer = new PeerMaker(Number160.createHash("bla")).setEnableMaintenance(false).ports(port).makeAndListen();
            System.err.println("get peer id: " + peer.getPeerAddress().getPeerId());
            System.err.println("hash:" + Number160.createHash(args[1]));
            
            InetAddress address = Inet4Address.getByName(args[0]);
            FutureRelay rf = new RelayConf(peer).bootstrapInetAddress(address).ports(PORT).start();
            rf.awaitUninterruptibly();

            if (rf.isSuccess()) {
                System.err.println("Successfuly set up relays");
            }

            FutureGet fg = peer.get(Number160.createHash(args[1])).start();
            fg.awaitUninterruptibly();
            if (fg.isSuccess()) {
                System.err.println("Received: " + fg.getData().object());
            } else {
                System.err.println(fg.getFailedReason());
            }
        }*/
    }

}
