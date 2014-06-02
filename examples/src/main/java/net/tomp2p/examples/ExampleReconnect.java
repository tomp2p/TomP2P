package net.tomp2p.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.tomp2p.p2p.Peer;
import net.tomp2p.p2p.PeerBuilder;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;
import net.tomp2p.rpc.ObjectDataReply;

public class ExampleReconnect {
	public static void main(String[] args) throws IOException, InterruptedException {
		if(args.length == 0) {
			final Peer peer = new PeerBuilder(Number160.createHash("master")).ports(1234).start();

	        while(true){
	            peer.send(Number160.createHash("client")).object("ping").start();
	            Thread.sleep(1000);
	        }
		} else {
			final Peer peer = new PeerBuilder(Number160.createHash("client")).ports(1235).start();
            List<PeerAddress> bootstrap = new ArrayList<PeerAddress>();
            bootstrap.add(new PeerAddress(Number160.createHash("master"), "localhost", 1234,1234));
            peer.bootstrap().bootstrapTo(bootstrap).start();

            peer.objectDataReply(new ObjectDataReply() {
                public Object reply(PeerAddress sender, Object request) throws Exception {
                    System.err.println("client received: " + request.toString());
                    return "pong";
                }
            });
		}
	}
}
